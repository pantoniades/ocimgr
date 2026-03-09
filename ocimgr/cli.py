#!/usr/bin/env python3
"""
OCIMgr CLI Interface - Python 3.13 with corrected match syntax
Command-line interface for OCI resource management using proper match statements
"""

#!/usr/bin/env python3
"""
OCIMgr CLI Interface - Python 3.13 with corrected imports
Command-line interface for OCI resource management using proper match statements
"""

import sys
import os
import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from dataclasses import asdict
from enum import Enum
import click
import oci
import random

# Use ABSOLUTE imports instead of relative imports
from ocimgr.core import (
    OCIConfig, AsyncOCISession, setup_async_logging, 
    get_registered_resource_types, AbstractOCIResource,
    ResourceDiscoveryEngine, OperationResult, ResourceStatus,
    AsyncResourceMixin
)
from ocimgr.utils import (
    ProgressTracker, ProgressItem, OutputFormatter, 
    InteractiveSelector, format_duration, run_with_backoff
)
from ocimgr.models.compartment import CompartmentManager


def get_region_cache_path(config_path: Optional[str]) -> Path:
    """Resolve the .region_cache file path next to the OCI config."""
    if config_path:
        return Path(config_path).expanduser().resolve().parent / ".region_cache"

    for candidate in OCIConfig.DEFAULT_CONFIG_PATHS:
        path = Path(candidate).expanduser()
        if path.exists():
            return path.resolve().parent / ".region_cache"

    return Path.home() / ".oci_mgr" / ".region_cache"


def load_region_cache(cache_path: Path) -> Optional[List[str]]:
    """Load cached regions from disk if present and valid."""
    if not cache_path.exists():
        return None

    try:
        import json

        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        regions = payload.get("regions")
        if isinstance(regions, list) and all(isinstance(r, str) for r in regions):
            return [r.strip() for r in regions if r.strip()]
    except Exception:
        return None

    return None


def write_region_cache(cache_path: Path, regions: List[str], profile: str) -> None:
    """Persist regions to the cache file in JSON format."""
    import json
    from datetime import datetime

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "profile": profile,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "regions": sorted(set(regions))
    }
    cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


async def refresh_session_regions(
    cli_app: "OCIMgrAsyncCLI",
    regions: List[str],
    verbose: bool,
    label: str
) -> None:
    """Rebuild the session with an updated region list."""
    if not regions:
        return

    await cli_app.session.close()
    cli_app.session.config.set_regions(regions)
    cli_app.session = AsyncOCISession(cli_app.session.config)
    await cli_app.session.wait_until_ready()
    cli_app.compartment_manager = CompartmentManager(cli_app.session)
    cli_app.discovery_engine = ResourceDiscoveryEngine(cli_app.session)
    if verbose:
        click.echo(f"🌍 Using {label} regions: {', '.join(regions)}")


async def discover_and_cache_regions(
    cli_app: "OCIMgrAsyncCLI",
    cache_path: Path,
    verbose: bool
) -> List[str]:
    """Discover subscribed regions and persist them to the cache."""
    identity_client = await cli_app.session.get_client('identity')
    region_response = await AsyncResourceMixin._run_oci_operation(
        identity_client.list_region_subscriptions,
        tenancy_id=cli_app.session.oci_config['tenancy']
    )
    subscribed_regions = [r.region_name for r in region_response.data]
    if subscribed_regions:
        write_region_cache(cache_path, subscribed_regions, cli_app.profile)
        if verbose:
            click.echo(f"🗃️  Cached {len(subscribed_regions)} regions to {cache_path}")
    return subscribed_regions



class CLIAction(Enum):
    """Available CLI actions for match statement handling"""
    LIST_RESOURCES = "list_resources"
    DELETE_RESOURCES = "delete_resources" 
    EXPORT_DATA = "export_data"
    VALIDATE_COMPARTMENT = "validate_compartment"
    QUIT = "quit"


class OCIMgrAsyncCLI:
    """
    Modern async CLI application class using Python 3.13 features.
    Handles concurrent operations and provides interactive experience.
    """
    
    def __init__(self, config_path: Optional[str] = None, profile: str = "DEFAULT", 
                 log_level: str = "INFO"):
        self.config_path = config_path
        self.profile = profile
        self.log_level = log_level
        self.session: Optional[AsyncOCISession] = None
        self.compartment_manager: Optional[CompartmentManager] = None
        self.discovery_engine: Optional[ResourceDiscoveryEngine] = None
        
        # Track current operation for cancellation
        self._current_operation: Optional[asyncio.Task] = None
    
    async def initialize(self) -> None:
        """Initialize async components"""
        log_file = await setup_async_logging(level=self.log_level)
        click.echo(f"📝 Logging to {log_file}")
        
        try:
            config = OCIConfig(self.config_path, self.profile)
            self.session = AsyncOCISession(config)
            
            # Wait for client initialization
            await self.session.wait_until_ready()
            
            self.compartment_manager = CompartmentManager(self.session)
            self.discovery_engine = ResourceDiscoveryEngine(self.session)
            
            click.echo(f"✓ Initialized OCIMgr for regions: {', '.join(self.session.get_all_regions())}")
            click.echo(f"✓ Current region: {self.session.get_current_region()}")
            click.echo(f"✓ Registered resource types: {len(get_registered_resource_types())}")
            
        except Exception as e:
            click.echo(f"✗ Failed to initialize OCIMgr: {e}", err=True)
            raise
    
    async def cleanup(self) -> None:
        """Cleanup async resources"""
        if self._current_operation and not self._current_operation.done():
            self._current_operation.cancel()
            try:
                await self._current_operation
            except asyncio.CancelledError:
                pass
        
        if self.session:
            await self.session.close()
    
    async def list_compartments(self) -> List[Dict[str, Any]]:
        """List all accessible compartments"""
        try:
            compartments = await self.compartment_manager.list_compartments()
            return compartments
        except Exception as e:
            click.echo(f"✗ Error listing compartments: {e}", err=True)
            return []
    
    async def discover_resources(
        self, 
        compartment_ids: List[str],
        resource_type_filter: Optional[List[str]] = None,
        skip_unauthorized: bool = True
    ) -> Dict[str, List[AbstractOCIResource]]:
        """
        Discover resources with concurrent multi-region scanning.
        
        Args:
            compartment_ids: List of compartment IDs to scan
            resource_type_filter: Optional filter for specific resource types
        
        Returns:
            Dictionary mapping compartment_id to list of resources
        """
        click.echo(f"🔍 Discovering resources across {len(self.session.get_all_regions())} regions...")
        
        try:
            # Create cancellable task
            self._current_operation = asyncio.create_task(
                self.discovery_engine.discover_all_resources(
                    compartment_ids, 
                    resource_type_filter,
                    skip_unauthorized=skip_unauthorized
                )
            )
            
            # Show progress while discovering
            progress_task = asyncio.create_task(self._show_discovery_progress())
            
            # Wait for discovery to complete
            results = await self._current_operation
            
            # Cancel progress display
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass
            
            click.echo("✓ Discovery completed")
            return results
            
        except asyncio.CancelledError:
            click.echo("\n⚠️ Discovery cancelled by user")
            return {}
        except Exception as e:
            click.echo(f"✗ Discovery failed: {e}", err=True)
            return {}
        finally:
            self._current_operation = None

    async def discover_resource_counts(
        self,
        compartment_ids: List[str],
        resource_type_filter: Optional[List[str]] = None,
        max_concurrent: int = 3
    ) -> Dict[str, Dict[str, int]]:
        """
        Discover resource counts per compartment with throttling-aware concurrency.
        
        Returns:
            Dict of compartment_id -> resource_type -> count
        """
        resource_types = get_registered_resource_types()
        if resource_type_filter:
            resource_types = {
                name: cls for name, cls in resource_types.items()
                if name in resource_type_filter
            }

        results: Dict[str, Dict[str, int]] = {}

        async def discover_type(compartment_id: str, resource_type_name: str, resource_cls):
            async def operation():
                resources = await resource_cls.discover(self.session, compartment_id)
                return len(resources)

            count = await run_with_backoff(operation)
            return resource_type_name, count

        for compartment_id in compartment_ids:
            semaphore = asyncio.Semaphore(max_concurrent)
            tasks = []

            for resource_type_name, resource_cls in resource_types.items():
                async def guarded_discover(rt_name=resource_type_name, rt_cls=resource_cls):
                    async with semaphore:
                        return await discover_type(compartment_id, rt_name, rt_cls)

                tasks.append(guarded_discover())

            type_results = await asyncio.gather(*tasks, return_exceptions=True)
            compartment_counts: Dict[str, int] = {}

            for result in type_results:
                if isinstance(result, tuple):
                    resource_type_name, count = result
                    compartment_counts[resource_type_name] = count
                elif isinstance(result, Exception):
                    logging.warning(f"Count discovery failed in compartment {compartment_id}: {result}")

            results[compartment_id] = compartment_counts

        return results

    async def discover_fast_counts(
        self,
        compartment_ids: List[str],
        resource_type_filter: Optional[List[str]] = None,
        max_concurrent: int = 1,
        verbose: bool = False,
        skip_unauthorized: bool = True,
        request_timeout: float = 60.0
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Fast, counts-only discovery using list calls (no per-resource detail).
        Designed to reduce throttling.
        """
        resource_map = {
            "compute_instance": ("compute", "list_instances"),
            "autonomous_database": ("database", "list_autonomous_databases"),
            "mysql_db_system": ("mysql", "list_db_systems"),
            "oke_cluster": ("container_engine", "list_clusters")
        }

        if resource_type_filter:
            resource_map = {
                name: value for name, value in resource_map.items()
                if name in resource_type_filter
            }

        results: Dict[str, Dict[str, int]] = {}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def list_count(
            compartment_id: str,
            resource_type: str,
            service: str,
            method_name: str,
            region: str
        ) -> tuple[str, int]:
            async def operation():
                client = await self.session.get_client(service, region)
                method = getattr(client, method_name)
                response = await asyncio.wait_for(
                    AsyncResourceMixin._run_oci_operation(
                        oci.pagination.list_call_get_all_results,
                        method,
                        compartment_id=compartment_id
                    ),
                    timeout=request_timeout
                )
                return region, len(response.data)

            async with semaphore:
                try:
                    return await run_with_backoff(operation)
                except asyncio.TimeoutError:
                    logging.warning(
                        "Fast count timed out for %s in %s (%s)",
                        resource_type,
                        compartment_id,
                        region
                    )
                    return region, 0
                except oci.exceptions.ServiceError as e:
                    if e.status in {401, 403}:
                        self.session.mark_region_unauthorized(region)
                        if skip_unauthorized:
                            if verbose:
                                logging.warning(f"Skipping unauthorized region {region} for {resource_type}: {e}")
                            return region, 0
                    raise

        for compartment_id in compartment_ids:
            counts: Dict[str, int] = {}

            for resource_type, (service, method_name) in resource_map.items():
                regions = (
                    self.session.get_authorized_regions()
                    if skip_unauthorized
                    else self.session.get_all_regions()
                )
                tasks = [
                    list_count(compartment_id, resource_type, service, method_name, region)
                    for region in regions
                ]

                logging.info(
                    "Fast counts for %s in compartment %s across %d regions",
                    resource_type,
                    compartment_id,
                    len(regions)
                )
                results_per_region = await asyncio.gather(*tasks, return_exceptions=True)
                total_count = 0
                regions_with_resources: List[str] = []

                for result in results_per_region:
                    if isinstance(result, Exception):
                        message = f"Fast count failed for {resource_type} in {compartment_id}: {result}"
                        if verbose:
                            logging.warning(message)
                        else:
                            logging.debug(message)
                    else:
                        region_name, region_count = result
                        total_count += region_count
                        if region_count > 0:
                            regions_with_resources.append(region_name)

                counts[resource_type] = {
                    "count": total_count,
                    "regions": sorted(set(regions_with_resources))
                }
                logging.info(
                    "Fast count result for %s in compartment %s: %d",
                    resource_type,
                    compartment_id,
                    total_count
                )

            results[compartment_id] = counts

        return results
    
    async def _show_discovery_progress(self) -> None:
        """Show animated progress during discovery"""
        spinner_chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        i = 0
        
        try:
            while True:
                click.echo(f"\r{spinner_chars[i % len(spinner_chars)]} Scanning resources...", nl=False)
                await asyncio.sleep(0.1)
                i += 1
        except asyncio.CancelledError:
            click.echo("\r" + " " * 30 + "\r", nl=False)  # Clear the line
    
    async def create_deletion_plan(self, resources: List[AbstractOCIResource]) -> List[AbstractOCIResource]:
        """
        Create an optimized deletion plan with dependency analysis.
        
        Args:
            resources: List of resources to delete
        
        Returns:
            Ordered list of resources for deletion
        """
        if not resources:
            return []
        
        # Sort by deletion order priority and estimated time
        sorted_resources = sorted(
            resources, 
            key=lambda r: (r.get_deletion_order_priority(), r.get_estimated_deletion_time())
        )
        
        click.echo(f"\n📋 Deletion Plan ({len(sorted_resources)} resources):")
        click.echo("=" * 60)
        
        total_time = 0
        for i, resource in enumerate(sorted_resources, 1):
            estimated_time = resource.get_estimated_deletion_time()
            total_time += estimated_time
            
            # Use match statement for resource type formatting (CORRECTED)
            icon = "📦"  # default
            match resource.info.resource_type:
                case "compute_instance":
                    icon = "🖥️"
                case "autonomous_database" | "mysql_db_system":
                    icon = "🗄️"
                case "oke_cluster":
                    icon = "☸️"
                case "block_volume":
                    icon = "💾"
                case _:
                    icon = "📦"
            
            protection_marker = " 🔒" if resource.info.has_delete_protection else ""
            
            click.echo(f"  {i:2d}. {icon} {resource.info.resource_type}: {resource.info.name}")
            click.echo(f"      Region: {resource.info.region} | Est: {format_duration(estimated_time)}{protection_marker}")
        
        click.echo("=" * 60)
        click.echo(f"📊 Total estimated time: {format_duration(total_time)}")
        
        if any(r.info.has_delete_protection for r in sorted_resources):
            click.echo("🔒 Resources with delete protection will be processed first")
        
        return sorted_resources
    
    async def execute_deletion(
        self, 
        resources: List[AbstractOCIResource], 
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """
        Execute resource deletion with async progress tracking.
        
        Args:
            resources: Ordered list of resources to delete
            dry_run: If True, only simulate deletion
        
        Returns:
            Summary of deletion results
        """
        if not resources:
            click.echo("No resources to delete.")
            return {"total": 0, "successful": 0, "failed": 0}
        
        action = "🧪 DRY RUN" if dry_run else "🗑️  DELETION"
        click.echo(f"\n{action} EXECUTION")
        click.echo("=" * 50)
        
        # Create progress items
        progress_items = [
            ProgressItem(f"{r.info.resource_type}: {r.info.name}", r.get_estimated_deletion_time())
            for r in resources
        ]
        
        # Initialize progress tracker
        tracker = ProgressTracker(progress_items, show_progress=not dry_run)
        tracker.start()
        
        results = []
        
        try:
            # Process resources with controlled concurrency
            semaphore = asyncio.Semaphore(3)  # Max 3 concurrent deletions
            
            async def process_resource(i: int, resource: AbstractOCIResource) -> OperationResult:
                async with semaphore:
                    tracker.start_item(i)
                    
                    if dry_run:
                        # Simulate dry run
                        click.echo(f"[DRY RUN] Would delete {resource.info.resource_type}: {resource.info.name}")
                        await asyncio.sleep(0.1)  # Simulate processing
                        
                        result = OperationResult(
                            resource_ocid=resource.info.ocid,
                            operation="dry_run_delete",
                            status=ResourceStatus.COMPLETED,
                            message="Dry run successful"
                        )
                        tracker.complete_item(i)
                        return result
                    else:
                        return await self._delete_single_resource(resource, i, tracker)
            
            # Execute deletions
            deletion_tasks = [
                process_resource(i, resource) 
                for i, resource in enumerate(resources)
            ]
            
            results = await asyncio.gather(*deletion_tasks, return_exceptions=True)
            
        except asyncio.CancelledError:
            click.echo("\n⚠️ Deletion cancelled by user")
        except Exception as e:
            click.echo(f"\n✗ Deletion execution failed: {e}", err=True)
        finally:
            tracker.finish()
            self._current_operation = None
        
        # Calculate summary using corrected match syntax
        successful = 0
        failed = 0
        
        for result in results:
            if isinstance(result, OperationResult):
                match result.status:
                    case ResourceStatus.COMPLETED:
                        successful += 1
                    case ResourceStatus.FAILED:
                        failed += 1
        
        summary = {
            "total": len(resources),
            "successful": successful, 
            "failed": failed,
            "results": [r for r in results if isinstance(r, OperationResult)]
        }
        
        # Print summary
        tracker.print_summary()
        self._print_deletion_summary(summary, dry_run)
        
        return summary
    
    async def _delete_single_resource(
        self, 
        resource: AbstractOCIResource, 
        index: int, 
        tracker: ProgressTracker
    ) -> OperationResult:
        """Delete a single resource with error handling"""
        try:
            click.echo(f"🗑️  Deleting {resource.info.resource_type}: {resource.info.name}")
            
            # Step 1: Disable delete protection
            if resource.info.has_delete_protection:
                protection_result = await resource.disable_delete_protection()
                
                match protection_result.status:
                    case ResourceStatus.COMPLETED:
                        pass  # Continue with deletion
                    case ResourceStatus.FAILED:
                        tracker.complete_item(index, "Failed to disable delete protection")
                        return protection_result
                    case _:
                        tracker.complete_item(index, f"Unknown protection status: {protection_result.status}")
                        return protection_result
            
            # Step 2: Perform deletion
            deletion_result = await resource.delete()
            
            match deletion_result.status:
                case ResourceStatus.COMPLETED:
                    tracker.complete_item(index)
                    return deletion_result
                case ResourceStatus.FAILED:
                    tracker.complete_item(index, deletion_result.message)
                    return deletion_result
                case _:
                    tracker.complete_item(index, "Unknown deletion status")
                    return OperationResult(
                        resource_ocid=resource.info.ocid,
                        operation="delete",
                        status=ResourceStatus.FAILED,
                        message="Unknown status returned"
                    )
                    
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            tracker.complete_item(index, error_msg)
            return OperationResult(
                resource_ocid=resource.info.ocid,
                operation="delete",
                status=ResourceStatus.FAILED,
                message=error_msg
            )
    
    def _print_deletion_summary(self, summary: Dict[str, Any], dry_run: bool) -> None:
        """Print a detailed deletion summary"""
        action = "Dry Run" if dry_run else "Deletion"
        
        click.echo(f"\n📊 {action} Summary:")
        click.echo("=" * 30)
        click.echo(f"Total resources: {summary['total']}")
        click.echo(f"✅ Successful: {summary['successful']}")
        click.echo(f"❌ Failed: {summary['failed']}")
        
        if summary['failed'] > 0:
            click.echo(f"\n❌ Failed Resources:")
            for result in summary['results']:
                match result.status:
                    case ResourceStatus.FAILED:
                        click.echo(f"  • {result.resource_ocid}: {result.message}")
    
    async def export_data(
        self, 
        data: Any, 
        format_type: str, 
        filename: Optional[str] = None
    ) -> bool:
        """
        Export data to file in specified format asynchronously.
        
        Args:
            data: Data to export
            format_type: Output format (json, csv)
            filename: Output filename (auto-generated if None)
        
        Returns:
            True if successful, False otherwise
        """
        if filename is None:
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ocimgr_export_{timestamp}.{format_type}"
        
        try:
            # Prepare data for export using corrected match syntax
            export_data = data
            match format_type:
                case 'json':
                    # Convert resources to serializable format
                    if isinstance(data, list) and data and hasattr(data[0], 'info'):
                        export_data = [asdict(item.info) for item in data]
                case 'csv':
                    # Convert objects to dictionaries for CSV export
                    if isinstance(data, list) and data and hasattr(data[0], 'info'):
                        export_data = [asdict(item.info) for item in data]
                case _:
                    click.echo(f"Unsupported format: {format_type}")
                    return False
            
            # Format content using corrected match syntax
            content = ""
            match format_type:
                case 'json':
                    content = OutputFormatter.format_json(export_data)
                case 'csv':
                    content = OutputFormatter.format_csv(export_data)
                case _:
                    content = ""
            
            # Save asynchronously
            success = await asyncio.get_event_loop().run_in_executor(
                None, 
                OutputFormatter.save_to_file, 
                content, 
                filename
            )
            
            if success:
                click.echo(f"✓ Data exported to {filename}")
                return True
            else:
                click.echo(f"✗ Failed to export data to {filename}")
                return False
                
        except Exception as e:
            click.echo(f"✗ Error exporting data: {e}")
            return False


# Async-aware Click commands
def async_command(f):
    """Decorator to make Click commands async-aware"""
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        task = loop.create_task(f(*args, **kwargs))
        try:
            return loop.run_until_complete(task)
        except KeyboardInterrupt:
            for pending in asyncio.all_tasks(loop):
                pending.cancel()
            loop.run_until_complete(
                asyncio.gather(*asyncio.all_tasks(loop), return_exceptions=True)
            )
            raise
        finally:
            loop.close()
    return wrapper


def _install_output_tee(output_path: Optional[str]) -> None:
    """Duplicate stdout/stderr to a file if output_path is provided."""
    if not output_path:
        return

    log_path = Path(output_path).expanduser().resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    class _Tee:
        def __init__(self, *streams):
            self._streams = streams

        def write(self, data):
            for stream in self._streams:
                stream.write(data)
            return len(data)

        def flush(self):
            for stream in self._streams:
                stream.flush()

    log_file = log_path.open("a", encoding="utf-8")
    sys.stdout = _Tee(sys.stdout, log_file)
    sys.stderr = _Tee(sys.stderr, log_file)


@click.group()
@click.option('--config', '-c', help='Path to OCI config file')
@click.option('--profile', '-p', default='DEFAULT', help='OCI config profile to use')
@click.option('--output', '-o', help='Save CLI output to a file')
@click.option('--log-level', '-l', default='INFO', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']))
@click.pass_context
def cli(ctx, config, profile, output, log_level):
    """OCIMgr - Oracle Cloud Infrastructure Management Tool (Python 3.13)"""
    ctx.ensure_object(dict)
    ctx.obj['config'] = config
    ctx.obj['profile'] = profile
    ctx.obj['output'] = output
    ctx.obj['log_level'] = log_level
    _install_output_tee(output)


@cli.command()
@click.pass_context
@async_command
async def interactive(ctx):
    """Modern interactive resource management mode with async operations"""
    cli_app = OCIMgrAsyncCLI(
        ctx.obj['config'], 
        ctx.obj['profile'], 
        ctx.obj['log_level']
    )
    
    try:
        await cli_app.initialize()
        
        click.echo("🚀 OCIMgr Interactive Mode (Python 3.13)")
        click.echo("Press Ctrl+C at any time to cancel operations\n")
        
        while True:
            try:
                # Main menu with modern formatting
                click.echo("📋 Main Menu:")
                menu_options = [
                    "🔍 List compartments and resources",
                    "🗑️  Delete resources from compartment(s)", 
                    "📤 Export resource data",
                    "✅ Validate compartment emptiness",
                    "🚪 Quit"
                ]
                
                for i, option in enumerate(menu_options, 1):
                    click.echo(f"  {i}. {option}")
                
                choice = click.prompt("\n🎯 Select an option", type=int, default=1)
                
                # Use match statement for menu handling (CORRECTED)
                action = None
                match choice:
                    case 1:
                        action = CLIAction.LIST_RESOURCES
                    case 2:
                        action = CLIAction.DELETE_RESOURCES
                    case 3:
                        action = CLIAction.EXPORT_DATA
                    case 4:
                        action = CLIAction.VALIDATE_COMPARTMENT
                    case 5:
                        action = CLIAction.QUIT
                    case _:
                        action = None
                
                # Handle action using corrected match syntax
                match action:
                    case CLIAction.LIST_RESOURCES:
                        await _handle_list_resources(cli_app)
                    case CLIAction.DELETE_RESOURCES:
                        await _handle_delete_resources(cli_app)
                    case CLIAction.EXPORT_DATA:
                        await _handle_export_data(cli_app)
                    case CLIAction.VALIDATE_COMPARTMENT:
                        await _handle_validate_compartment(cli_app)
                    case CLIAction.QUIT:
                        click.echo("👋 Goodbye!")
                        break
                    case None:
                        click.echo("❌ Invalid option. Please try again.")
                
                click.echo("\n" + "="*60 + "\n")
                
            except KeyboardInterrupt:
                if cli_app._current_operation:
                    click.echo("\n⚠️ Cancelling current operation...")
                    cli_app._current_operation.cancel()
                    try:
                        await cli_app._current_operation
                    except asyncio.CancelledError:
                        pass
                else:
                    confirm_quit = InteractiveSelector.confirm_action("Exit OCIMgr?", default=True)
                    if confirm_quit:
                        break
            
    finally:
        await cli_app.cleanup()


async def _handle_list_resources(cli_app: OCIMgrAsyncCLI) -> None:
    """Handle listing compartments and resources with async operations"""
    compartments = await cli_app.list_compartments()
    if not compartments:
        return
    
    # Display compartments for selection
    compartment_names = [f"{comp['name']} ({comp['id'][:20]}...)" for comp in compartments]
    
    selected_index = InteractiveSelector.select_single(
        compartment_names, 
        "🎯 Select compartment to examine"
    )
    
    if selected_index is None:
        return
    
    selected_compartment = compartments[selected_index]
    click.echo(f"\n🔍 Examining compartment: {selected_compartment['name']}")
    
    # Discover resources with async progress
    resources = await cli_app.discover_resources([selected_compartment['id']])
    compartment_resources = resources.get(selected_compartment['id'], [])
    
    if not compartment_resources:
        click.echo("📭 No resources found in this compartment.")
        return
    
    # Display resources grouped by type
    click.echo(f"\n📦 Found {len(compartment_resources)} resources:")
    
    # Group by resource type for better display
    by_type = {}
    for resource in compartment_resources:
        resource_type = resource.info.resource_type
        if resource_type not in by_type:
            by_type[resource_type] = []
        by_type[resource_type].append(resource)
    
    for resource_type, type_resources in by_type.items():
        # Use match for type-specific icons (CORRECTED)
        icon = "📦"  # default
        match resource_type:
            case "compute_instance":
                icon = "🖥️"
            case "autonomous_database" | "mysql_db_system":
                icon = "🗄️"
            case "oke_cluster":
                icon = "☸️"
            case _:
                icon = "📦"
        
        click.echo(f"\n{icon} {resource_type.replace('_', ' ').title()} ({len(type_resources)}):")
        
        for resource in type_resources:
            protection = " 🔒" if resource.info.has_delete_protection else ""
            estimated_time = format_duration(resource.get_estimated_deletion_time())
            click.echo(f"  • {resource.info.name} ({resource.info.region}) - Est: {estimated_time}{protection}")


async def _handle_delete_resources(cli_app: OCIMgrAsyncCLI) -> None:
    """Handle resource deletion workflow with async operations"""
    compartments = await cli_app.list_compartments()
    if not compartments:
        return
    
    # Select compartments
    compartment_names = [f"{comp['name']} ({comp['id'][:20]}...)" for comp in compartments]
    
    selected_indices = InteractiveSelector.select_multiple(
        compartment_names,
        "🎯 Select compartment(s) for resource deletion"
    )
    
    if not selected_indices:
        return
    
    selected_compartments = [compartments[i] for i in selected_indices]
    compartment_ids = [comp['id'] for comp in selected_compartments]
    
    click.echo(f"\n✅ Selected {len(selected_compartments)} compartment(s):")
    for comp in selected_compartments:
        click.echo(f"  • {comp['name']}")
    
    # Discover resources with progress
    all_resources = await cli_app.discover_resources(compartment_ids)
    
    # Flatten all resources
    all_found_resources = []
    for comp_id, resources in all_resources.items():
        all_found_resources.extend(resources)
    
    if not all_found_resources:
        click.echo("📭 No resources found in selected compartments.")
        return
    
    # Handle resource selection for single compartment
    selected_resources = all_found_resources
    
    if len(selected_compartments) == 1:
        click.echo(f"\n📦 Found {len(all_found_resources)} resources.")
        selection_options = ["🗑️  Delete all resources", "🎯 Select specific resources"]
        
        delete_choice_index = InteractiveSelector.select_single(
            selection_options, 
            "Choose deletion scope"
        )
        
        if delete_choice_index == 1:  # Select specific resources
            resource_names = [
                f"{r.info.resource_type}: {r.info.name} ({r.info.region})" 
                for r in all_found_resources
            ]
            
            selected_resource_indices = InteractiveSelector.select_multiple(
                resource_names,
                "🎯 Select resources to delete"
            )
            
            if selected_resource_indices:
                selected_resources = [all_found_resources[i] for i in selected_resource_indices]
            else:
                return
    
    # Create deletion plan
    deletion_plan = await cli_app.create_deletion_plan(selected_resources)
    
    # Confirm deletion
    if not InteractiveSelector.confirm_action(
        f"\n⚠️  Are you sure you want to delete {len(deletion_plan)} resources?", 
        default=False
    ):
        click.echo("❌ Operation cancelled.")
        return
    
    # Option for dry run
    dry_run = InteractiveSelector.confirm_action(
        "🧪 Perform dry run first?", 
        default=True
    )
    
    if dry_run:
        await cli_app.execute_deletion(deletion_plan, dry_run=True)
        
        if not InteractiveSelector.confirm_action(
            "\n🚨 Proceed with actual deletion?", 
            default=False
        ):
            click.echo("❌ Operation cancelled.")
            return
    
    # Execute actual deletion
    await cli_app.execute_deletion(deletion_plan, dry_run=False)


async def _handle_export_data(cli_app: OCIMgrAsyncCLI) -> None:
    """Handle data export workflow with async operations"""
    compartments = await cli_app.list_compartments()
    if not compartments:
        return
    
    # Select compartment
    compartment_names = [f"{comp['name']} ({comp['id'][:20]}...)" for comp in compartments]
    
    selected_index = InteractiveSelector.select_single(
        compartment_names,
        "🎯 Select compartment to export"
    )
    
    if selected_index is None:
        return
    
    selected_compartment = compartments[selected_index]
    
    # Discover resources
    resources = await cli_app.discover_resources([selected_compartment['id']])
    compartment_resources = resources.get(selected_compartment['id'], [])
    
    if not compartment_resources:
        click.echo("📭 No resources found to export.")
        return
    
    # Select export format using corrected approach
    formats = ['json', 'csv']
    format_names = ['📄 JSON', '📊 CSV']
    
    format_index = InteractiveSelector.select_single(format_names, "📤 Select export format")
    
    if format_index is None:
        return
    
    export_format = formats[format_index]
    
    # Optional filename
    filename = click.prompt("📁 Enter filename (or press Enter for auto-generated)", 
                           default="", show_default=False)
    
    if not filename:
        filename = None
    
    # Export data
    success = await cli_app.export_data(compartment_resources, export_format, filename)
    
    if success:
        click.echo("✅ Export completed successfully!")
    else:
        click.echo("❌ Export failed!")


async def _handle_validate_compartment(cli_app: OCIMgrAsyncCLI) -> None:
    """Handle compartment validation with async operations"""
    compartments = await cli_app.list_compartments()
    if not compartments:
        return
    
    # Select compartment
    compartment_names = [f"{comp['name']} ({comp['id'][:20]}...)" for comp in compartments]
    
    selected_index = InteractiveSelector.select_single(
        compartment_names,
        "🎯 Select compartment to validate"
    )
    
    if selected_index is None:
        return
    
    selected_compartment = compartments[selected_index]
    click.echo(f"\n🔍 Validating compartment: {selected_compartment['name']}")
    
    # Check if compartment is empty
    resources = await cli_app.discover_resources([selected_compartment['id']])
    compartment_resources = resources.get(selected_compartment['id'], [])
    
    if not compartment_resources:
        click.echo("✅ Compartment is empty and ready for deletion!")
    else:
        click.echo(f"❌ Compartment contains {len(compartment_resources)} resources:")
        
        # Group by type
        by_type = {}
        for resource in compartment_resources:
            resource_type = resource.info.resource_type
            by_type[resource_type] = by_type.get(resource_type, 0) + 1
        
        for resource_type, count in by_type.items():
            click.echo(f"  • {resource_type}: {count}")


@cli.command()
@click.argument('compartment_id')
@click.option('--format', '-f', type=click.Choice(['table', 'json', 'csv']), 
              default='table', help='Output format')
@click.option('--output', '-o', help='Output file (stdout if not specified)')
@click.option('--resource-types', help='Comma-separated list of resource types to filter')
@click.option('--skip-unauthorized/--no-skip-unauthorized', default=True,
              help='Skip 401/403 unauthorized regions (default: on)')
@click.pass_context
@async_command
async def list_resources(ctx, compartment_id, format, output, resource_types, skip_unauthorized):
    """List resources in a specific compartment with async discovery"""
    cli_app = OCIMgrAsyncCLI(
        ctx.obj['config'], 
        ctx.obj['profile'], 
        ctx.obj['log_level']
    )
    
    try:
        await cli_app.initialize()
        
        # Parse resource type filter
        type_filter = None
        if resource_types:
            type_filter = [t.strip() for t in resource_types.split(',')]
        
        # Discover resources
        resources = await cli_app.discover_resources(
            [compartment_id],
            type_filter,
            skip_unauthorized=skip_unauthorized
        )
        compartment_resources = resources.get(compartment_id, [])
        
        if not compartment_resources:
            click.echo("No resources found in compartment.")
            return
        
        # Prepare data for output
        resource_data = []
        for resource in compartment_resources:
            resource_data.append({
                'name': resource.info.name,
                'type': resource.info.resource_type,
                'region': resource.info.region,
                'state': resource.info.lifecycle_state,
                'protected': resource.info.has_delete_protection,
                'estimated_deletion_time': resource.get_estimated_deletion_time()
            })
        
        # Format output using corrected match syntax
        content = ""
        match format:
            case 'table':
                content = OutputFormatter.format_table(resource_data)
            case 'json':
                content = OutputFormatter.format_json(resource_data)
            case 'csv':
                content = OutputFormatter.format_csv(resource_data)
            case _:
                content = "Unsupported format"
        
        # Output
        if output:
            if await cli_app.export_data(resource_data, format.replace('table', 'json'), output):
                click.echo(f"Output saved to {output}")
            else:
                click.echo("Failed to save output file", err=True)
        else:
            click.echo(content)
            
    finally:
        await cli_app.cleanup()


@cli.command()
@click.argument('compartment_id')
@click.option('--dry-run', is_flag=True, help='Perform dry run only')
@click.option('--yes', is_flag=True, help='Skip confirmation prompts')
@click.option('--resource-types', help='Comma-separated list of resource types to filter')
@click.option('--skip-unauthorized/--no-skip-unauthorized', default=True,
              help='Skip 401/403 unauthorized regions (default: on)')
@click.pass_context
@async_command
async def delete_all(ctx, compartment_id, dry_run, yes, resource_types, skip_unauthorized):
    """Delete all resources in a compartment with async operations"""
    cli_app = OCIMgrAsyncCLI(
        ctx.obj['config'], 
        ctx.obj['profile'], 
        ctx.obj['log_level']
    )
    
    try:
        await cli_app.initialize()
        
        # Parse resource type filter
        type_filter = None
        if resource_types:
            type_filter = [t.strip() for t in resource_types.split(',')]
        
        # Discover resources
        resources = await cli_app.discover_resources(
            [compartment_id],
            type_filter,
            skip_unauthorized=skip_unauthorized
        )
        compartment_resources = resources.get(compartment_id, [])
        
        if not compartment_resources:
            click.echo("No resources found in compartment.")
            return
        
        # Create deletion plan
        deletion_plan = await cli_app.create_deletion_plan(compartment_resources)
        
        # Confirm unless --yes flag is used
        if not yes:
            action = "simulate deletion of" if dry_run else "delete"
            if not click.confirm(f"Are you sure you want to {action} {len(deletion_plan)} resources?"):
                click.echo("Operation cancelled.")
                return
        
        # Execute deletion
        await cli_app.execute_deletion(deletion_plan, dry_run=dry_run)
        
    finally:
        await cli_app.cleanup()

# ================================================================================================
# ADD THESE COMMANDS TO YOUR EXISTING ocimgr/cli.py FILE
# Just paste this at the bottom, before "if __name__ == '__main__':"
# ================================================================================================

@cli.command("compartments")
@click.option('--format', '-f', type=click.Choice(['table', 'json', 'csv']), 
              default='table', help='Output format')
@click.option('--output', '-o', help='Save output to file')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.pass_context
@async_command
async def compartments_command(ctx, format, output, verbose):
    """📋 List all compartments across all regions (numbered for easy reference)"""
    
    if verbose:
        click.echo("🔍 Initializing OCI session...")
    
    config = OCIConfig(ctx.obj.get('config'), ctx.obj.get('profile', 'DEFAULT'))
    session = AsyncOCISession(config)
    await session.wait_until_ready()
    
    try:
        compartment_manager = CompartmentManager(session)
        
        if verbose:
            click.echo(f"🌍 Scanning regions: {', '.join(session.get_all_regions())}")
        
        # Get all compartments
        compartments = await compartment_manager.list_compartments()
        
        if not compartments:
            click.echo("📭 No compartments found.")
            return
        
        # Format output based on strategy
        formatted_output = ""
        match format.lower():
            case "json":
                numbered_compartments = [
                    {**comp, "number": i} 
                    for i, comp in enumerate(compartments, 1)
                ]
                formatted_output = OutputFormatter.format_json(numbered_compartments)
            
            case "csv":
                numbered_compartments = [
                    {**comp, "number": i} 
                    for i, comp in enumerate(compartments, 1)
                ]
                formatted_output = OutputFormatter.format_csv(numbered_compartments)
            
            case "table" | _:
                # Create a nice table format
                lines = [
                    "📋 Available Compartments (use numbers with 'ocimgr resources' command):",
                    "=" * 80
                ]
                
                for i, comp in enumerate(compartments, 1):
                    # Truncate long names for readability
                    name = comp['name'][:50] + "..." if len(comp['name']) > 50 else comp['name']
                    comp_id = comp['id'][-20:] if len(comp['id']) > 20 else comp['id']
                    
                    lines.append(f"{i:3d}. {name}")
                    lines.append(f"     ID: ...{comp_id}")
                    if comp.get('description'):
                        desc = comp['description'][:70] + "..." if len(comp['description']) > 70 else comp['description']
                        lines.append(f"     Desc: {desc}")
                    lines.append("")
                
                lines.append(f"💡 Usage: ocimgr resources 1 3 5  # List resources from compartments 1, 3, and 5")
                formatted_output = "\n".join(lines)
        
        # Display output
        click.echo(formatted_output)
        
        # Save to file if requested
        if output:
            try:
                with open(output, 'w') as f:
                    f.write(formatted_output)
                click.echo(f"💾 Output saved to {output}")
            except Exception as e:
                click.echo(f"❌ Failed to save to {output}: {e}")
        
        if verbose:
            click.echo(f"✅ Found {len(compartments)} compartments total")
    
    finally:
        await session.close()


@cli.command("resources")
@click.argument('compartment_numbers', nargs=-1, type=int, required=True)
@click.option('--format', '-f', type=click.Choice(['table', 'json', 'csv']), 
              default='table', help='Output format')
@click.option('--output', '-o', help='Save output to file')
@click.option('--types', '-t', help='Resource types: compute_instance,autonomous_database,mysql_db_system,oke_cluster')
@click.option('--skip-unauthorized/--no-skip-unauthorized', default=True,
              help='Skip 401/403 unauthorized regions (default: on)')
@click.option('--verbose', '-v', is_flag=True, help='Show detailed progress')
@click.pass_context
@async_command
async def resources_command(ctx, compartment_numbers, format, output, types, skip_unauthorized, verbose):
    """
    📦 List resources in specified compartments by number
    
    Examples:
      ocimgr resources 1              # Resources in compartment #1
      ocimgr resources 1 3 5          # Resources in compartments #1, #3, #5  
      ocimgr resources 2 --types compute_instance,autonomous_database
      ocimgr resources 1 --format json --output resources.json
    
    First run 'ocimgr compartments' to see the numbered list.
    """
    
    # Validate input
    if not compartment_numbers:
        click.echo("❌ Please specify at least one compartment number.")
        click.echo("💡 First run: ocimgr compartments")
        return
    
    if verbose:
        click.echo("🔍 Initializing OCI session...")
    
    config = OCIConfig(ctx.obj.get('config'), ctx.obj.get('profile', 'DEFAULT'))
    session = AsyncOCISession(config)
    await session.wait_until_ready()
    
    try:
        compartment_manager = CompartmentManager(session)
        discovery_engine = ResourceDiscoveryEngine(session)
        
        # Get all compartments first
        if verbose:
            click.echo("📋 Loading compartment list...")
        
        all_compartments = await compartment_manager.list_compartments()
        
        if not all_compartments:
            click.echo("❌ No compartments found.")
            return
        
        # Validate compartment numbers
        selected_compartments = []
        invalid_numbers = []
        
        for num in compartment_numbers:
            if 1 <= num <= len(all_compartments):
                selected_compartments.append(all_compartments[num - 1])
            else:
                invalid_numbers.append(num)
        
        if invalid_numbers:
            click.echo(f"⚠️ Invalid compartment numbers: {invalid_numbers}")
            click.echo(f"   Valid range: 1-{len(all_compartments)}")
            click.echo("💡 Run 'ocimgr compartments' to see the numbered list")
        
        if not selected_compartments:
            click.echo("❌ No valid compartments selected.")
            return
        
        # Parse resource type filter
        resource_type_filter = None
        if types:
            resource_type_filter = [t.strip() for t in types.split(',')]
            if verbose:
                click.echo(f"🔍 Filtering for types: {', '.join(resource_type_filter)}")
        
        # Show selected compartments
        if verbose:
            click.echo(f"🎯 Selected {len(selected_compartments)} compartment(s):")
            for comp in selected_compartments:
                click.echo(f"  • {comp['name']}")
        
        # Discover resources
        compartment_ids = [comp['id'] for comp in selected_compartments]
        
        if verbose:
            click.echo(f"🔍 Discovering resources across {len(session.get_all_regions())} regions...")

        all_resources = await discovery_engine.discover_all_resources(
            compartment_ids,
            resource_type_filter,
            skip_unauthorized=skip_unauthorized
        )
        # Flatten and organize results
        flattened_resources = []
        compartment_resource_counts = {}
        
        for comp_id, resources in all_resources.items():
            flattened_resources.extend(resources)
            # Find compartment name for reporting
            comp_name = next(
                (c['name'] for c in selected_compartments if c['id'] == comp_id),
                comp_id[:20]
            )
            compartment_resource_counts[comp_name] = len(resources)
        
        if not flattened_resources:
            click.echo("📭 No resources found in selected compartments.")
            if verbose:
                for comp_name, count in compartment_resource_counts.items():
                    click.echo(f"  • {comp_name}: {count} resources")
            return
        
        # Format output
        formatted_output = ""
        match format.lower():
            case "json":
                resource_data = [
                    {
                        "name": r.info.name,
                        "type": r.info.resource_type,
                        "region": r.info.region,
                        "state": r.info.lifecycle_state,
                        "protected": r.info.has_delete_protection,
                        "estimated_deletion_time": r.get_estimated_deletion_time(),
                        "ocid": r.info.ocid,
                        "compartment_id": r.info.compartment_id
                    }
                    for r in flattened_resources
                ]
                formatted_output = OutputFormatter.format_json(resource_data)
            
            case "csv":
                resource_data = [
                    {
                        "name": r.info.name,
                        "type": r.info.resource_type,
                        "region": r.info.region,
                        "state": r.info.lifecycle_state,
                        "protected": r.info.has_delete_protection,
                        "estimated_deletion_time": r.get_estimated_deletion_time(),
                        "ocid": r.info.ocid
                    }
                    for r in flattened_resources
                ]
                formatted_output = OutputFormatter.format_csv(resource_data)
            
            case "table" | _:
                # Group resources by type for better display
                by_type = {}
                for resource in flattened_resources:
                    resource_type = resource.info.resource_type
                    if resource_type not in by_type:
                        by_type[resource_type] = []
                    by_type[resource_type].append(resource)
                
                lines = [
                    f"📦 Found {len(flattened_resources)} resources in {len(selected_compartments)} compartment(s):",
                    "=" * 80
                ]
                
                for resource_type, type_resources in by_type.items():
                    # Get icon using match
                    icon = "📦"
                    match resource_type:
                        case "compute_instance":
                            icon = "🖥️"
                        case "autonomous_database" | "mysql_db_system":
                            icon = "🗄️"
                        case "oke_cluster":
                            icon = "☸️"
                        case "block_volume":
                            icon = "💾"
                        case _:
                            icon = "📦"
                    
                    lines.append(f"\n{icon} {resource_type.replace('_', ' ').title()} ({len(type_resources)}):")
                    lines.append("-" * 60)
                    
                    for resource in type_resources:
                        protection = " 🔒" if resource.info.has_delete_protection else ""
                        estimated_time = format_duration(resource.get_estimated_deletion_time())
                        
                        lines.extend([
                            f"  Name: {resource.info.name}{protection}",
                            f"  Region: {resource.info.region}",
                            f"  State: {resource.info.lifecycle_state}",
                            f"  Est. Deletion Time: {estimated_time}",
                            ""
                        ])
                
                formatted_output = "\n".join(lines)
        
        # Display results
        click.echo(formatted_output)
        
        # Save to file if requested
        if output:
            try:
                with open(output, 'w') as f:
                    f.write(formatted_output)
                click.echo(f"💾 Output saved to {output}")
            except Exception as e:
                click.echo(f"❌ Failed to save to {output}: {e}")
        
        # Summary
        if verbose:
            click.echo(f"\n📊 Summary:")
            for comp_name, count in compartment_resource_counts.items():
                click.echo(f"  • {comp_name}: {count} resources")
            click.echo(f"  • Total: {len(flattened_resources)} resources")
    
    finally:
        await session.close()


@cli.command("inventory")
@click.option('--format', '-f', type=click.Choice(['table', 'json', 'csv']),
              default='table', help='Output format')
@click.option('--output', '-o', help='Save output to file')
@click.option('--compartment-list', type=click.Path(dir_okay=False),
              help='Write qualified compartment list (comments + OCIDs) to file')
@click.option('--compartment', '-c', help='Compartment name to scope (includes sub-compartments)')
@click.option('--types', '-t', help='Resource types to include (comma-separated)')
@click.option('--max-concurrent', type=int, default=1, help='Max concurrent API calls per compartment')
@click.option('--compartment-concurrency', type=int, default=1, help='Max concurrent compartments to scan')
@click.option('--list-empty', is_flag=True, help='Include rows with zero counts')
@click.option('--discover-regions', is_flag=True, help='Rebuild cached regions from OCI')
@click.option('--skip-unauthorized/--no-skip-unauthorized', default=True,
              help='Skip 401/403 unauthorized regions (default: on)')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.pass_context
@async_command
async def inventory_command(
    ctx,
    format,
    output,
    compartment_list,
    compartment,
    types,
    max_concurrent,
    compartment_concurrency,
    list_empty,
    discover_regions,
    skip_unauthorized,
    verbose
):
    """
    📊 High-level inventory (counts-only) across compartments and regions.
    Defaults to all compartments; use --compartment to scope to a subtree.
    """
    cli_app = OCIMgrAsyncCLI(
        ctx.obj['config'],
        ctx.obj['profile'],
        ctx.obj['log_level']
    )

    try:
        await cli_app.initialize()

        logging.info("Starting inventory command")

        click.echo("🌍 Resolving regions (cache/subscriptions)...")
        cache_path = get_region_cache_path(ctx.obj.get('config'))
        cached_regions = None if discover_regions else load_region_cache(cache_path)

        if cached_regions:
            click.echo(f"🗃️  Using cached regions from {cache_path}")
            logging.info("Using cached regions from %s", cache_path)
            await refresh_session_regions(cli_app, cached_regions, verbose, "cached")
        else:
            if discover_regions:
                click.echo("♻️  Rebuilding region cache...")
            else:
                click.echo("🗃️  Region cache missing; rebuilding...")
            click.echo("🔍 Discovering subscribed regions (may take a moment)...")
            logging.info("Discovering subscribed regions")
            subscribed_regions = await discover_and_cache_regions(cli_app, cache_path, verbose)
            if subscribed_regions:
                click.echo(f"✅ Discovered {len(subscribed_regions)} subscribed regions")
                logging.info("Discovered %d subscribed regions", len(subscribed_regions))
                await refresh_session_regions(cli_app, subscribed_regions, verbose, "subscribed")
            elif verbose:
                click.echo("⚠️ No subscribed regions returned; using config default")

        click.echo("📋 Loading compartment list...")
        logging.info("Loading compartment list")

        all_compartments = await cli_app.list_compartments()
        if not all_compartments:
            click.echo("📭 No compartments found.")
            return

        logging.info("Loaded %d compartments", len(all_compartments))

        # Resolve compartment scope
        scoped_compartments = all_compartments
        if compartment:
            search_term = compartment.lower()
            matches = [
                comp for comp in all_compartments
                if search_term in comp['name'].lower()
            ]

            if not matches:
                click.echo(f"❌ No compartments matched '{compartment}'.")
                return

            # Use the highest-scoring match (exact prefix if present)
            matches.sort(key=lambda c: (c['name'].lower().startswith(search_term), c['name']), reverse=True)
            target = matches[0]

            def is_descendant(child_id: str, parent_id: str, parent_map: Dict[str, Optional[str]]) -> bool:
                current = child_id
                while current:
                    if current == parent_id:
                        return True
                    current = parent_map.get(current)
                return False

            parent_map = {comp['id']: comp.get('parent_id') for comp in all_compartments}
            scoped_compartments = [
                comp for comp in all_compartments
                if is_descendant(comp['id'], target['id'], parent_map)
            ]

            if verbose:
                click.echo(f"🎯 Scoped to compartment subtree: {target['name']}")
            logging.info("Scoped inventory to subtree: %s", target['name'])

        resource_type_filter = None
        if types:
            resource_type_filter = [t.strip() for t in types.split(',') if t.strip()]

        compartment_ids = [comp['id'] for comp in scoped_compartments]
        parent_map = {comp['id']: comp.get('parent_id') for comp in all_compartments}
        name_map = {comp['id']: comp['name'] for comp in all_compartments}

        def build_compartment_path(compartment_id: str) -> str:
            path_parts = []
            current_id = compartment_id
            while current_id:
                name = name_map.get(current_id, current_id)
                path_parts.append(name)
                parent_id = parent_map.get(current_id)
                if not parent_id or parent_id == current_id:
                    break
                current_id = parent_id
            return "/".join(reversed(path_parts))

        compartment_names = {
            comp_id: build_compartment_path(comp_id)
            for comp_id in compartment_ids
        }

        if compartment_list:
            list_lines = [
                "# Qualified compartment list (path then OCID)",
                "# Edit as needed and pass to delete-compartment --targets-file"
            ]
            for comp in scoped_compartments:
                display_name = compartment_names.get(comp['id'], comp['name'])
                list_lines.append(f"# {display_name}")
                list_lines.append(comp['id'])
                list_lines.append("")

            list_path = Path(compartment_list).expanduser().resolve()
            try:
                list_path.parent.mkdir(parents=True, exist_ok=True)
                list_path.write_text("\n".join(list_lines).rstrip() + "\n", encoding="utf-8")
                click.echo(f"📝 Compartment list written to {list_path}")
                logging.info("Wrote compartment list to %s", list_path)
            except Exception as exc:
                click.echo(f"❌ Failed to write compartment list to {list_path}: {exc}")
                logging.error("Failed to write compartment list to %s: %s", list_path, exc)

        click.echo(
            f"🔍 Scanning {len(compartment_ids)} compartments across "
            f"{len(cli_app.session.get_all_regions())} regions..."
        )
        logging.info(
            "Scanning %d compartments across %d regions",
            len(compartment_ids),
            len(cli_app.session.get_all_regions())
        )

        counts_by_compartment: Dict[str, Dict[str, Dict[str, Any]]] = {}
        total_compartments = len(compartment_ids)
        semaphore = asyncio.Semaphore(max(1, compartment_concurrency))
        progress_index = 0
        progress_lock = asyncio.Lock()

        async def scan_compartment(compartment_id: str) -> Dict[str, Dict[str, Any]]:
            nonlocal progress_index
            async with semaphore:
                async with progress_lock:
                    progress_index += 1
                    name = compartment_names.get(compartment_id, compartment_id)
                    click.echo(
                        f"➡️  [{progress_index}/{total_compartments}] Scanning compartment {name}"
                    )
                    logging.info(
                        "Inventory scan %d/%d for compartment %s",
                        progress_index,
                        total_compartments,
                        name
                    )

                return await cli_app.discover_fast_counts(
                    [compartment_id],
                    resource_type_filter=resource_type_filter,
                    max_concurrent=max_concurrent,
                    verbose=verbose,
                    skip_unauthorized=skip_unauthorized
                )

        tasks = [asyncio.create_task(scan_compartment(compartment_id)) for compartment_id in compartment_ids]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        for result in results:
            if isinstance(result, dict):
                counts_by_compartment.update(result)
            elif isinstance(result, Exception):
                logging.warning(f"Compartment scan failed: {result}")

        # Build rows for output
        rows = []
        totals_by_type: Dict[str, int] = {}
        for comp in scoped_compartments:
            comp_counts = counts_by_compartment.get(comp['id'], {})
            display_name = compartment_names.get(comp['id'], comp['name'])
            for resource_type, payload in comp_counts.items():
                count = payload.get("count", 0)
                regions = payload.get("regions", [])
                if count == 0 and not list_empty:
                    continue
                rows.append({
                    "compartment": display_name,
                    "resource_type": resource_type,
                    "count": count,
                    "regions": ", ".join(regions)
                })
                totals_by_type[resource_type] = totals_by_type.get(resource_type, 0) + count

        if not rows:
            click.echo("📭 No resources found in selected scope.")
            return

        # Format output
        if format == "json":
            content = OutputFormatter.format_json(rows)
        elif format == "csv":
            content = OutputFormatter.format_csv(rows)
        else:
            content = OutputFormatter.format_table(rows, headers=["compartment", "resource_type", "count", "regions"])

        if output:
            if OutputFormatter.save_to_file(content, output):
                click.echo(f"💾 Inventory saved to {output}")
            else:
                click.echo("❌ Failed to save output")
        else:
            click.echo(content)

        if totals_by_type:
            click.echo("\n📊 Totals by resource type:")
            for resource_type, count in sorted(totals_by_type.items()):
                click.echo(f"  • {resource_type}: {count}")
                logging.info("Inventory total %s: %d", resource_type, count)

        logging.info("Inventory command completed")

    finally:
        await cli_app.cleanup()


@cli.command("delete-compartment")
@click.argument('targets', nargs=-1, required=False)
@click.option('--targets-file', type=click.Path(exists=True, dir_okay=False),
              help='Path to a file containing one compartment name/OCID per line')
@click.option('--remaining-file', type=click.Path(dir_okay=False),
              help='Write remaining (not deleted) targets to this file')
@click.option('--skip-protected', is_flag=True,
              help='Skip targets containing delete-protected databases (report them for manual cleanup)')
@click.option('--dry-run', is_flag=True, help='Show deletion plan only (no changes)')
@click.option('--yes', is_flag=True, help='Skip confirmation prompts')
@click.option('--max-concurrent', type=int, default=3, help='Max concurrent API calls per compartment')
@click.option('--delete-retry-max', type=int, default=6, help='Max delete retries when throttled')
@click.option('--delete-retry-base-delay', type=float, default=0.75, help='Base delay (seconds) for delete backoff')
@click.option('--delete-retry-max-delay', type=float, default=12.0, help='Max delay (seconds) for delete backoff')
@click.option('--compartment-delay', type=float, default=0.0, help='Delay (seconds) between compartment deletes')
@click.option('--target-delay', type=float, default=0.0, help='Delay (seconds) between target compartments')
@click.option('--discover-regions', is_flag=True, help='Rebuild cached regions from OCI')
@click.option('--skip-unauthorized/--no-skip-unauthorized', default=True,
              help='Skip 401/403 unauthorized regions (default: on)')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.pass_context
@async_command
async def delete_compartment_command(
    ctx,
    targets,
    targets_file,
    remaining_file,
    skip_protected,
    dry_run,
    yes,
    max_concurrent,
    delete_retry_max,
    delete_retry_base_delay,
    delete_retry_max_delay,
    compartment_delay,
    target_delay,
    discover_regions,
    skip_unauthorized,
    verbose
):
    """
    🗑️ Delete one or more compartments by OCID or name (includes subtree).
    Finds resources, disables delete protection, and deletes in dependency order.
    """
    cli_app = OCIMgrAsyncCLI(
        ctx.obj['config'],
        ctx.obj['profile'],
        ctx.obj['log_level']
    )

    try:
        await cli_app.initialize()

        file_targets: List[str] = []
        if targets_file:
            try:
                targets_path = Path(targets_file)
                file_targets = [
                    line.strip() for line in targets_path.read_text(encoding="utf-8").splitlines()
                    if line.strip() and not line.strip().startswith("#")
                ]
            except Exception as e:
                click.echo(f"❌ Failed to read targets file {targets_file}: {e}")
                return

        combined_targets = list(targets) + file_targets
        combined_targets = [t.strip() for t in combined_targets if t.strip()]

        if not combined_targets:
            click.echo("❌ No compartment targets provided.")
            click.echo("💡 Provide targets as arguments or with --targets-file")
            logging.warning("delete-compartment invoked with no targets")
            return

        click.echo("🌍 Resolving regions (cache/subscriptions)...")
        cache_path = get_region_cache_path(ctx.obj.get('config'))
        cached_regions = None if discover_regions else load_region_cache(cache_path)

        if cached_regions:
            click.echo(f"🗃️  Using cached regions from {cache_path}")
            await refresh_session_regions(cli_app, cached_regions, verbose, "cached")
        else:
            if discover_regions:
                click.echo("♻️  Rebuilding region cache...")
            else:
                click.echo("🗃️  Region cache missing; rebuilding...")
            click.echo("🔍 Discovering subscribed regions (may take a moment)...")
            subscribed_regions = await discover_and_cache_regions(cli_app, cache_path, verbose)
            if subscribed_regions:
                click.echo(f"✅ Discovered {len(subscribed_regions)} subscribed regions")
                await refresh_session_regions(cli_app, subscribed_regions, verbose, "subscribed")
            elif verbose:
                click.echo("⚠️ No subscribed regions returned; using config default")

        all_compartments = await cli_app.list_compartments()
        if not all_compartments:
            click.echo("📭 No compartments found.")
            logging.warning("No compartments returned during delete-compartment")
            return

        # validate all provided targets (OCIDs or names)
        found = []
        missing = []
        
        # helper to build fully qualified path
        def get_qualified_path(compartment_id: str) -> str:
            path = []
            current = compartment_id
            while current:
                comp = next((c for c in all_compartments if c['id'] == current), None)
                if comp:
                    path.insert(0, comp['name'])
                    current = comp.get('parent_id')
                else:
                    break
            return '/'.join(path)
        
        # resolve each target (OCID or name) to a compartment object if possible
        for target in combined_targets:
            if target.startswith("ocid1.compartment"):
                comp = next((c for c in all_compartments if c['id'] == target), None)
                if comp:
                    found.append(comp)
                else:
                    missing.append(target)
            else:
                term = target.lower()
                matches = [c for c in all_compartments if term in c['name'].lower()]
                if matches:
                    matches.sort(key=lambda c: (c['name'].lower().startswith(term), c['name']), reverse=True)
                    found.append(matches[0])
                else:
                    missing.append(target)

        if found:
            click.echo("\n🔐 Validating specified compartments:")
            click.echo("The following compartments will be targeted for deletion:")
            for comp in found:
                qualified_path = get_qualified_path(comp['id'])
                click.echo(f"  • {qualified_path}")
        if missing:
            click.echo("\n⚠️  The following targets were not found and will be skipped:")
            for t in missing:
                click.echo(f"  • {t}")

        # always ask once before proceeding regardless of --yes; --yes only skips later prompts
        if not click.confirm(f"Continue with deletion of {len(found)} found compartments?", default=False):
            click.echo("❌ Operation cancelled.")
            logging.info("User cancelled delete-compartment confirmation prompt")
            return

        parent_map = {comp['id']: comp.get('parent_id') for comp in all_compartments}

        def is_descendant(child_id: str, parent_id: str) -> bool:
            current = child_id
            while current:
                if current == parent_id:
                    return True
                current = parent_map.get(current)
            return False

        home_region = None
        try:
            identity_client = await cli_app.session.get_client('identity')
            region_response = await AsyncResourceMixin._run_oci_operation(
                identity_client.list_region_subscriptions,
                tenancy_id=cli_app.session.oci_config['tenancy']
            )
            home_region = next(
                (r.region_name for r in region_response.data if getattr(r, "is_home_region", False)),
                None
            )
        except Exception as e:
            if verbose:
                click.echo(f"⚠️ Unable to resolve home region for compartment deletes: {e}")

        delete_region = home_region or cli_app.session.get_current_region()
        if verbose:
            click.echo(f"🏠 Using home region for compartment deletes: {delete_region}")
        logging.info(f"Compartment deletion region: {delete_region}")

        delete_success = True
        deleted_count = 0
        failed_count = 0
        resource_deleted_total = 0
        resource_failed_total = 0
        resource_target_total = 0
        was_cancelled = False
        skipped_protected: Dict[str, Dict[str, Any]] = {}

        def print_running_totals(label: str = "Running") -> None:
            click.echo(
                f"📊 {label} totals: resources deleted {resource_deleted_total}, "
                f"failed {resource_failed_total} (targets {resource_target_total}); "
                f"compartments deleted {deleted_count}, failed {failed_count}"
            )

        remaining_targets = []
        randomized_targets = combined_targets.copy()
        random.shuffle(randomized_targets)
        if verbose:
            click.echo("🔀 Randomized target processing order to reduce repeated blocking patterns.")

        for target in randomized_targets:
            target_compartment = None
            if target.startswith("ocid1.compartment"):
                target_compartment = next(
                    (comp for comp in all_compartments if comp['id'] == target),
                    None
                )
            else:
                search_term = target.lower()
                matches = [
                    comp for comp in all_compartments
                    if search_term in comp['name'].lower()
                ]
                if matches:
                    matches.sort(
                        key=lambda c: (c['name'].lower().startswith(search_term), c['name']),
                        reverse=True
                    )
                    target_compartment = matches[0]

            if not target_compartment:
                click.echo(f"❌ Compartment '{target}' not found.")
                logging.warning(f"Target compartment not found: {target}")
                delete_success = False
                remaining_targets.append(target)
                continue

            scoped_compartments = [
                comp for comp in all_compartments
                if is_descendant(comp['id'], target_compartment['id'])
            ]

            def get_depth(compartment_id: str) -> int:
                depth = 0
                current = compartment_id
                while current and current in parent_map:
                    parent_id = parent_map.get(current)
                    if not parent_id or parent_id == current:
                        break
                    depth += 1
                    current = parent_id
                return depth

            scoped_compartments = sorted(
                scoped_compartments,
                key=lambda comp: get_depth(comp['id']),
                reverse=True
            )

            logging.info(
                f"Processing target compartment {target_compartment['name']} with {len(scoped_compartments)} total in subtree"
            )
            click.echo(
                f"🎯 Target compartment: {target_compartment['name']}"
                f" ({len(scoped_compartments)} compartments in subtree)"
            )

            compartment_ids = [comp['id'] for comp in scoped_compartments]

            click.echo("🔍 Discovering resources for deletion plan...")

            async def attempt_discovery(retries:int=3) -> dict:
                """Try discovery repeatedly when we hit rate limits or circuits.

                Returns the resources dict on success or raises on final failure.
                """
                nonlocal max_concurrent
                for attempt in range(1, retries+1):
                    try:
                        return await cli_app.discover_resources(
                            compartment_ids,
                            skip_unauthorized=skip_unauthorized
                        )
                    except Exception as ee:
                        errstr = str(ee)
                        if "Circuit" in errstr or "429" in errstr:
                            click.echo(f"⚠️  Service rate limited or circuit open (attempt {attempt}): {ee}")
                            # reduce concurrency on either event to slow overall pace
                            new_max = max(1, max_concurrent - 1)
                            if new_max < max_concurrent:
                                click.echo(f"⚠️  Reducing --max-concurrent from {max_concurrent} to {new_max} due to throttling/circuit")
                                logging.warning(f"Concurrency reduced: {max_concurrent} → {new_max} due to {errstr[:80]}" )
                                max_concurrent = new_max
                            # pause before retrying
                            if attempt < retries:
                                wait = 30
                                click.echo(f"⏳  Pausing {wait}s before retrying discovery...")
                                await asyncio.sleep(wait)
                                continue
                        # not a rate/circuit error or no retries left
                        raise
                # unreachable
            
            try:
                resources_by_compartment = await attempt_discovery()
            except Exception as e:
                # final failure after retries
                click.echo(f"⚠️  Still rate limited after retries: {e}")
                click.echo(f"❌ Cannot proceed with compartment deletion. Remaining targets saved.")
                remaining_targets.append(target)
                delete_success = False
                continue

            all_resources = []
            for resources in resources_by_compartment.values():
                all_resources.extend(resources)

            protected_resources = [r for r in all_resources if r.info.has_delete_protection]
            if skip_protected and protected_resources:
                skipped_protected[target_compartment['id']] = {
                    'target': target,
                    'qualified_path': get_qualified_path(target_compartment['id']),
                    'resources': [
                        {
                            'resource_type': r.info.resource_type,
                            'name': r.info.name,
                            'region': r.info.region,
                            'ocid': r.info.ocid,
                            'compartment_id': r.info.compartment_id
                        }
                        for r in protected_resources
                    ]
                }
                click.echo(
                    f"⚠️  Skipping {target_compartment['name']} due to delete-protected resources "
                    f"({len(protected_resources)})."
                )
                remaining_targets.append(target)
                delete_success = False
                continue

            if not all_resources:
                click.echo("📭 No resources found in selected compartments.")
                click.echo("🗑️ Directly deleting empty compartments (no resource cleanup needed)...")
                logging.info(f"No resources found in subtree for {target_compartment['id']}")
                target_deleted = True
                failed_compartments: set[str] = set()

                def has_failed_descendant(comp_id: str) -> bool:
                    return any(
                        failed_id != comp_id and is_descendant(failed_id, comp_id)
                        for failed_id in failed_compartments
                    )

                for index, comp in enumerate(scoped_compartments, start=1):
                    click.echo(
                        f"🔁 Deleting compartment {index} of {len(scoped_compartments)}: {comp.get('name','<unknown>')}"
                    )
                    # For empty compartments, skip resource deletion entirely
                    if comp['id'] == target_compartment['id'] or comp.get('parent_id'):
                        if has_failed_descendant(comp['id']):
                            target_deleted = False
                            failed_count += 1
                            failed_compartments.add(comp['id'])
                            delete_success = False
                            click.echo(
                                f"⚠️  Skipping {comp['name']} because a child compartment failed to delete."
                            )
                            print_running_totals("Skipped")
                            continue

                        async def delete_operation() -> bool:
                            return await cli_app.compartment_manager.delete_compartment(
                                comp['id'],
                                region=delete_region
                            )

                        try:
                            success = await run_with_backoff(
                                delete_operation,
                                max_retries=delete_retry_max,
                                base_delay=delete_retry_base_delay,
                                max_delay=delete_retry_max_delay
                            )
                        except Exception as e:
                            success = False
                            error_str = str(e)
                            if "429" in error_str:
                                # Reduce concurrency on rate limit
                                new_max = max(1, max_concurrent - 1)
                                click.echo(f"⚠️  Rate limited (429). Reducing --max-concurrent from {max_concurrent} to {new_max}")
                                logging.warning(f"Rate limit (429) detected. Reduced concurrency: {max_concurrent} → {new_max}")
                                max_concurrent = new_max
                                # pause briefly to allow throttling to ease
                                click.echo("⏳  Sleeping 30s after rate limit before continuing")
                                await asyncio.sleep(30)
                            elif "Circuit" in error_str and "OPEN" in error_str:
                                logging.error(f"Circuit breaker opened for compartment deletion: {error_str}")
                                click.echo("⏳  Circuit breaker open; waiting 30s before next delete")
                                await asyncio.sleep(30)
                            if verbose:
                                click.echo(f"⚠️  Retry exhausted for {comp['name']}: {e}")

                        if success:
                            deleted_count += 1
                            logging.info(f"✅ Deleted compartment: {comp.get('name')} ({comp['id']})")
                        else:
                            delete_success = False
                            target_deleted = False
                            failed_count += 1
                            failed_compartments.add(comp['id'])
                            click.echo(f"❌ Failed to delete compartment {comp['name']}")
                            logging.warning(
                                f"❌ Failed to delete compartment: {comp.get('name')} ({comp['id']})"
                            )

                        print_running_totals()

                        if compartment_delay > 0 and index < len(scoped_compartments):
                            await asyncio.sleep(compartment_delay)

                if target_delay > 0:
                    await asyncio.sleep(target_delay)

                if not target_deleted:
                    remaining_targets.append(target)
                continue
            else:
                deletion_plan = await cli_app.create_deletion_plan(all_resources)
                protected_resources = [r for r in deletion_plan if r.info.has_delete_protection]

                if protected_resources:
                    click.echo(
                        f"🔒 {len(protected_resources)} resources have delete protection enabled."
                    )
                    for resource in protected_resources:
                        click.echo(f"  • {resource.info.resource_type}: {resource.info.name}")

                    if dry_run:
                        click.echo("🧪 Dry run mode enabled - delete protection will NOT be disabled.")
                    elif not yes:
                        if not click.confirm(
                            "Disable delete protection for these resources and continue?"
                        ):
                            click.echo("❌ Operation cancelled.")
                            delete_success = False
                            continue

                if dry_run:
                    click.echo("🧪 Dry run mode enabled - no changes will be made.")
                elif not yes:
                    if not click.confirm(
                        f"Proceed to delete {len(deletion_plan)} resources across "
                        f"{len(scoped_compartments)} compartments?"
                    ):
                        click.echo("❌ Operation cancelled.")
                        logging.info("User cancelled resource deletion confirmation")
                        delete_success = False
                        continue

                if not dry_run:
                    deletion_summary = await cli_app.execute_deletion(deletion_plan, dry_run=False)
                    resource_target_total += deletion_summary.get("total", 0)
                    resource_deleted_total += deletion_summary.get("successful", 0)
                    resource_failed_total += deletion_summary.get("failed", 0)
                    print_running_totals()

            if dry_run:
                click.echo("🧪 Dry run complete. No resources were deleted.")
                if target_delay > 0:
                    await asyncio.sleep(target_delay)
                remaining_targets.append(target)
                continue

            if not yes:
                if not click.confirm(
                    f"Delete {len(scoped_compartments)} compartments after cleanup?",
                    default=False
                ):
                    click.echo("❌ Compartment deletion cancelled.")
                    logging.info("User cancelled compartment deletion prompt")
                    delete_success = False
                    remaining_targets.append(target)
                    continue

            click.echo("🗑️ Deleting compartments (leaf to root, dependency aware)...")
            target_deleted = True
            failed_compartments: set[str] = set()

            def has_failed_descendant(comp_id: str) -> bool:
                return any(
                    failed_id != comp_id and is_descendant(failed_id, comp_id)
                    for failed_id in failed_compartments
                )

            for index, comp in enumerate(scoped_compartments, start=1):
                # progress indicator
                click.echo(
                    f"🔁 Deleting compartment {index} of {len(scoped_compartments)}: {comp.get('name','<unknown>')}"
                )
                if comp['id'] == target_compartment['id'] or comp.get('parent_id'):
                    if has_failed_descendant(comp['id']):
                        target_deleted = False
                        failed_count += 1
                        failed_compartments.add(comp['id'])
                        delete_success = False
                        click.echo(
                            f"⚠️  Skipping {comp['name']} because a child compartment failed to delete."
                        )
                        print_running_totals("Skipped")
                        continue

                    async def delete_operation() -> bool:
                        return await cli_app.compartment_manager.delete_compartment(
                            comp['id'],
                            region=delete_region
                        )

                    try:
                        success = await run_with_backoff(
                            delete_operation,
                            max_retries=delete_retry_max,
                            base_delay=delete_retry_base_delay,
                            max_delay=delete_retry_max_delay
                        )
                    except Exception as e:
                        success = False
                        if verbose:
                            click.echo(f"⚠️ Throttling retry exhausted for {comp['name']}: {e}")

                    if success:
                        deleted_count += 1
                    else:
                        delete_success = False
                        target_deleted = False
                        failed_count += 1
                        failed_compartments.add(comp['id'])
                        click.echo(f"❌ Failed to delete compartment {comp['name']}")
                        logging.warning(
                            f"Failed to delete compartment {comp.get('name')} ({comp['id']})"
                        )

                    print_running_totals()

                    if compartment_delay > 0 and index < len(scoped_compartments):
                        await asyncio.sleep(compartment_delay)

            if target_delay > 0:
                await asyncio.sleep(target_delay)

            if not target_deleted:
                remaining_targets.append(target)

        if remaining_file:
            try:
                remaining_path = Path(remaining_file)
                remaining_path.write_text("\n".join(remaining_targets) + "\n", encoding="utf-8")
                click.echo(f"📝 Remaining targets written to {remaining_file}")
            except Exception as e:
                click.echo(f"❌ Failed to write remaining targets file: {e}")
                logging.error(f"Failed to write remaining targets file {remaining_file}: {e}")
                delete_success = False

        if skipped_protected:
            click.echo("\n🔒 Targets skipped due to delete-protected databases:")
            for payload in skipped_protected.values():
                click.echo(f"  • {payload['qualified_path']}")
                for resource in payload['resources']:
                    click.echo(
                        "    - {resource_type}: {name} ({region}) {ocid}".format(**resource)
                    )

        click.echo(
            f"\n🔢 Summary: {deleted_count} compartments deleted, {failed_count} failures. "
            f"Resources deleted: {resource_deleted_total}, failed: {resource_failed_total} "
            f"(targets: {resource_target_total})."
        )
        if delete_success:
            click.echo("✅ Compartment deletion initiated successfully.")
        else:
            click.echo("⚠️ Some compartments failed to delete. Check logs for details.")

    except (KeyboardInterrupt, asyncio.CancelledError):
        was_cancelled = True
        click.echo("\n⚠️ Operation cancelled by user.")
        click.echo(
            f"🔢 Final totals (cancelled): compartments deleted {deleted_count}, "
            f"failed {failed_count}; resources deleted {resource_deleted_total}, "
            f"failed {resource_failed_total} (targets {resource_target_total})."
        )
    finally:
        if not was_cancelled:
            click.echo(
                f"🔢 Final totals: compartments deleted {deleted_count}, failed {failed_count}; "
                f"resources deleted {resource_deleted_total}, failed {resource_failed_total} "
                f"(targets {resource_target_total})."
            )
        await cli_app.cleanup()


if __name__ == '__main__':
    try:
        cli()
    except KeyboardInterrupt:
        click.echo("\n⚠️ Operation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        click.echo(f"💥 Unexpected error: {e}", err=True)
        sys.exit(1)