#!/usr/bin/env python3
"""
OCIMgr Async CLI Application
Core application class for OCI resource management operations.
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable
from dataclasses import asdict
from enum import Enum

import click
import oci

from ocimgr.core import (
    OCIConfig, AsyncOCISession, setup_async_logging,
    get_registered_resource_types, AbstractOCIResource,
    ResourceDiscoveryEngine, OperationResult, ResourceStatus,
    AsyncResourceMixin,
)
from ocimgr.utils import (
    ProgressTracker, ProgressItem, OutputFormatter,
    format_duration, run_with_backoff,
)
from ocimgr.cli_utils import resource_icon, _print_auth_help
from ocimgr.models.compartment import CompartmentManager


class CLIAction(Enum):
    """Available CLI actions for interactive mode"""
    LIST_RESOURCES = "list_resources"
    DELETE_RESOURCES = "delete_resources"
    EXPORT_DATA = "export_data"
    VALIDATE_COMPARTMENT = "validate_compartment"
    QUIT = "quit"


class OCIMgrAsyncCLI:
    """
    Async CLI application class.
    Handles concurrent operations and provides interactive experience.
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        profile: str = "DEFAULT",
        log_level: str = "INFO",
    ):
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
            return await self.compartment_manager.list_compartments()
        except oci.exceptions.ServiceError as e:
            if e.status in {401, 403}:
                _print_auth_help(e, self.session.oci_config)
            else:
                click.echo(f"✗ Error listing compartments: {e.message}", err=True)
            return []
        except Exception as e:
            click.echo(f"✗ Error listing compartments: {e}", err=True)
            return []

    async def discover_resources(
        self,
        compartment_ids: List[str],
        resource_type_filter: Optional[List[str]] = None,
        skip_unauthorized: bool = True,
    ) -> Dict[str, List[AbstractOCIResource]]:
        """Discover resources with concurrent multi-region scanning."""
        click.echo(f"🔍 Discovering resources across {len(self.session.get_all_regions())} regions...")

        try:
            self._current_operation = asyncio.create_task(
                self.discovery_engine.discover_all_resources(
                    compartment_ids,
                    resource_type_filter,
                    skip_unauthorized=skip_unauthorized,
                )
            )

            progress_task = asyncio.create_task(self._show_discovery_progress())
            results = await self._current_operation

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

    async def discover_fast_counts(
        self,
        compartment_ids: List[str],
        resource_type_filter: Optional[List[str]] = None,
        max_concurrent: int = 1,
        verbose: bool = False,
        skip_unauthorized: bool = True,
        request_timeout: float = 60.0,
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Fast, counts-only discovery using list calls (no per-resource detail)."""
        resource_map = {
            "compute_instance": ("compute", "list_instances"),
            "instance_pool": ("compute_management", "list_instance_pools"),
            "autonomous_database": ("database", "list_autonomous_databases"),
            "mysql_db_system": ("mysql", "list_db_systems"),
            "oke_cluster": ("container_engine", "list_clusters"),
        }

        if resource_type_filter:
            resource_map = {
                name: value
                for name, value in resource_map.items()
                if name in resource_type_filter
            }

        results: Dict[str, Dict[str, int]] = {}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def list_count(
            compartment_id: str,
            resource_type: str,
            service: str,
            method_name: str,
            region: str,
        ) -> tuple[str, int]:
            async def operation():
                client = await self.session.get_client(service, region)
                method = getattr(client, method_name)
                response = await asyncio.wait_for(
                    AsyncResourceMixin._run_oci_operation(
                        oci.pagination.list_call_get_all_results,
                        method,
                        compartment_id=compartment_id,
                    ),
                    timeout=request_timeout,
                )
                terminal_states = {"TERMINATED", "TERMINATING", "DELETED", "DELETING"}
                active_count = 0
                for item in response.data:
                    lifecycle_state = getattr(item, "lifecycle_state", None)
                    if lifecycle_state and str(lifecycle_state).upper() in terminal_states:
                        continue
                    active_count += 1
                return region, active_count

            async with semaphore:
                try:
                    return await run_with_backoff(operation)
                except asyncio.TimeoutError:
                    logging.warning(
                        "Fast count timed out for %s in %s (%s)",
                        resource_type,
                        compartment_id,
                        region,
                    )
                    return region, 0
                except oci.exceptions.ServiceError as e:
                    if e.status in {401, 403}:
                        self.session.mark_region_unauthorized(region)
                        if skip_unauthorized:
                            if verbose:
                                logging.warning(
                                    "Skipping unauthorized region %s for %s: %s",
                                    region,
                                    resource_type,
                                    e,
                                )
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
                    len(regions),
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
                    "regions": sorted(set(regions_with_resources)),
                }
                logging.info(
                    "Fast count result for %s in compartment %s: %d",
                    resource_type,
                    compartment_id,
                    total_count,
                )

            results[compartment_id] = counts

        return results

    async def _show_discovery_progress(self) -> None:
        """Show animated progress during discovery"""
        spinner_chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        i = 0
        try:
            while True:
                click.echo(
                    f"\r{spinner_chars[i % len(spinner_chars)]} Scanning resources...",
                    nl=False,
                )
                await asyncio.sleep(0.1)
                i += 1
        except asyncio.CancelledError:
            click.echo("\r" + " " * 30 + "\r", nl=False)

    async def create_deletion_plan(
        self,
        resources: List[AbstractOCIResource],
        balance_by_region: bool = False,
    ) -> List[AbstractOCIResource]:
        """Create an optimized deletion plan with dependency analysis."""
        if not resources:
            return []

        sorted_resources = sorted(
            resources,
            key=lambda r: (r.get_deletion_order_priority(), r.get_estimated_deletion_time()),
        )

        if balance_by_region:
            sorted_resources = self._balance_deletion_plan_by_region(sorted_resources)
            click.echo("🌍 Balanced deletion plan by region (round-robin) within each deletion phase")

        click.echo(f"\n📋 Deletion Plan ({len(sorted_resources)} resources):")
        click.echo("=" * 60)

        total_time = 0
        for i, resource in enumerate(sorted_resources, 1):
            estimated_time = resource.get_estimated_deletion_time()
            total_time += estimated_time

            icon = resource_icon(resource.info.resource_type)
            protection_marker = " 🔒" if resource.info.has_delete_protection else ""

            click.echo(f"  {i:2d}. {icon} {resource.info.resource_type}: {resource.info.name}")
            click.echo(
                f"      Region: {resource.info.region} | Est: {format_duration(estimated_time)}{protection_marker}"
            )

        click.echo("=" * 60)
        click.echo(f"📊 Total estimated time: {format_duration(total_time)}")

        if any(r.info.has_delete_protection for r in sorted_resources):
            click.echo("🔒 Resources with delete protection will be processed first")

        return sorted_resources

    @staticmethod
    def _balance_deletion_plan_by_region(
        resources: List[AbstractOCIResource],
    ) -> List[AbstractOCIResource]:
        """Round-robin resources by region within each deletion order phase."""
        if not resources:
            return []

        by_order: Dict[int, Dict[str, List[AbstractOCIResource]]] = {}
        for resource in resources:
            order = resource.get_deletion_order_priority()
            region = resource.info.region or "unknown"
            by_order.setdefault(order, {}).setdefault(region, []).append(resource)

        balanced: List[AbstractOCIResource] = []
        for order in sorted(by_order.keys()):
            region_groups = by_order[order]
            for region_items in region_groups.values():
                region_items.sort(key=lambda r: r.get_estimated_deletion_time())

            regions = sorted(region_groups.keys())
            while any(region_groups[region] for region in regions):
                for region in regions:
                    if region_groups[region]:
                        balanced.append(region_groups[region].pop(0))

        return balanced

    async def execute_deletion(
        self,
        resources: List[AbstractOCIResource],
        dry_run: bool = True,
        delete_concurrency: int = 3,
        delete_retry_max: int = 6,
        delete_retry_base_delay: float = 0.75,
        delete_retry_max_delay: float = 12.0,
    ) -> Dict[str, Any]:
        """Execute resource deletion with async progress tracking."""
        if not resources:
            click.echo("No resources to delete.")
            return {"total": 0, "successful": 0, "failed": 0}

        action = "🧪 DRY RUN" if dry_run else "🗑️  DELETION"
        click.echo(f"\n{action} EXECUTION")
        click.echo("=" * 50)

        progress_items = [
            ProgressItem(f"{r.info.resource_type}: {r.info.name}", r.get_estimated_deletion_time())
            for r in resources
        ]

        tracker = ProgressTracker(progress_items, show_progress=not dry_run)
        tracker.start()

        results = []

        try:
            current_concurrency = max(1, delete_concurrency)
            throttle_detected = False

            def on_retry(exc: Exception, delay: float, attempt: int) -> None:
                nonlocal throttle_detected
                error_str = str(exc)
                if "429" in error_str or "Circuit" in error_str:
                    throttle_detected = True

            async def process_resource(i: int, resource: AbstractOCIResource) -> OperationResult:
                tracker.start_item(i)

                if dry_run:
                    click.echo(f"[DRY RUN] Would delete {resource.info.resource_type}: {resource.info.name}")
                    await asyncio.sleep(0.1)
                    result = OperationResult(
                        resource_ocid=resource.info.ocid,
                        operation="dry_run_delete",
                        status=ResourceStatus.COMPLETED,
                        message="Dry run successful",
                    )
                    tracker.complete_item(i)
                    return result

                return await self._delete_single_resource(
                    resource,
                    i,
                    tracker,
                    delete_retry_max=delete_retry_max,
                    delete_retry_base_delay=delete_retry_base_delay,
                    delete_retry_max_delay=delete_retry_max_delay,
                    on_retry=on_retry,
                )

            index = 0
            while index < len(resources):
                throttle_detected = False
                batch = resources[index : index + current_concurrency]
                deletion_tasks = [
                    process_resource(i, resource)
                    for i, resource in enumerate(batch, start=index)
                ]
                batch_results = await asyncio.gather(*deletion_tasks, return_exceptions=True)
                results.extend(batch_results)

                if throttle_detected and current_concurrency > 1:
                    current_concurrency = max(1, current_concurrency - 1)
                    click.echo(
                        f"⚠️  Throttling detected. Reducing delete concurrency to {current_concurrency}"
                    )

                index += len(batch)

        except asyncio.CancelledError:
            click.echo("\n⚠️ Deletion cancelled by user")
        except Exception as e:
            click.echo(f"\n✗ Deletion execution failed: {e}", err=True)
        finally:
            tracker.finish()
            self._current_operation = None

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
            "results": [r for r in results if isinstance(r, OperationResult)],
        }

        tracker.print_summary()
        self._print_deletion_summary(summary, dry_run)

        return summary

    async def _delete_single_resource(
        self,
        resource: AbstractOCIResource,
        index: int,
        tracker: ProgressTracker,
        delete_retry_max: int = 6,
        delete_retry_base_delay: float = 0.75,
        delete_retry_max_delay: float = 12.0,
        on_retry: Optional[Callable[[Exception, float, int], None]] = None,
    ) -> OperationResult:
        """Delete a single resource with error handling"""
        try:
            click.echo(f"🗑️  Deleting {resource.info.resource_type}: {resource.info.name}")

            # Refresh lifecycle state before acting
            try:
                client_map = {
                    "compute_instance": ("compute", "get_instance", "instance_id"),
                    "instance_pool": ("compute_management", "get_instance_pool", "instance_pool_id"),
                    "autonomous_database": ("database", "get_autonomous_database", "autonomous_database_id"),
                    "mysql_db_system": ("mysql", "get_db_system", "db_system_id"),
                    "oke_cluster": ("container_engine", "get_cluster", "cluster_id"),
                }
                service, method_name, id_param = client_map.get(
                    resource.info.resource_type, (None, None, None)
                )
                if service:
                    client = await self.session.get_client(service, resource.info.region)
                    method = getattr(client, method_name)
                    response = await run_with_backoff(
                        lambda m=method, p=id_param: AsyncResourceMixin._run_oci_operation(
                            m, **{p: resource.info.ocid}
                        ),
                        max_retries=delete_retry_max,
                        base_delay=delete_retry_base_delay,
                        max_delay=delete_retry_max_delay,
                        on_retry=on_retry,
                    )
                    if hasattr(response, "data") and hasattr(response.data, "lifecycle_state"):
                        resource.info.lifecycle_state = response.data.lifecycle_state
            except Exception as exc:
                logging.debug(
                    "Unable to refresh state for %s %s: %s",
                    resource.info.resource_type,
                    resource.info.ocid,
                    exc,
                )

            # Step 1: Disable delete protection
            if resource.info.has_delete_protection:
                protection_result = await run_with_backoff(
                    resource.disable_delete_protection,
                    max_retries=delete_retry_max,
                    base_delay=delete_retry_base_delay,
                    max_delay=delete_retry_max_delay,
                    on_retry=on_retry,
                )

                match protection_result.status:
                    case ResourceStatus.COMPLETED:
                        pass
                    case ResourceStatus.FAILED:
                        tracker.complete_item(index, "Failed to disable delete protection")
                        return protection_result
                    case _:
                        tracker.complete_item(
                            index, f"Unknown protection status: {protection_result.status}"
                        )
                        return protection_result

            # Step 2: Perform deletion
            deletion_result = await run_with_backoff(
                resource.delete,
                max_retries=delete_retry_max,
                base_delay=delete_retry_base_delay,
                max_delay=delete_retry_max_delay,
                on_retry=on_retry,
            )

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
                        message="Unknown status returned",
                    )

        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            tracker.complete_item(index, error_msg)
            return OperationResult(
                resource_ocid=resource.info.ocid,
                operation="delete",
                status=ResourceStatus.FAILED,
                message=error_msg,
            )

    def _print_deletion_summary(self, summary: Dict[str, Any], dry_run: bool) -> None:
        """Print a detailed deletion summary"""
        action = "Dry Run" if dry_run else "Deletion"

        click.echo(f"\n📊 {action} Summary:")
        click.echo("=" * 30)
        click.echo(f"Total resources: {summary['total']}")
        click.echo(f"✅ Successful: {summary['successful']}")
        click.echo(f"❌ Failed: {summary['failed']}")

        if summary["failed"] > 0:
            click.echo("\n❌ Failed Resources:")
            for result in summary["results"]:
                match result.status:
                    case ResourceStatus.FAILED:
                        click.echo(f"  • {result.resource_ocid}: {result.message}")

    async def export_data(
        self,
        data: Any,
        format_type: str,
        filename: Optional[str] = None,
    ) -> bool:
        """Export data to file in specified format asynchronously."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ocimgr_export_{timestamp}.{format_type}"

        try:
            export_data = data
            match format_type:
                case "json" | "csv":
                    if isinstance(data, list) and data and hasattr(data[0], "info"):
                        export_data = [asdict(item.info) for item in data]
                case _:
                    click.echo(f"Unsupported format: {format_type}")
                    return False

            content = ""
            match format_type:
                case "json":
                    content = OutputFormatter.format_json(export_data)
                case "csv":
                    content = OutputFormatter.format_csv(export_data)
                case _:
                    content = ""

            success = await asyncio.get_running_loop().run_in_executor(
                None,
                OutputFormatter.save_to_file,
                content,
                filename,
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
