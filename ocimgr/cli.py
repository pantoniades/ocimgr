#!/usr/bin/env python3
"""
OCIMgr CLI Interface
Command-line interface for OCI resource management
"""

import sys
import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import click
import oci
import random

from ocimgr.core import (
    OCIConfig, AsyncOCISession,
    get_registered_resource_types,
    ResourceDiscoveryEngine, ResourceStatus,
    AsyncResourceMixin,
)
from ocimgr.utils import (
    OutputFormatter, InteractiveSelector, format_duration, run_with_backoff,
)
from ocimgr.cli_utils import (
    resource_icon, async_command, install_output_tee,
)
from ocimgr.app import OCIMgrAsyncCLI, CLIAction
from ocimgr.region_cache import (
    get_region_cache_path, load_region_cache,
    refresh_session_regions, discover_and_cache_regions,
)
from ocimgr.models.compartment import CompartmentManager


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
    install_output_tee(output)


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
        icon = resource_icon(resource_type)
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
                    icon = resource_icon(resource_type)
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
@click.option('--max-concurrent', type=int, default=3, help='Max concurrent API calls per compartment (default: 3)')
@click.option('--compartment-concurrency', type=int, default=3, help='Max concurrent compartments to scan (default: 3)')
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
            else:
                click.echo(
                    f"⚠️ Region discovery failed; falling back to config default: "
                    f"{', '.join(cli_app.session.get_all_regions())}"
                )

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
@click.option('--compartment-concurrency', type=int, default=2,
              help='Max concurrent target compartments to process')
@click.option('--delete-retry-max', type=int, default=6, help='Max delete retries when throttled')
@click.option('--delete-retry-base-delay', type=float, default=1.5, help='Base delay (seconds) for delete backoff')
@click.option('--delete-retry-max-delay', type=float, default=30.0, help='Max delay (seconds) for delete backoff')
@click.option('--compartment-delay', type=float, default=0.0, help='Delay (seconds) between compartment deletes')
@click.option('--target-delay', type=float, default=0.0, help='Delay (seconds) between target compartments')
@click.option('--retry-pass-delay', type=float, default=120.0,
              help='Delay (seconds) between resource cleanup and compartment delete retry pass')
@click.option('--compartment-delete-passes', type=int, default=2,
              help='Number of compartment delete passes to attempt')
@click.option('--verify-cleanup/--no-verify-cleanup', default=True,
              help='Re-discover resources before deleting compartments (default: on)')
@click.option('--no-discovery', is_flag=True,
              help='Skip resource discovery and attempt compartment deletion directly')
@click.option('--auto-discovery/--no-auto-discovery', default=True,
              help='Auto-skip full discovery when fast counts find zero resources (default: on)')
@click.option('--delete-timeout', type=int, default=120,
              help='Timeout (seconds) for compartment delete API calls')
@click.option('--regions', help='Comma-separated list of regions to limit discovery')
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
    compartment_concurrency,
    delete_retry_max,
    delete_retry_base_delay,
    delete_retry_max_delay,
    compartment_delay,
    target_delay,
    retry_pass_delay,
    compartment_delete_passes,
    verify_cleanup,
    no_discovery,
    auto_discovery,
    delete_timeout,
    regions,
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

    was_cancelled = False
    deleted_count = 0
    failed_count = 0
    resource_deleted_total = 0
    resource_failed_total = 0
    resource_target_total = 0

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
            else:
                click.echo(
                    f"⚠️ Region discovery failed; falling back to config default: "
                    f"{', '.join(cli_app.session.get_all_regions())}"
                )

        if regions:
            requested_regions = [r.strip() for r in regions.split(',') if r.strip()]
            if requested_regions:
                click.echo(f"🎯 Limiting discovery to regions: {', '.join(requested_regions)}")
                await refresh_session_regions(cli_app, requested_regions, verbose, "requested")

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
        skipped_protected_resources: List[Dict[str, Any]] = []
        pending_deletions: List[Dict[str, Any]] = []
        mysql_db_systems: List[Dict[str, Any]] = []
        failed_resource_deletions: List[Dict[str, Any]] = []

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

        if not yes and not dry_run and compartment_concurrency > 1:
            click.echo(
                "⚠️  Interactive prompts detected; forcing --compartment-concurrency to 1."
            )
            compartment_concurrency = 1

        target_semaphore = asyncio.Semaphore(max(1, compartment_concurrency))
        state_lock = asyncio.Lock()
        concurrency_state = {"max": max_concurrent}

        async def process_target(target: str) -> None:
            nonlocal delete_success
            nonlocal resource_target_total
            nonlocal resource_deleted_total
            nonlocal resource_failed_total
            async with target_semaphore:
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
                    async with state_lock:
                        delete_success = False
                        remaining_targets.append(target)
                    return

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

                if no_discovery:
                    click.echo("⚡ No-discovery mode enabled; skipping resource scan.")
                    if dry_run:
                        click.echo("🧪 Dry run mode enabled - skipping compartment deletes.")
                        if target_delay > 0:
                            await asyncio.sleep(target_delay)
                        async with state_lock:
                            remaining_targets.append(target)
                        return

                    async with state_lock:
                        pending_deletions.append({
                            "target": target,
                            "qualified_path": get_qualified_path(target_compartment['id']),
                            "target_compartment": target_compartment,
                            "scoped_compartments": scoped_compartments
                        })
                    if target_delay > 0:
                        await asyncio.sleep(target_delay)
                    return

                if auto_discovery:
                    click.echo("⚡ Auto-discovery enabled; running fast counts first...")
                    fast_counts = await cli_app.discover_fast_counts(
                        [comp['id'] for comp in scoped_compartments],
                        max_concurrent=max(1, max_concurrent),
                        verbose=verbose,
                        skip_unauthorized=skip_unauthorized,
                        request_timeout=45.0
                    )
                    remaining_fast = 0
                    for payload in fast_counts.values():
                        for metrics in payload.values():
                            remaining_fast += metrics.get("count", 0)

                    if remaining_fast == 0:
                        click.echo(
                            "✅ Fast counts show zero resources; skipping full discovery and queueing for delete."
                        )
                        logging.info(
                            "auto_discovery_fast_counts=0 for %s",
                            get_qualified_path(target_compartment['id'])
                        )
                        if dry_run:
                            click.echo("🧪 Dry run mode enabled - skipping compartment deletes.")
                            if target_delay > 0:
                                await asyncio.sleep(target_delay)
                            async with state_lock:
                                remaining_targets.append(target)
                            return

                        async with state_lock:
                            pending_deletions.append({
                                "target": target,
                                "qualified_path": get_qualified_path(target_compartment['id']),
                                "target_compartment": target_compartment,
                                "scoped_compartments": scoped_compartments
                            })
                        if target_delay > 0:
                            await asyncio.sleep(target_delay)
                        return

                click.echo("🔍 Discovering resources for deletion plan...")

                async def attempt_discovery(retries: int = 3) -> dict:
                    """Try discovery repeatedly when we hit rate limits or circuits.

                    Returns the resources dict on success or raises on final failure.
                    """
                    for attempt in range(1, retries + 1):
                        try:
                            return await cli_app.discover_resources(
                                compartment_ids,
                                skip_unauthorized=skip_unauthorized
                            )
                        except Exception as ee:
                            errstr = str(ee)
                            if "Circuit" in errstr or "429" in errstr:
                                click.echo(
                                    f"⚠️  Service rate limited or circuit open (attempt {attempt}): {ee}"
                                )
                                async with state_lock:
                                    current_max = concurrency_state["max"]
                                    new_max = max(1, current_max - 1)
                                    if new_max < current_max:
                                        concurrency_state["max"] = new_max
                                        click.echo(
                                            f"⚠️  Reducing --max-concurrent from {current_max} to {new_max} due to throttling/circuit"
                                        )
                                        logging.warning(
                                            "Concurrency reduced: %s → %s due to %s",
                                            current_max,
                                            new_max,
                                            errstr[:80]
                                        )
                                if attempt < retries:
                                    wait = 30
                                    click.echo(f"⏳  Pausing {wait}s before retrying discovery...")
                                    await asyncio.sleep(wait)
                                    continue
                            raise

                try:
                    resources_by_compartment = await attempt_discovery()
                except Exception as e:
                    click.echo(f"⚠️  Still rate limited after retries: {e}")
                    click.echo(f"❌ Cannot proceed with compartment deletion. Remaining targets saved.")
                    async with state_lock:
                        remaining_targets.append(target)
                        delete_success = False
                    return

                all_resources = []
                for resources in resources_by_compartment.values():
                    all_resources.extend(resources)

                async with state_lock:
                    for resource in all_resources:
                        if resource.info.resource_type == "mysql_db_system":
                            mysql_db_systems.append({
                                "compartment_path": get_qualified_path(resource.info.compartment_id),
                                "compartment_id": resource.info.compartment_id,
                                "name": resource.info.name,
                                "region": resource.info.region,
                                "ocid": resource.info.ocid,
                                "protected": resource.info.has_delete_protection
                            })

                protected_resources = [r for r in all_resources if r.info.has_delete_protection]
                if skip_protected and protected_resources:
                    async with state_lock:
                        skipped_protected[target_compartment['id']] = {
                            'target': target,
                            'qualified_path': get_qualified_path(target_compartment['id']),
                            'resources': [
                                {
                                    'resource_type': r.info.resource_type,
                                    'name': r.info.name,
                                    'region': r.info.region,
                                    'ocid': r.info.ocid,
                                    'compartment_id': r.info.compartment_id,
                                    'compartment_path': get_qualified_path(r.info.compartment_id)
                                }
                                for r in protected_resources
                            ]
                        }
                        skipped_protected_resources.extend(
                            skipped_protected[target_compartment['id']]['resources']
                        )
                        remaining_targets.append(target)
                        delete_success = False
                    click.echo(
                        f"⚠️  Skipping {target_compartment['name']} due to delete-protected resources "
                        f"({len(protected_resources)})."
                    )
                    return

                if not all_resources:
                    click.echo("📭 No resources found in selected compartments.")
                    click.echo("🗂️ Queueing empty compartments for delete pass (no resource cleanup needed)...")
                    logging.info(f"No resources found in subtree for {target_compartment['id']}")
                    if dry_run:
                        click.echo("🧪 Dry run mode enabled - skipping compartment deletes.")
                        if target_delay > 0:
                            await asyncio.sleep(target_delay)
                        async with state_lock:
                            remaining_targets.append(target)
                        return

                    async with state_lock:
                        pending_deletions.append({
                            "target": target,
                            "qualified_path": get_qualified_path(target_compartment['id']),
                            "target_compartment": target_compartment,
                            "scoped_compartments": scoped_compartments
                        })
                    if target_delay > 0:
                        await asyncio.sleep(target_delay)
                    return

                deletion_plan = await cli_app.create_deletion_plan(
                    all_resources,
                    balance_by_region=True
                )
                protected_resources = [r for r in deletion_plan if r.info.has_delete_protection]

                if protected_resources:
                    click.echo(
                        f"🔒 {len(protected_resources)} resources have delete protection enabled."
                    )
                    for resource in protected_resources:
                        compartment_path = (
                            get_qualified_path(resource.info.compartment_id)
                            or resource.info.compartment_id
                        )
                        region = resource.info.region or "unknown"
                        click.echo(
                            "  • {resource_type}: {name} | {region} | {compartment_path} | {ocid}".format(
                                resource_type=resource.info.resource_type,
                                name=resource.info.name,
                                region=region,
                                compartment_path=compartment_path,
                                ocid=resource.info.ocid
                            )
                        )

                    if dry_run:
                        click.echo("🧪 Dry run mode enabled - delete protection will NOT be disabled.")
                    elif not yes:
                        if not click.confirm(
                            "Disable delete protection for these resources and continue?"
                        ):
                            click.echo("❌ Operation cancelled.")
                            async with state_lock:
                                delete_success = False
                            return

                if dry_run:
                    click.echo("🧪 Dry run mode enabled - no changes will be made.")
                elif not yes:
                    if not click.confirm(
                        f"Proceed to delete {len(deletion_plan)} resources across "
                        f"{len(scoped_compartments)} compartments?"
                    ):
                        click.echo("❌ Operation cancelled.")
                        logging.info("User cancelled resource deletion confirmation")
                        async with state_lock:
                            delete_success = False
                        return

                if not dry_run:
                    resource_index = {r.info.ocid: r for r in deletion_plan}
                    async with state_lock:
                        delete_concurrency = concurrency_state["max"]
                    deletion_summary = await cli_app.execute_deletion(
                        deletion_plan,
                        dry_run=False,
                        delete_concurrency=delete_concurrency,
                        delete_retry_max=delete_retry_max,
                        delete_retry_base_delay=delete_retry_base_delay,
                        delete_retry_max_delay=delete_retry_max_delay
                    )
                    async with state_lock:
                        resource_target_total += deletion_summary.get("total", 0)
                        resource_deleted_total += deletion_summary.get("successful", 0)
                        resource_failed_total += deletion_summary.get("failed", 0)
                        for result in deletion_summary.get("results", []):
                            if result.status == ResourceStatus.FAILED:
                                resource = resource_index.get(result.resource_ocid)
                                compartment_path = (
                                    get_qualified_path(resource.info.compartment_id)
                                    if resource
                                    else "unknown"
                                )
                                failed_resource_deletions.append({
                                    "resource_type": resource.info.resource_type if resource else "unknown",
                                    "name": resource.info.name if resource else "unknown",
                                    "region": resource.info.region if resource else "unknown",
                                    "ocid": result.resource_ocid,
                                    "compartment_path": compartment_path,
                                    "protected": resource.info.has_delete_protection if resource else False,
                                    "error": result.message
                                })
                    print_running_totals()

                if dry_run:
                    click.echo("🧪 Dry run complete. No resources were deleted.")
                    if target_delay > 0:
                        await asyncio.sleep(target_delay)
                    async with state_lock:
                        remaining_targets.append(target)
                    return

                if not yes:
                    if not click.confirm(
                        f"Delete {len(scoped_compartments)} compartments after cleanup?",
                        default=False
                    ):
                        click.echo("❌ Compartment deletion cancelled.")
                        logging.info("User cancelled compartment deletion prompt")
                        async with state_lock:
                            delete_success = False
                            remaining_targets.append(target)
                        return

                async with state_lock:
                    pending_deletions.append({
                        "target": target,
                        "qualified_path": get_qualified_path(target_compartment['id']),
                        "target_compartment": target_compartment,
                        "scoped_compartments": scoped_compartments
                    })
                if target_delay > 0:
                    await asyncio.sleep(target_delay)

        target_tasks = [asyncio.create_task(process_target(target)) for target in randomized_targets]
        target_results = await asyncio.gather(*target_tasks, return_exceptions=True)
        for result in target_results:
            if isinstance(result, Exception):
                logging.error("Target processing failed: %s", result)

        async def execute_compartment_deletions() -> None:
            nonlocal delete_success, deleted_count, failed_count, max_concurrent
            if not pending_deletions:
                return

            for pass_index in range(1, max(1, compartment_delete_passes) + 1):
                if pass_index > 1 and retry_pass_delay > 0:
                    click.echo(
                        f"⏳ Waiting {retry_pass_delay:.0f}s before compartment delete pass {pass_index}..."
                    )
                    await asyncio.sleep(retry_pass_delay)

                click.echo(
                    "🗑️ Starting compartment delete pass "
                    f"{pass_index}/{max(1, compartment_delete_passes)} (leaf to root, dependency aware)..."
                )

                next_pending_deletions: List[Dict[str, Any]] = []

                for entry in pending_deletions:
                    scoped_compartments = entry["scoped_compartments"]
                    target_compartment = entry["target_compartment"]

                    if verify_cleanup:
                        click.echo(
                            f"🔁 Verifying cleanup for {entry['qualified_path']} before delete pass..."
                        )
                        fast_counts = await cli_app.discover_fast_counts(
                            [comp['id'] for comp in scoped_compartments],
                            max_concurrent=max(1, max_concurrent),
                            verbose=verbose,
                            skip_unauthorized=skip_unauthorized,
                            request_timeout=45.0
                        )
                        remaining_fast = 0
                        for payload in fast_counts.values():
                            for metrics in payload.values():
                                remaining_fast += metrics.get("count", 0)

                        if remaining_fast == 0:
                            logging.info(
                                "verify_cleanup_fast_counts=0 for %s",
                                entry["qualified_path"]
                            )
                        else:
                            resources_after_cleanup = await cli_app.discover_resources(
                                [comp['id'] for comp in scoped_compartments],
                                skip_unauthorized=skip_unauthorized
                            )
                            remaining_resources = [
                                res
                                for res_list in resources_after_cleanup.values()
                                for res in res_list
                            ]
                            if remaining_resources:
                                click.echo(
                                    f"⚠️  {len(remaining_resources)} resources still present; deferring compartment delete."
                                )
                                remaining_summary = []
                                for resource in remaining_resources:
                                    remaining_summary.append({
                                        "resource_type": resource.info.resource_type,
                                        "name": resource.info.name,
                                        "region": resource.info.region,
                                        "ocid": resource.info.ocid,
                                        "compartment_path": get_qualified_path(resource.info.compartment_id)
                                        or resource.info.compartment_id
                                    })
                                click.echo("  Remaining resources (sorted by compartment/region):")
                                for resource_entry in sorted(
                                    remaining_summary,
                                    key=lambda r: (r.get("compartment_path", ""), r.get("region", ""), r.get("name", ""))
                                ):
                                    click.echo(
                                        "    - {resource_type}: {name} | {region} | {compartment_path} | {ocid}".format(**resource_entry)
                                    )
                                delete_success = False
                                next_pending_deletions.append(entry)
                                continue

                    click.echo(
                        f"🎯 Deleting compartment tree for {entry['qualified_path']} "
                        f"({len(scoped_compartments)} compartments)"
                    )

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
                                    region=delete_region,
                                    timeout_seconds=delete_timeout
                                )

                            try:
                                success = await run_with_backoff(
                                    delete_operation,
                                    max_retries=delete_retry_max,
                                    base_delay=delete_retry_base_delay,
                                    max_delay=delete_retry_max_delay,
                                    retry_log_label="compartment_delete"
                                )
                            except Exception as e:
                                success = False
                                error_str = str(e)
                                if "429" in error_str:
                                    new_max = max(1, max_concurrent - 1)
                                    click.echo(
                                        f"⚠️  Rate limited (429). Reducing --max-concurrent from {max_concurrent} to {new_max}"
                                    )
                                    logging.warning(
                                        "Rate limit (429) detected. Reduced concurrency: %s → %s",
                                        max_concurrent,
                                        new_max
                                    )
                                    max_concurrent = new_max
                                    click.echo("⏳  Sleeping 30s after rate limit before continuing")
                                    await asyncio.sleep(30)
                                elif "Circuit" in error_str and "OPEN" in error_str:
                                    logging.error(
                                        "Circuit breaker opened for compartment deletion: %s",
                                        error_str
                                    )
                                    click.echo("⏳  Circuit breaker open; waiting 30s before next delete")
                                    await asyncio.sleep(30)
                                if verbose:
                                    click.echo(f"⚠️  Retry exhausted for {comp['name']}: {e}")

                            if success:
                                deleted_count += 1
                                logging.info(
                                    "✅ Deleted compartment: %s (%s)",
                                    comp.get('name'),
                                    comp['id']
                                )
                            else:
                                delete_success = False
                                target_deleted = False
                                failed_count += 1
                                failed_compartments.add(comp['id'])
                                click.echo(f"❌ Failed to delete compartment {comp['name']}")
                                logging.warning(
                                    "Failed to delete compartment %s (%s)",
                                    comp.get('name'),
                                    comp['id']
                                )

                            print_running_totals()

                            if compartment_delay > 0 and index < len(scoped_compartments):
                                await asyncio.sleep(compartment_delay)

                    if target_delay > 0:
                        await asyncio.sleep(target_delay)

                    if not target_deleted:
                        next_pending_deletions.append(entry)

                pending_deletions.clear()
                pending_deletions.extend(next_pending_deletions)

                if not pending_deletions:
                    break

        if not dry_run:
            await execute_compartment_deletions()

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
            click.echo("\n🔒 Targets skipped due to delete-protected resources:")
            for payload in skipped_protected.values():
                click.echo(f"  • {payload['qualified_path']}")
            if skipped_protected_resources:
                click.echo("  Protected resources (sorted by compartment/region):")
                for resource in sorted(
                    skipped_protected_resources,
                    key=lambda r: (r.get("compartment_path", ""), r.get("region", ""), r.get("name", ""))
                ):
                    click.echo(
                        "    - {resource_type}: {name} ({region}) {ocid} [{compartment_path}]".format(**resource)
                    )

        if mysql_db_systems:
            click.echo("\n🗄️  MySQL DB systems requiring deletion:")
            for system in sorted(
                mysql_db_systems,
                key=lambda s: (s.get("compartment_path", ""), s.get("region", ""), s.get("name", ""))
            ):
                protection_flag = " 🔒" if system.get("protected") else ""
                click.echo(
                    "  • {compartment_path} | {region} | {name}{protection_flag} | {ocid}".format(
                        **system,
                        protection_flag=protection_flag
                    )
                )

        if failed_resource_deletions:
            click.echo("\n⚠️  Resources that failed to delete (manual cleanup needed):")
            for resource in sorted(
                failed_resource_deletions,
                key=lambda r: (r.get("compartment_path", ""), r.get("region", ""), r.get("name", ""))
            ):
                protected_flag = " 🔒" if resource.get("protected") else ""
                click.echo(
                    "  • {compartment_path} | {region} | {resource_type} | {name}{protected_flag} | {ocid} | {error}".format(
                        **resource,
                        protected_flag=protected_flag
                    )
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