#!/usr/bin/env python3
"""
OCIMgr Region Cache
Discover, cache, and reload OCI region subscriptions.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, TYPE_CHECKING

import click
import oci

from ocimgr.core import (
    OCIConfig, AsyncOCISession, AsyncResourceMixin,
    ResourceDiscoveryEngine,
)
from ocimgr.cli_utils import _print_auth_help

if TYPE_CHECKING:
    from ocimgr.app import OCIMgrAsyncCLI


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
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        regions = payload.get("regions")
        if isinstance(regions, list) and all(isinstance(r, str) for r in regions):
            return [r.strip() for r in regions if r.strip()]
    except Exception:
        return None

    return None


def write_region_cache(cache_path: Path, regions: List[str], profile: str) -> None:
    """Persist regions to the cache file in JSON format."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "profile": profile,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "regions": sorted(set(regions)),
    }
    cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


async def refresh_session_regions(
    cli_app: "OCIMgrAsyncCLI",
    regions: List[str],
    verbose: bool,
    label: str,
) -> None:
    """Rebuild the session with an updated region list."""
    from ocimgr.models.compartment import CompartmentManager

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
    verbose: bool,
) -> List[str]:
    """Discover subscribed regions and persist them to the cache.

    Returns the list of subscribed region names, or an empty list if
    region discovery fails (e.g. auth errors).
    """
    try:
        identity_client = await cli_app.session.get_client("identity")
        region_response = await AsyncResourceMixin._run_oci_operation(
            identity_client.list_region_subscriptions,
            tenancy_id=cli_app.session.oci_config["tenancy"],
        )
    except oci.exceptions.ServiceError as e:
        if e.status in {401, 403}:
            _print_auth_help(e, cli_app.session.oci_config)
        else:
            click.echo(f"❌ Region discovery failed: {e.message}", err=True)
            logging.error("Region discovery failed (status=%s): %s", e.status, e)
        return []
    except Exception as e:
        click.echo(f"❌ Region discovery failed: {e}", err=True)
        logging.error("Region discovery failed: %s", e)
        return []

    subscribed_regions = [r.region_name for r in region_response.data]
    if subscribed_regions:
        write_region_cache(cache_path, subscribed_regions, cli_app.profile)
        if verbose:
            click.echo(f"🗃️  Cached {len(subscribed_regions)} regions to {cache_path}")
    return subscribed_regions
