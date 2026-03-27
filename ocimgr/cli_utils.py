#!/usr/bin/env python3
"""
OCIMgr CLI Utilities
Shared helpers for the CLI layer: auth diagnostics, async wrappers, output tee, icons.
"""

import sys
import asyncio
import functools
import logging
from pathlib import Path
from typing import Optional

import click
import oci


# ── Resource type icons ──────────────────────────────────────────────────────

RESOURCE_ICONS = {
    "compute_instance": "\U0001f5a5\ufe0f",      # 🖥️
    "instance_pool": "\U0001f9f0",                 # 🧰
    "autonomous_database": "\U0001f5c4\ufe0f",    # 🗄️
    "mysql_db_system": "\U0001f5c4\ufe0f",        # 🗄️
    "oke_cluster": "\u2638\ufe0f",                 # ☸️
    "block_volume": "\U0001f4be",                  # 💾
}

_DEFAULT_ICON = "\U0001f4e6"  # 📦


def resource_icon(resource_type: str) -> str:
    """Return the emoji icon for a resource type."""
    return RESOURCE_ICONS.get(resource_type, _DEFAULT_ICON)


# ── Auth diagnostics ─────────────────────────────────────────────────────────

_auth_help_shown = False


def _print_auth_help(error: oci.exceptions.ServiceError, oci_config: dict) -> None:
    """Print actionable guidance for OCI authentication failures.

    Only prints the full checklist once per process; subsequent calls show a
    short reminder so the output doesn't get overwhelming.
    """
    global _auth_help_shown
    request_id = getattr(error, "request_id", None)
    config_user = oci_config.get("user", "")
    config_fingerprint = oci_config.get("fingerprint", "")
    config_key_file = oci_config.get("key_file", "")
    config_tenancy = oci_config.get("tenancy", "")

    if _auth_help_shown:
        click.echo(
            f"\n❌ OCI authentication failed again ({error.status} {error.code}, "
            f"operation={getattr(error, 'operation_name', 'unknown')}). "
            "See checklist above.",
            err=True,
        )
    else:
        _auth_help_shown = True
        click.echo(
            f"\n❌ OCI authentication failed ({error.status} {error.code}).\n"
            "\n"
            "  Checklist:\n"
            f"  1. API key fingerprint matches the one uploaded to OCI console\n"
            f"     Config fingerprint: {config_fingerprint}\n"
            f"     Verify at: OCI Console → Identity → Users → API Keys\n"
            "\n"
            f"  2. Private key file exists and matches the uploaded public key\n"
            f"     Config key_file: {config_key_file}\n"
            f"     Test: openssl rsa -in {config_key_file} -check -noout\n"
            "\n"
            f"  3. User OCID is correct and the user is not disabled\n"
            f"     Config user: {config_user}\n"
            "\n"
            f"  4. Tenancy OCID is correct\n"
            f"     Config tenancy: {config_tenancy}\n"
            "\n"
            "  5. Clock skew: OCI rejects requests with >5 min clock drift\n"
            "     Test: date -u && curl -s --head https://objectstorage.us-ashburn-1.oraclecloud.com | grep Date\n"
            "\n"
            "  Common causes:\n"
            "  - API key was deleted/rotated in the OCI console but config still references the old fingerprint\n"
            "  - Wrong profile selected (use --profile to switch)\n"
            "  - User account was deactivated or moved to a different tenancy\n",
            err=True,
        )
    if request_id:
        click.echo(f"  OCI request ID: {request_id}\n", err=True)
    logging.error(
        "OCI auth failed: status=%s code=%s request_id=%s user=%s fingerprint=%s",
        error.status,
        error.code,
        request_id or "n/a",
        config_user[-20:] if config_user else "n/a",
        config_fingerprint or "n/a",
    )


# ── Async Click wrapper ─────────────────────────────────────────────────────


def async_command(f):
    """Decorator to make Click commands async-aware."""
    @functools.wraps(f)
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


# ── Output tee ───────────────────────────────────────────────────────────────


class _Tee:
    """Duplicate writes to multiple streams."""

    def __init__(self, *streams):
        self._streams = streams

    def write(self, data):
        for stream in self._streams:
            stream.write(data)
        return len(data)

    def flush(self):
        for stream in self._streams:
            stream.flush()

    # Needed by tqdm / Click colour detection
    @property
    def encoding(self):
        return getattr(self._streams[0], "encoding", "utf-8")

    def isatty(self):
        return getattr(self._streams[0], "isatty", lambda: False)()

    def fileno(self):
        return self._streams[0].fileno()


def install_output_tee(output_path: Optional[str]) -> None:
    """Duplicate stdout/stderr to a file if *output_path* is provided."""
    if not output_path:
        return

    log_path = Path(output_path).expanduser().resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    log_file = log_path.open("a", encoding="utf-8")
    sys.stdout = _Tee(sys.stdout, log_file)
    sys.stderr = _Tee(sys.stderr, log_file)
