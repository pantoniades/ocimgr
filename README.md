"""
OCIMgr - Oracle Cloud Infrastructure Management Tool
"""

# OCIMgr


# OCIMgr

OCIMgr is a small toolkit for discovering and managing Oracle Cloud Infrastructure (OCI) resources asynchronously.

Quick start
-----------

1. Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# install test deps
pip install pytest pytest-asyncio
```

2. Run the CLI (example):

```bash
# after installing package (editable) or running via python -m
python -m ocimgr.cli inventory --compartment-list ./all-compartments.txt
```

Testing
-------

Run unit tests with pytest:

```bash
pytest -q
```

Notes
-----
- The project relies on OCI config and credentials. See OCI SDK documentation for configuring `~/.oci/config`.
- The tests in `tests/` are lightweight and use mocks to avoid network calls.


## ✅ Features (Current Scope)
- Multi-region discovery (async)
- High-level **inventory (counts-only)** to avoid throttling
- Interactive and scripted CLI usage
- JSON/CSV export for future bulk-delete workflows

## Installation
```bash
pip install -r requirements.txt
pip install -e .
```

## OCI Config Setup
OCIMgr reads OCI config from:
- `~/.oci/config` (default)
- `~/.oci_mgr/config`
- `./oci_mgr.ini`

### Example config with multiple regions
```ini
[DEFAULT]
user=ocid1.user.oc1..xxxx
fingerprint=xx:xx:xx
key_file=~/.oci/oci_api_key.pem
tenancy=ocid1.tenancy.oc1..xxxx
region=us-ashburn-1
regions=us-ashburn-1,us-phoenix-1
```

## Quick Start

### Write CLI output to a file (global)
```bash
ocimgr --output run.log inventory
```

### List compartments
```bash
ocimgr compartments
```

### High-level inventory (counts only, hides zero rows)
```bash
ocimgr inventory
```

OCIMgr stores a `.region_cache` JSON file next to your OCI config. If it is missing,
inventory will discover subscribed regions and create it.

### Force region discovery (refresh cache)
```bash
ocimgr inventory --discover-regions
```

### Skip unauthorized regions (ignore 401s)
```bash
ocimgr inventory --skip-unauthorized
```

### Inventory scoped to a compartment subtree
```bash
ocimgr inventory --compartment "Finance"
```

### Limit concurrency (avoid throttling)
```bash
ocimgr inventory --max-concurrent 1
```

### Show empty rows (optional)
```bash
ocimgr inventory --list-empty
```

### Export inventory for future bulk delete workflows
```bash
ocimgr inventory --format csv --output inventory.csv
```

### Delete a compartment (with dry-run)
```bash
ocimgr delete-compartment "Finance" --dry-run
```

### Delete a compartment while skipping unauthorized regions
```bash
ocimgr delete-compartment "Finance" --skip-unauthorized
```

### Delete a compartment by OCID (with confirmation)
```bash
ocimgr delete-compartment ocid1.compartment.oc1..xxxx
```

## Throttling Guidance
OCI will throttle list calls under heavy concurrency. Use:
- `--max-concurrent` to reduce simultaneous requests
- `--compartment` to narrow scope
- `--discover-regions` to refresh cached regions
- Prefer **inventory counts-only** mode before deep discovery

## Safety Guidance (Delete Compartment)
- Start with `--dry-run` to review the deletion plan.
- Deletion disables delete-protection automatically when supported.
- Compartments are deleted **leaf-to-root** after resources are removed.

## How `delete-compartment` Works
1. **Resolve target** by OCID or name (name searches and includes subtree).
2. **Discover resources** across the target compartment and descendants.
3. **Build deletion plan** ordered by dependency (`DeletionOrder`).
4. **Disable delete protection** where supported before deletion.
5. **Delete resources**, then delete compartments **leaf-to-root**.

Use `--dry-run` to validate the plan without making changes.

The inventory output includes a `regions` column listing where resources were found.

The CLI includes exponential backoff for 429 responses.

## Current Resource Types (Limited Scope)
- Compute Instances
- Autonomous Databases
- MySQL DB Systems
- OKE Clusters

## Roadmap (Next)
- Add block volumes, load balancers, networking, storage
- Cost/usage correlation for idle detection
- Bulk-delete workflows driven by inventory export

## TODO (Code Review Follow-ups)
- [x] Add compartment-level concurrency for inventory scans
- [x] Show compartment names in progress output (not just OCIDs)
- [ ] Add a `ocimgr regions` command to view the cached region list
- [ ] Add cache TTL/expiry for long-lived tenancies

