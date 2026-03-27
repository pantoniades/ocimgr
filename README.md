"""
OCIMgr - Oracle Cloud Infrastructure Management Tool
"""

# OCIMgr

OCIMgr is a lightweight toolkit for discovering and managing Oracle Cloud Infrastructure (OCI) resources asynchronously.
It focuses on safe inventory collection and guided deletion workflows with clear logging and retry handling.

## ✅ Prerequisites
- **Python 3.10+**
- **OCI API key configured** and a valid `~/.oci/config` profile

## 📦 Installation

### 1) Create a virtual environment
```bash
python -m venv .venv
source .venv/bin/activate
```

### 2) Install dependencies
```bash
pip install -r requirements.txt
pip install -e .
```

> **Tip:** `pip install -e .` installs the `ocimgr` CLI so you can run commands like `ocimgr inventory` directly.

### 3) (Optional) Install test dependencies
```bash
pip install pytest pytest-asyncio
```

## 📤 Distribution (Sharing with New Users)

### Option 1: Install from a Git repo (recommended for teams)
```bash
pip install git+https://github.com/your-org/ocimgr.git
```

### Option 2: Build and share a wheel (recommended for internal releases)
```bash
python -m pip install --upgrade build
python -m build
ls dist/
# share the .whl file (e.g., ocimgr-0.1.0-py3-none-any.whl)
```
New users can install the wheel with:
```bash
pip install /path/to/ocimgr-0.1.0-py3-none-any.whl
```

### Option 3: Zip and run locally (simplest)
```bash
zip -r ocimgr.zip .
```
New users unzip and run:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
python -m ocimgr.cli inventory
```

## ✅ Quick Start

### Verify your OCI config and run inventory
```bash
# after installing package (editable) or running via python -m
ocimgr inventory --compartment-list ./all-compartments.txt
# or
python -m ocimgr.cli inventory --compartment-list ./all-compartments.txt
```

### Interactive mode
```bash
python -m ocimgr.cli interactive
```

## Testing

Run unit tests with pytest:

```bash
pytest -q
```

Run the inventory fast-count tests only:

```bash
pytest -q tests/test_inventory_fast_counts.py
```

## 🧭 Configuration

OCIMgr reads OCI config from:
- `~/.oci/config` (default OCI SDK location)
- `~/.oci_mgr/config`
- `./oci_mgr.ini`

You can also pass `--config` and `--profile` to the CLI.

### Required keys per profile
Each config profile **must** include:
- `user`
- `fingerprint`
- `key_file`
- `tenancy`
- `region`

If any required key is missing, OCIMgr will raise a clear error describing what is missing.

### Example config with multiple regions (placeholders)
```ini
[DEFAULT]
user=ocid1.user.oc1..xxxx
fingerprint=xx:xx:xx
key_file=~/.oci/oci_api_key.pem
tenancy=ocid1.tenancy.oc1..xxxx
region=us-ashburn-1
regions=us-ashburn-1,us-phoenix-1
```

### Region cache
OCIMgr stores a `.region_cache` JSON file next to your OCI config. If it is missing,
inventory will discover subscribed regions and create it.

## 🛠️ Troubleshooting Config Errors

| Symptom | What it means | Fix |
| --- | --- | --- |
| `Config file not found` | The config path does not exist | Pass `--config /path/to/config` or create `~/.oci/config`. |
| `profile 'X' not found` | The named profile section is missing | Add `[X]` section to the config file or use `--profile`. |
| `missing required keys` | Keys like `user`, `tenancy`, or `region` are missing | Add the missing keys to the profile. |
| `OCI API key file not found` | The `key_file` path does not exist | Update the path or generate a new API key. |

If you still see auth errors (401/403), confirm your API key is uploaded to OCI and the
user has the correct policies for the target compartments.

## Notes
- The project relies on OCI config and credentials. See OCI SDK documentation for configuring `~/.oci/config`.
- The tests in `tests/` are lightweight and use mocks to avoid network calls.
- **Log files contain resource identifiers** (OCIDs, compartment names). Avoid committing logs to Git.


## ✅ Features (Current Scope)
- Multi-region discovery (async)
- High-level **inventory (counts-only)** to avoid throttling
- Interactive and scripted CLI usage
- JSON/CSV export for future bulk-delete workflows

## CLI Examples

### Write CLI output to a file (global)
```bash
ocimgr --output run.log inventory
```

By default, OCIMgr writes a unique log file per run (e.g. `ocimgr-20250306-111755.log`).
Use `--output` to tee console output to a specific file; structured logs still go to
the auto-generated log file unless you override it in code.

Inventory now logs progress (regions, compartment scans, totals) at INFO level so long
runs will keep the log file active even when CLI output is quiet.

If inventory appears to stall, each regional list call now has a bounded timeout so
slow regions return sooner and logging continues.

### List compartments
```bash
ocimgr compartments
```

### Export full compartment list
```bash
ocimgr compartments --format csv --output compartments.csv
ocimgr compartments --format json --output compartments.json
```

If you see identity timeouts while listing compartments, ensure your OCI config
`region` is set to your tenancy’s home region. OCIMgr now uses the configured
home region for identity calls and retries transient failures automatically.

### High-level inventory (counts only, hides zero rows)
```bash
ocimgr inventory
```

### Inventory only specific resource types (including instance pools)
```bash
ocimgr inventory --types compute_instance,instance_pool,mysql_db_system
```

OCIMgr stores a `.region_cache` JSON file next to your OCI config. If it is missing,
inventory will discover subscribed regions and create it.

### Force region discovery (refresh cache)
```bash
ocimgr inventory --discover-regions
```

### Skip unauthorized regions (ignore 401/403)
Unauthorized regions are skipped automatically to reduce throttling. Use
`--no-skip-unauthorized` if you want strict behavior.
```bash
ocimgr inventory --skip-unauthorized
ocimgr inventory --no-skip-unauthorized
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

### Full compartment inventory (include zeros + save CSV)
```bash
ocimgr inventory --list-empty --format csv --output full-inventory.csv
```

### Delete a compartment (with dry-run)
```bash
ocimgr delete-compartment "Finance" --dry-run
```

### Delete multiple compartments from a targets file
```bash
ocimgr delete-compartment --targets-file ./delete-compartments-0307.txt --dry-run
```

### Delete a compartment without resource discovery (fast delete-only)
```bash
ocimgr delete-compartment "Finance" --no-discovery
```

### Auto-skip deep discovery when fast counts are zero (default on)
```bash
ocimgr delete-compartment "Finance" --auto-discovery
```

### Add a delay before compartment deletion pass
```bash
ocimgr delete-compartment "Finance" --retry-pass-delay 300
```

### Tune delete timeout and retries
```bash
ocimgr delete-compartment "Finance" --delete-timeout 180 --delete-retry-max 8
```

### Control compartment delete passes and cleanup verification
```bash
ocimgr delete-compartment "Finance" --compartment-delete-passes 3
ocimgr delete-compartment "Finance" --no-verify-cleanup
```

### Limit discovery to specific regions
```bash
ocimgr delete-compartment "Finance" --regions us-ashburn-1,us-phoenix-1
```

### Delete a compartment while skipping unauthorized regions
```bash
ocimgr delete-compartment "Finance" --skip-unauthorized
ocimgr delete-compartment "Finance" --no-skip-unauthorized
```

### Delete a compartment by OCID (with confirmation, placeholder OCID)
```bash
ocimgr delete-compartment ocid1.compartment.oc1..xxxx
```

### Delete a compartment while skipping delete-protected resources
```bash
ocimgr delete-compartment "Finance" --skip-protected
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
- A second pass (or more) deletes compartments after a configurable wait (`--retry-pass-delay`) to
  allow asynchronous cleanup to settle. Use `--compartment-delete-passes` to attempt more passes.
- Cleanup verification now uses a **fast counts-only** scan first, and only performs full discovery
  if counts are non-zero. Disable verification with `--no-verify-cleanup`.
- Use `--no-discovery` to skip resource scanning when you know the compartment is empty.
- Auto-discovery uses fast counts up front and skips full discovery when counts are zero (toggle with
  `--no-auto-discovery`).

## How `delete-compartment` Works
1. **Resolve target** by OCID or name (name searches and includes subtree).
2. **Discover resources** across the target compartment and descendants (skipped with `--no-discovery`; auto-skipped when
   fast counts are zero).
3. **Build deletion plan** ordered by dependency (`DeletionOrder`) and balanced round-robin by region.
4. **Disable delete protection** where supported before deletion.
5. **Delete resources**, queue compartments for deletion, verify cleanup, then delete compartments
   **leaf-to-root** with configurable passes (`--retry-pass-delay`, `--compartment-delete-passes`).

Cleanup verification uses fast counts first; only if counts are non-zero does it fall back to a
full discovery pass.

Use `--dry-run` to validate the plan without making changes.

The inventory output includes a `regions` column listing where resources were found.

The CLI includes exponential backoff for 429 responses.

## Current Resource Types (Limited Scope)
- Compute Instances
- Instance Pools
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

