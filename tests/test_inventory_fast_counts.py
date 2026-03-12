import asyncio
from types import SimpleNamespace

import pytest

import oci

from ocimgr.cli import OCIMgrAsyncCLI
import ocimgr.core as core


class DummyClient:
    def __init__(self, service: str, region: str) -> None:
        self.service = service
        self.region = region

    def list_instances(self, compartment_id=None):
        pass

    def list_autonomous_databases(self, compartment_id=None):
        pass

    def list_db_systems(self, compartment_id=None):
        pass

    def list_clusters(self, compartment_id=None):
        pass


class DummySession:
    def __init__(self, regions):
        self._regions = list(regions)
        self._unauthorized_regions = set()

    async def get_client(self, service: str, region: str = None):
        return DummyClient(service, region)

    def mark_region_unauthorized(self, region: str) -> None:
        self._unauthorized_regions.add(region)

    def get_authorized_regions(self):
        return [region for region in self._regions if region not in self._unauthorized_regions]

    def get_all_regions(self):
        return list(self._regions)


def _default_fake_run(counts_by_region):
    async def fake_run(operation, *args, **kwargs):
        assert operation is oci.pagination.list_call_get_all_results
        method = args[0]
        region = method.__self__.region
        count = counts_by_region.get(region, 0)
        return SimpleNamespace(data=list(range(count)))

    return fake_run


@pytest.mark.asyncio
async def test_discover_fast_counts_aggregates_regions(monkeypatch):
    counts_by_region = {
        "us-ashburn-1": 2,
        "us-phoenix-1": 1,
    }
    monkeypatch.setattr(core.AsyncResourceMixin, "_run_oci_operation", _default_fake_run(counts_by_region))

    cli = OCIMgrAsyncCLI()
    cli.session = DummySession(list(counts_by_region.keys()))

    result = await cli.discover_fast_counts(
        ["compartment-a"],
        resource_type_filter=["compute_instance"],
        max_concurrent=2,
        request_timeout=1.0,
    )

    payload = result["compartment-a"]["compute_instance"]
    assert payload["count"] == 3
    assert payload["regions"] == ["us-ashburn-1", "us-phoenix-1"]


@pytest.mark.asyncio
async def test_discover_fast_counts_timeout_returns_zero(monkeypatch):
    async def fake_run(operation, *args, **kwargs):
        assert operation is oci.pagination.list_call_get_all_results
        method = args[0]
        if method.__self__.region == "us-ashburn-1":
            raise asyncio.TimeoutError()
        return SimpleNamespace(data=list(range(2)))

    monkeypatch.setattr(core.AsyncResourceMixin, "_run_oci_operation", fake_run)

    cli = OCIMgrAsyncCLI()
    cli.session = DummySession(["us-ashburn-1", "us-phoenix-1"])

    result = await cli.discover_fast_counts(
        ["compartment-a"],
        resource_type_filter=["compute_instance"],
        max_concurrent=1,
        request_timeout=1.0,
    )

    payload = result["compartment-a"]["compute_instance"]
    assert payload["count"] == 2
    assert payload["regions"] == ["us-phoenix-1"]


@pytest.mark.asyncio
async def test_discover_fast_counts_cancellation(monkeypatch):
    async def fake_run(operation, *args, **kwargs):
        await asyncio.sleep(0.2)
        return SimpleNamespace(data=[1])

    monkeypatch.setattr(core.AsyncResourceMixin, "_run_oci_operation", fake_run)

    cli = OCIMgrAsyncCLI()
    cli.session = DummySession(["us-ashburn-1"])

    task = asyncio.create_task(
        cli.discover_fast_counts(
            ["compartment-a"],
            resource_type_filter=["compute_instance"],
            max_concurrent=1,
            request_timeout=1.0,
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task