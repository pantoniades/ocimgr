import pytest
from types import SimpleNamespace
import ocimgr.core as core


@pytest.mark.asyncio
async def test_wait_for_state_immediate(monkeypatch):
    async def fake_run(operation, *args, **kwargs):
        return SimpleNamespace(data=SimpleNamespace(lifecycle_state='AVAILABLE'))

    monkeypatch.setattr(core.AsyncResourceMixin, '_run_oci_operation', fake_run)

    # get_operation not actually executed by fake_run, pass a dummy callable
    result = await core.AsyncResourceMixin._wait_for_state(lambda: None, ['AVAILABLE'], max_wait=1, poll_interval=0)
    assert result is True
