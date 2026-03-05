import pytest
from types import SimpleNamespace
import oci

from ocimgr.models.compartment import CompartmentManager
import ocimgr.core as core


class DummyIdentityClient:
    def get_user(self, user_id):
        pass

    def list_compartments(self, compartment_id=None, access_level=None, compartment_id_in_subtree=None):
        pass

    def get_compartment(self, compartment_id):
        pass


class DummySession:
    def __init__(self):
        self.oci_config = {'user': 'test-user'}

    async def get_client(self, name, region=None):
        return DummyIdentityClient()


@pytest.mark.asyncio
async def test_list_compartments_returns_root_and_active(monkeypatch):
    # Fake responses for different identity calls
    async def fake_run(operation, *args, **kwargs):
        # operation may be a pagination helper or a client method
        if operation is oci.pagination.list_call_get_all_results:
            # simply delegate to the underlying client method passed as first arg
            inner = args[0]
            # simulate one compartment
            c = SimpleNamespace(
                id='c1',
                name='Comp1',
                description='d1',
                time_created='t1',
                lifecycle_state='ACTIVE'
            )
            return SimpleNamespace(data=[c])

        name = getattr(operation, '__name__', '')
        if name == 'get_user':
            return SimpleNamespace(data=SimpleNamespace(compartment_id='root-id'))
        if name == 'list_compartments':
            c = SimpleNamespace(
                id='c1',
                name='Comp1',
                description='d1',
                time_created='t1',
                lifecycle_state='ACTIVE'
            )
            return SimpleNamespace(data=[c])
        if name == 'get_compartment':
            return SimpleNamespace(data=SimpleNamespace(
                id='root-id', name='Root', description='rootdesc', time_created='t0', lifecycle_state='ACTIVE'
            ))
        return SimpleNamespace(data=None)

    monkeypatch.setattr(core.AsyncResourceMixin, '_run_oci_operation', fake_run)

    session = DummySession()
    manager = CompartmentManager(session)

    compartments = await manager.list_compartments(None)

    assert isinstance(compartments, list)
    assert len(compartments) == 2
    assert compartments[0]['id'] == 'root-id'
    assert compartments[1]['id'] == 'c1'
