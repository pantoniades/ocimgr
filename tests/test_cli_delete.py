import asyncio
import os
from click.testing import CliRunner
import pytest

from ocimgr.cli import delete_compartment_command, OCIMgrAsyncCLI


class DummyCLI(OCIMgrAsyncCLI):
    def __init__(self, *args, **kwargs):
        # accept same signature as base class but ignore for testing
        super().__init__(*args, **kwargs)
        self.initialized = False

    async def initialize(self):
        self.initialized = True

    async def list_compartments(self):
        # return sample compartments
        return [
            {'id': 'ocid1.compartment.oc1..aaaa', 'name': 'Alpha'},
            {'id': 'ocid1.compartment.oc1..bbbb', 'name': 'Beta'},
        ]


def test_delete_compartment_confirm_step(monkeypatch, tmp_path):
    # monkeypatch CLI class used in command
    monkeypatch.setattr('ocimgr.cli.OCIMgrAsyncCLI', DummyCLI)
    # stub region refresh to avoid needing a real session
    async def dummy_refresh(cli_app, regions, verbose, label):
        return
    monkeypatch.setattr('ocimgr.cli.refresh_session_regions', dummy_refresh)

    # create a file with one valid and one invalid ocid
    file_path = tmp_path / "targets.txt"
    file_path.write_text("ocid1.compartment.oc1..aaaa\nocid1.compartment.oc1..cccc\n")

    runner = CliRunner()
    # invoke command; answer 'n' to prompt
    result = runner.invoke(
        delete_compartment_command,
        ['--targets-file', str(file_path)],
        input='n\n',
        catch_exceptions=False,
        obj={'config': None, 'profile': 'DEFAULT', 'log_level': 'INFO'}
    )
    assert result.exit_code == 0


# additional test using the real delete-compartments file if it exists

def test_delete_compartments_real_file(monkeypatch):
    real_path = "/Users/philip/mysql-docs/ocimgr_clone/delete-compartments.txt"
    if not os.path.exists(real_path):
        pytest.skip("real delete-compartments file not present")

    # build fake compartments list from file comments and ocids
    comps = []
    last_comment = None
    with open(real_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("#"):
                last_comment = line[1:].strip()
            elif line.startswith("ocid1.compartment"):
                name = last_comment or line
                comps.append({"id": line, "name": name})
                last_comment = None
    # patch DummyCLI implementation to return this list
    class RealDummyCLI(DummyCLI):
        async def list_compartments(self):
            return comps

    monkeypatch.setattr('ocimgr.cli.OCIMgrAsyncCLI', RealDummyCLI)
    async def dummy_refresh(cli_app, regions, verbose, label):
        return
    monkeypatch.setattr('ocimgr.cli.refresh_session_regions', dummy_refresh)

    runner = CliRunner()
    result = runner.invoke(
        delete_compartment_command,
        ['--targets-file', real_path],
        input='n\n',
        catch_exceptions=False,
        obj={'config': None, 'profile': 'DEFAULT', 'log_level': 'INFO'}
    )

    # should mention at least first few names and OCIDs
    assert comps[0]['id'] in result.output
    assert comps[0]['name'] in result.output
    assert 'Continue with deletion of' in result.output and 'found compartments?' in result.output
    assert result.exit_code == 0
