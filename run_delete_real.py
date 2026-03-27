#!/usr/bin/env python3
import os
from click.testing import CliRunner

from ocimgr.cli import delete_compartment_command, OCIMgrAsyncCLI

# create dummy CLI subclass that mimics list_compartments from file
class DummyCLI(OCIMgrAsyncCLI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.initialized = False

    async def initialize(self):
        self.initialized = True

    async def list_compartments(self):
        path = os.environ.get("OCIMGR_TARGETS_FILE", "./delete-compartments.txt")
        comps = []
        last_comment = None
        try:
            with open(path, encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith('#'):
                        last_comment = line[1:].strip()
                    elif line.startswith('ocid1.compartment'):
                        name = last_comment or line
                        comps.append({'id': line, 'name': name})
                        last_comment = None
        except Exception as e:
            print('failed to read file', e)
        return comps

async def dummy_refresh(cli_app, regions, verbose, label):
    return

import ocimgr.cli as cli_module
cli_module.OCIMgrAsyncCLI = DummyCLI
cli_module.refresh_session_regions = dummy_refresh

if __name__ == '__main__':
    runner = CliRunner()
    result = runner.invoke(
        delete_compartment_command,
        ['--targets-file', os.environ.get("OCIMGR_TARGETS_FILE", "./delete-compartments.txt")],
        input='n\n',
        obj={'config':None,'profile':'DEFAULT','log_level':'INFO'}
    )
    print('---CLI output---')
    print(result.output)
    print('exit code', result.exit_code)
