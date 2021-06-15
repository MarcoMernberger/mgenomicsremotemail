import pytest
import sys
from prompt_toolkit.application import create_app_session
from prompt_toolkit.input import create_pipe_input
from prompt_toolkit.output import DummyOutput
import re
import runpy
from mgenomicsremotemail.dispatch import RunDispatcher
from pathlib import Path
from argparse import ArgumentParser as ArgParser
from pprint import pprint
from mock import patch


@pytest.fixcture
def mock_input():
    pipe_input = create_pipe_input()
    try:
        with create_app_session(input=pipe_input, output=DummyOutput()):
            yield pipe_input
    finally:
        pipe_input.close()

class TestRun:
    exec_file = str(Path(__file__, '..', '..', 'src', 'mgenomicsremotemail', 'bin', 'send_run').resolve())

    @pytest.mark.parametrize('executable, argument', [(exec_file, "--ids")])
    def test_script_execution_ids(self, executable, argument, capsys):
        assert isinstance(executable, str)
        sys.argv = [executable, argument]
        try:
            runpy.run_path(executable, run_name="__main__")
        except SystemExit:
            captured = capsys.readouterr().out
            assert len(captured.split("\n")) >= 500

    @pytest.mark.parametrize('executable, argument', [(exec_file, "--check")])  #, (exec_file, "--check"), (exec_file, "--help")])
    def test_script_execution_check(self, executable, argument, capsys):
        assert isinstance(executable, str)
        sys.argv = [executable, argument]
        try:
            runpy.run_path(executable, run_name="__main__")
        except SystemExit:
            captured = capsys.readouterr().out
            assert len(re.findall(" ok", captured)) >= 100

    @pytest.mark.parametrize('executable, argument', [(exec_file, "--help")])
    def test_script_execution_help(self, executable, argument, capsys):
        assert isinstance(executable, str)
        sys.argv = [executable, argument]
        try:
            runpy.run_path(executable, run_name="__main__")
        except SystemExit:
            captured = capsys.readouterr().out
            assert "help" in captured

    @pytest.mark.parametrize('executable', [exec_file])
    def test_script_execution(self, executable, capsys, monkeypatch):
        try:
            runpy.run_path(executable, run_name="__main__")
        except SystemExit:
            captured = capsys.readouterr().out
            print(captured)
            assert 1 == 2
