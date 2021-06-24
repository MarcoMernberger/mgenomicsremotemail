import pytest
import sys
import re
import runpy
from pathlib import Path
from mock import patch
from conftest import MockApp


class TestRun:
    exec_file = str(Path(__file__, '..', '..', 'src', 'mgenomicsremotemail', 'bin', 'run.py').resolve())

    @pytest.mark.parametrize('executable, argument', [(exec_file, "--ids")])
    def test_script_execution_ids(self, executable, argument, capsys):
        assert isinstance(executable, str)
        sys.argv = [executable, argument]
        try:
            runpy.run_path(executable, run_name="__main__")
        except SystemExit:
            captured = capsys.readouterr().out
            assert len(captured.split("\n")) >= 500
            assert "Existing run ids:" in captured

    @pytest.mark.parametrize('executable, argument', [(exec_file, "--check")])
    def test_script_execution_check(self, executable, argument, capsys):
        assert isinstance(executable, str)
        sys.argv = [executable, argument]
        try:
            runpy.run_path(executable, run_name="__main__")
        except SystemExit:
            captured = capsys.readouterr().out
            assert len(re.findall(" ok", captured)) >= 100
            assert "Checking all Run IDs:" in captured

    @pytest.mark.parametrize('executable, argument', [(exec_file, "--help")])
    def test_script_execution_help(self, executable, argument, capsys):
        assert isinstance(executable, str)
        sys.argv = [executable, argument]
        try:
            runpy.run_path(executable, run_name="__main__")
        except SystemExit:
            captured = capsys.readouterr().out
            assert "help" in captured
            assert "Usage:" in captured

    @pytest.mark.parametrize('executable', [exec_file])
    def test_script_execution(self, executable, capsys):
        sys.argv = [executable]
        try:
            with patch("mgenomicsremotemail.dispatch.RunDispatcher.run", return_value="Run completed"):
                runpy.run_path(executable, run_name="__main__")
                captured = capsys.readouterr().out
                print(captured)
        except SystemExit:
            captured = capsys.readouterr().out
            assert "Run completed" in captured

    @pytest.mark.parametrize('executable', [exec_file])
    def test_script_execution_no_run_ids(self, executable, capsys):
        sys.argv = [executable]
        try:
            with patch("mgenomicsremotemail.dispatch.RunDispatcher._get_run_id_app", return_value=MockApp([])):
                runpy.run_path(executable, run_name="__main__")
                captured = capsys.readouterr().out
                print(captured)
        except SystemExit:
            captured = capsys.readouterr().out
            assert "Exit" in captured

    @pytest.mark.parametrize('executable', [exec_file])
    def test_script_execution_abort_run_ids(self, executable, capsys):
        sys.argv = [executable]
        try:
            with patch("mgenomicsremotemail.dispatch.RunDispatcher._get_run_id_app", return_value=MockApp(None)):
                runpy.run_path(executable, run_name="__main__")
                captured = capsys.readouterr().out
                print(captured)
        except SystemExit:
            captured = capsys.readouterr().out
            assert "Aborted" in captured

    @pytest.mark.parametrize('executable', [exec_file])
    def test_script_execution_abort_emails(self, executable, capsys):
        sys.argv = [executable]
        try:
            with patch("mgenomicsremotemail.dispatch.RunDispatcher._get_run_id_app", return_value=MockApp("12345")):
                with patch("mgenomicsremotemail.dispatch.RunDispatcher._get_email_app", return_value=MockApp(None)):
                    runpy.run_path(executable, run_name="__main__")
                    captured = capsys.readouterr().out
                    print(captured)
        except SystemExit:
            captured = capsys.readouterr().out
            assert "Aborted" in captured 

    @pytest.mark.parametrize('executable', [exec_file])
    def test_script_execution_abort_groups(self, executable, capsys):
        sys.argv = [executable]
        try:
            with patch("mgenomicsremotemail.dispatch.RunDispatcher._get_run_id_app", return_value=MockApp("12345")):
                with patch("mgenomicsremotemail.dispatch.RunDispatcher._get_email_app", return_value=MockApp(None)):
                    with patch("mgenomicsremotemail.dispatch.RunDispatcher._get_input_app", return_value=MockApp(None)):
                        runpy.run_path(executable, run_name="__main__")
                        captured = capsys.readouterr().out
                        print(captured)
        except SystemExit:
            captured = capsys.readouterr().out
            assert "Aborted" in captured
# 

