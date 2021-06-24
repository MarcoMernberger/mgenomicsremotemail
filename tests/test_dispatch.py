# -*- coding: utf-8 -*-

import pytest
import tarfile
import subprocess
import smtplib
from mgenomicsremotemail.dispatch import RunDispatcher
from pathlib import Path
from prompt_toolkit.input import create_pipe_input
from prompt_toolkit.output import DummyOutput
from prompt_toolkit.application.current import AppSession
from mock import patch
from datetime import datetime
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.application import Application
from conftest import MockApp


__author__ = "MarcoMernberger"
__copyright__ = "MarcoMernberger"
__license__ = "mit"


def app_func_with_input(app_function, *args):
    def __func():
        return app_function(*args)

    return __func


def get_mock(input_fuction_dict, app_function):
    def __mock_function(*formatted_text):
        for key in input_fuction_dict:
            if str(formatted_text[0][0][1]).startswith(key):
                inp = input_fuction_dict[key]
                app_func = app_func_with_input(app_function, *formatted_text)
                return get_patched_app(inp, app_func)
        else:
            raise ValueError()

    return __mock_function


def get_patched_app(input, app_function):
    patched_app = app_function()
    patched_app.input = input
    patched_app.output = DummyOutput()
    return patched_app


def test_init():
    dispatcher = RunDispatcher()
    all_paths = [
        "/rose/ffs/incoming",
        "/rose/ffs/incoming/NextSeq",
        "/rose/ffs/incoming/MiSeq",
    ]
    for path in dispatcher.all_paths:
        assert str(path) in all_paths
    assert dispatcher.public_path == Path("/mf/ffs/www/imtwww_nginx/webroot/public")
    assert dispatcher.MAXDAYS == 14
    assert dispatcher.do_clean_up
    assert isinstance(dispatcher.all_run_ids_and_folders_as_tuples, list)


def test_ids():
    dispatcher = RunDispatcher()
    assert len(dispatcher.run_ids) > 1
    for run_id in dispatcher.run_ids:
        try:
            assert run_id[0].isdigit()
            assert isinstance(run_id, str)
            assert isinstance(dispatcher.run_ids[run_id], Path)
        except AssertionError:
            print(run_id, print(type(run_id)))
            print(dispatcher.run_ids[run_id])
            raise


def test_check_for_fastq(tmp_path):
    dispatcher = RunDispatcher()
    path_2_files = tmp_path
    fastq = path_2_files / "dummy.fastq"
    fastq.write_text("hi")
    assert dispatcher.check_for_fastq(path_2_files)
    fastq.rename(path_2_files / "dummy.txt")
    assert not (path_2_files / "dummy.fastq").exists()
    assert not dispatcher.check_for_fastq(path_2_files)


def test_tar_gz(tmp_path):
    dispatcher = RunDispatcher()
    infolder = tmp_path
    for ii in ["1", "2"]:
        fastq = infolder / f"dummy{ii}.fastq.gz"
        fastq.write_text("hi")
    dispatcher._targz(infolder, tmp_path / "test.tar.gz")
    archive = infolder / "test.tar.gz"
    assert archive.exists()
    tf = tarfile.open(archive)
    assert fastq.name == tf.getmembers()[1].name


def test_get_md5sum(tmp_path):
    dispatcher = RunDispatcher()
    infolder = tmp_path
    fastq = infolder / "dummy.fastq.gz"
    fastq.write_text("hi")
    dispatcher._targz(infolder, tmp_path / "test.tar.gz")
    archive = infolder / "test.tar.gz"
    sum1 = dispatcher._get_md5sum(archive)
    sum2 = subprocess.check_output(["md5sum", str(archive)]).decode().split()[0]
    assert sum1 == sum2


def test_get_input_folder(tmp_path):
    dispatcher = RunDispatcher()
    run_id = "12345"
    folder1 = tmp_path / run_id
    in1 = (
        folder1 / run_id / "Alignment_1" / "22222" / "Fastq"
    )  # current style NextSeq folder (2021)
    in2 = folder1 / run_id / "Alignment_2" / "23333" / "Fastq"
    run_id = "12346"
    folder2 = tmp_path / run_id
    in3 = folder2 / "Data" / "Intensities" / "BaseCalls"
    run_id = "12347"
    folder3 = tmp_path / run_id
    in4 = folder3 / "Unaligned"
    for infolder in [in1, in2, in3, in4]:
        infolder.mkdir(parents=True, exist_ok=True)
    folder = dispatcher.get_input_folder(folder1, "12345")
    assert folder == in2
    folder = dispatcher.get_input_folder(folder2, "12346")
    assert folder == in3
    folder = dispatcher.get_input_folder(folder3, "12347")
    assert folder == in4
    with pytest.raises(ValueError):
        dispatcher.get_input_folder(tmp_path, "12347")


def test_clear_archive(tmp_path):
    dispatcher = RunDispatcher()
    fastq = tmp_path / "dummy.fastq.gz"
    fastq.write_text("hi")
    assert fastq.exists()
    sum = dispatcher._get_md5sum(fastq)
    dispatcher.clear_archive(sum, fastq)
    assert fastq.exists()
    dispatcher.clear_archive("1234", fastq)
    assert not fastq.exists()
    assert dispatcher.clear_archive("1234", Path("noarchive")) is None


def test_generate_message():
    dispatcher = RunDispatcher()
    msg = dispatcher.generate_message("dummy.txt", "abc123", ["x.y@gmail.com"], "AG")
    assert msg["Subject"] == "Sequencing run finished"
    assert msg["From"] == "IMT Bioinformatics system <imtseq@imt.uni-marburg.de>"
    assert "x.y@gmail.com" in msg["To"]
    assert "https://mbf.imt.uni-marburg.de/public/dummy.txt" in msg._payload
    assert "AG AG" in msg._payload
    assert "md5sum=abc123" in msg._payload
    assert f"This link will expire in {dispatcher.MAXDAYS} days." in msg._payload


def test_print_check_all_folders(capsys):
    dispatcher = RunDispatcher()
    dispatcher.print_check_all_folders()
    captured = capsys.readouterr()
    assert "ok" in captured.out


def test_check_all_folders(capsys):
    dispatcher = RunDispatcher()
    res = dispatcher.check_all_folders()
    assert "ok" in res


def test_print_run_ids(capsys):
    dispatcher = RunDispatcher()
    dispatcher.print_run_ids()
    captured = capsys.readouterr().out
    assert len(captured.split("\n")) >= 10


def test_get_run_ids():
    dispatcher = RunDispatcher()
    run_id_str = dispatcher.get_run_ids_string()
    assert len(run_id_str.split("\n")) >= 10


def test_request_groups():
    dispatcher = RunDispatcher()
    inp = create_pipe_input()
    inp.send_text("Test\r\r")
    group = ""
    with patch(
        "prompt_toolkit.application.current._current_app_session",
        return_value=AppSession(inp, DummyOutput()),
    ):
        with patch("prompt_toolkit.renderer.Renderer.render", return_value=None):
            with patch(
                "mgenomicsremotemail.dispatch.RunDispatcher._get_input_app",
                return_value=get_patched_app(inp, dispatcher._get_input_app),
            ):
                group = dispatcher.request_groups()
                assert "Test" == group
            with patch(
                "mgenomicsremotemail.dispatch.RunDispatcher._get_input_app",
                return_value=MockApp(None),
            ):
                with pytest.raises(SystemExit, match="Aborted"):
                    group = dispatcher.request_groups()


def test_request_emails():
    dispatcher = RunDispatcher()
    with patch("prompt_toolkit.renderer.Renderer.render", return_value=None):
        with patch(
            "prompt_toolkit.application.current._current_app_session",
            return_value=AppSession(create_pipe_input(), DummyOutput()),
        ):
            inp_correct = create_pipe_input()
            inp_correct.send_text("valid_email@test.com\r\r")
            inp_wrong1 = create_pipe_input()
            inp_wrong1.send_text("invalid.email\r\r")
            inp_wrong2 = create_pipe_input()
            inp_wrong2.send_text("invalidłemail.com\r\r")
            alternate_inputs = {
                "Please": inp_wrong1,
                "'invalid.email'": inp_wrong2,
                "'invalidłemail.com'": inp_correct,
            }
            dispatcher._get_email_app = get_mock(
                alternate_inputs, dispatcher._get_email_app
            )
            recipients = dispatcher.request_emails()
            assert recipients[0] == "valid_email@test.com"


def test_request_emails_once():
    dispatcher = RunDispatcher()
    with patch("prompt_toolkit.renderer.Renderer.render", return_value=None):
        inp = create_pipe_input()
        with patch(
            "prompt_toolkit.application.current._current_app_session",
            return_value=AppSession(inp, DummyOutput()),
        ):
            emails = ["valid.email@test.com", "another_valid_email@test.com"]
            inp.send_text(
                "valid.email@test.com,another_valid_email@test.com\r\rvalid.email@test.com\r\rinvalid.emailłtest.com\r\rvalid.email@test.com,  another_valid_email@test.com\r\r\r\r\t\t\r"
            )
            with patch(
                "mgenomicsremotemail.dispatch.RunDispatcher._get_email_app",
                return_value=get_patched_app(inp, dispatcher._get_input_app),
            ):
                recipients = dispatcher._request_emails_once("", None)
                for expected, received in zip(emails, recipients):
                    assert expected == received
            with patch(
                "mgenomicsremotemail.dispatch.RunDispatcher._get_email_app",
                return_value=get_patched_app(inp, dispatcher._get_input_app),
            ):
                recipients = dispatcher._request_emails_once("", None)
                assert "valid.email@test.com" == recipients[0]
            with patch(
                "mgenomicsremotemail.dispatch.RunDispatcher._get_email_app",
                return_value=get_patched_app(inp, dispatcher._get_input_app),
            ):
                recipients = dispatcher._request_emails_once("", None)
                assert "invalid.emailłtest.com" == recipients[0]
                accepted, _ = dispatcher._validate_recipients(["invalid.email@test"])
                assert not accepted
            with patch(
                "mgenomicsremotemail.dispatch.RunDispatcher._get_email_app",
                return_value=get_patched_app(inp, dispatcher._get_input_app),
            ):
                recipients = dispatcher._request_emails_once("", None)
                for expected, received in zip(emails, recipients):
                    assert expected == received
            with patch(
                "mgenomicsremotemail.dispatch.RunDispatcher._get_email_app",
                return_value=get_patched_app(inp, dispatcher._get_input_app),
            ):
                recipients = dispatcher._request_emails_once("", None)
                print(recipients)
                print(dispatcher._validate_recipients(recipients))
                assert len(recipients) == 0


def test_validate_recipients():
    dispatcher = RunDispatcher()
    accept, _ = dispatcher._validate_recipients(
        ["valid.email@test.com", "another_valid_email@test.com"]
    )
    assert accept
    accept, _ = dispatcher._validate_recipients(["invalid.emailłtest.com"])
    assert not accept
    accept, _ = dispatcher._validate_recipients(["invalid.email@test"])
    assert not accept


def make_mock_file(path_2_files, name):
    with (path_2_files / f"{name}.tar.gz").open("w") as op:
        op.write("something")


def test_dispatch(capsys, tmp_path):
    dispatcher = RunDispatcher()
    pubpath = tmp_path / "dest"
    pubpath.mkdir()
    dispatcher.public_path = pubpath
    assert dispatcher.dispatch([], "", [])[0] == -1
    with pytest.raises(ValueError):
        dispatcher.dispatch(["run_ids"], "ag", [])
    dispatcher.to_default_recipients = False
    valid_run_id = next(iter(dispatcher.run_ids.keys()))
    empty_folder = tmp_path / "empty"
    empty_folder.mkdir()
    dispatcher.run_ids[valid_run_id] = tmp_path
    with pytest.raises(ValueError, match=f"Run 12 does not exist"):
        dispatcher.dispatch(["12"], "ag", [dispatcher.default_recipients[0]])
    dispatcher.run_ids["fake_id"] = Path("non_existing_path")
    with pytest.raises(ValueError, match="Folder non_existing_path does not exist."):
        dispatcher.dispatch(["fake_id"], "ag", [dispatcher.default_recipients[0]])
    with pytest.raises(ValueError, match="No folder containing fastq files found in"):
        dispatcher.dispatch([valid_run_id], "ag", [dispatcher.default_recipients[0]])
    with patch("mgenomicsremotemail.dispatch.RunDispatcher.get_input_folder", return_value=empty_folder):
        with pytest.raises(ValueError, match=f" is empty for {valid_run_id}"):
            dispatcher.dispatch([valid_run_id], "ag", [dispatcher.default_recipients[0]])
    input_file = tmp_path / "input" / "test.fastq.gz"
    input_file.parent.mkdir(exist_ok=True, parents=True)
    with input_file.open("w") as op:
        op.write("something")
    assert input_file.exists()
    dispatcher.send_email = lambda *args: (1, "send called")
    with patch("mgenomicsremotemail.dispatch.RunDispatcher.get_input_folder", return_value=input_file.parent):
        res = dispatcher.dispatch([valid_run_id], "ag", [dispatcher.default_recipients[0]])
        captured = capsys.readouterr().out
        assert res[1] == "send called"
        assert "Collecting data" in captured
        assert "Dispatching emails" in captured
        assert "Creating tar.gz" in captured
        res = dispatcher.dispatch([valid_run_id], "ag", [dispatcher.default_recipients[0]])
        captured = capsys.readouterr().out
        print(captured)
        assert "Archive already exists" in captured
        assert res[1] == "send called"


def test_send_email():
    dispatcher = RunDispatcher()
    dispatcher.to_default_recipients = False
    with pytest.raises(smtplib.SMTPRecipientsRefused):
        dispatcher.send_email("test.tar.gz", "1234", "", "TEST")
    with patch("smtplib.SMTP.sendmail", return_value=False):
        res = dispatcher.send_email("test.tar.gz", "1234", [dispatcher.default_recipients[0]], "TEST")
        assert res[0] == 221


def test_get_ctime():
    dispatcher = RunDispatcher()
    dt = dispatcher.get_ctime(Path(__file__))
    assert isinstance(dt, datetime)


def test_get_old_files():
    dispatcher = RunDispatcher()
    thisfolder = Path(__file__).parent
    dispatcher.public_path = thisfolder
    dispatcher.MAXDAYS = 0
    for f in thisfolder.iterdir():
        assert f in dispatcher._get_old_files()


def test_cleanup(tmp_path):
    testfile = Path(tmp_path, "test")
    with testfile.open("w") as op:
        op.write("something")
    dispatcher = RunDispatcher()
    dispatcher.public_path = tmp_path
    dispatcher.MAXDAYS = 0
    assert testfile.exists()
    dispatcher.cleanup()
    assert not testfile.exists()


def test_move(tmp_path):
    new_archive = tmp_path / "source"
    public_archive = tmp_path / "dest"
    with (new_archive).open("w") as op:
        op.write("something")
    assert new_archive.exists()
    dispatcher = RunDispatcher()
    dispatcher._move(new_archive, public_archive)
    assert public_archive.exists()
    assert not new_archive.exists()


def test_get_formatted_text():
    dispatcher = RunDispatcher()
    assert isinstance(dispatcher._get_formatted_text("str", False), FormattedText)
    assert isinstance(dispatcher._get_formatted_text("str", False), FormattedText)


def test_run():
    dispatcher = RunDispatcher()
    dispatcher.dispatch = lambda *args: print("dispatch called")
    dispatcher.request_run_ids = lambda *args: print("request_run_ids called")
    dispatcher.request_emails = lambda *args: print("request_emails called")
    dispatcher.request_groups = lambda *args: print("request_groups called")
    dispatcher.dispatch = lambda *args: print("dispatch called")
    dispatcher.cleanup = lambda *args: print("cleanup called")
    assert dispatcher.run()


def test_get_run_id_app():
    with patch(
        "prompt_toolkit.application.current._current_app_session",
        return_value=AppSession(create_pipe_input(), DummyOutput()),
    ):
        with patch("prompt_toolkit.renderer.Renderer.render", return_value=None):
            dispatcher = RunDispatcher()
            assert isinstance(dispatcher._get_run_id_app(), Application)
