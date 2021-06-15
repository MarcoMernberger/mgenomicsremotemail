# -*- coding: utf-8 -*-

import pytest
import hashlib
import tarfile
import subprocess
import smtplib
from mgenomicsremotemail.dispatch import RunDispatcher
from pathlib import Path


__author__ = "MarcoMernberger"
__copyright__ = "MarcoMernberger"
__license__ = "mit"


def test_fib():
    assert 1 == 1


def test_init():
    dispatcher = RunDispatcher()
    all_paths = ["/rose/ffs/incoming", "/rose/ffs/incoming/NextSeq", "/rose/ffs/incoming/MiSeq"]
    for path in dispatcher.all_paths:
        assert str(path) in all_paths
    assert dispatcher.public_path == Path("/mf/ffs/www/imtwww_nginx/webroot/public")
    assert dispatcher.MAXDAYS == 14


def test_ids():
    dispatcher = RunDispatcher()
    assert len(dispatcher.run_ids) > 1
    for run_id in dispatcher.run_ids:
        try:
            print(run_id)
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
    fastq = infolder / "dummy.fastq.gz"
    fastq.write_text("hi")
    dispatcher._targz(infolder, "test")
    archive = infolder / "test.tar.gz"
    assert archive.exists()
    tf = tarfile.open(archive)
    assert fastq.name == tf.getmembers()[0].name


def test_get_md5sum(tmp_path):
    dispatcher = RunDispatcher()
    infolder = tmp_path
    fastq = infolder / "dummy.fastq.gz"
    fastq.write_text("hi")
    dispatcher._targz(infolder, "test")
    archive = infolder / "test.tar.gz"
    sum1 = dispatcher._get_md5sum(archive)
    sum2 = subprocess.check_output(["md5sum", str(archive)]).decode().split()[0]
    assert sum1 == sum2


def test_get_input_folder(tmp_path):
    dispatcher = RunDispatcher()
    run_id = "12345"
    folder1 = tmp_path / run_id
    in1 = folder1 / run_id / "Alignment_1" / "22222" / "Fastq"  # current style NextSeq folder (2021)
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


def test_check_all_folders(capsys):
    dispatcher = RunDispatcher()
    dispatcher.check_all_folders()
    captured = capsys.readouterr()
    assert "ok" in captured.out


def test_get_run_ids(capsys):
    dispatcher = RunDispatcher()
    dispatcher.print_run_ids()
    captured = capsys.readouterr().out
    assert len(captured.split("\n")) >= 10


def test_dispatch(capsys):
    dispatcher = RunDispatcher()
    dispatcher.to_default_recipients = False
    with pytest.raises(ValueError):
        dispatcher.dispatch("run_ids", "ag", [])
    valid_run_id = next(iter(dispatcher.run_ids.keys()))
    with pytest.raises(ValueError):
        dispatcher.dispatch(valid_run_id, "ag", [])
    with pytest.raises(smtplib.SMTPRecipientsRefused):
        dispatcher.dispatch([valid_run_id], "ag", [])
    res = dispatcher.dispatch([valid_run_id], "ag", [dispatcher.default_recipients[0]])
    captured = capsys.readouterr().out
    assert res[0] == 221
    assert "Dispatching emails" in captured
    dispatcher.run_ids["bla"] = Path("some_none_Existing_folder")
    with pytest.raises(ValueError):
        dispatcher.dispatch(["bla"], "ag", [dispatcher.default_recipients[0]])


def test_send_email():
    dispatcher = RunDispatcher()
    dispatcher.to_default_recipients = False
    with pytest.raises(smtplib.SMTPRecipientsRefused):
        dispatcher.send_email("test.tar.gz", "1234", "", "TEST")
    res = dispatcher.send_email("test.tar.gz", "1234", dispatcher.default_recipients[0], "TEST")
    assert res[0] == 221
