#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""dispatcher.py: Contains ...."""

from pathlib import Path
from typing import Optional, Callable, List, Dict, Tuple, Any, Union
from pathlib import Path
from email.mime.text import MIMEText
import sys
import smtplib
import click
import subprocess
import shutil
import os
import time
import inspect
import email_validator
import hashlib
from prompt_toolkit import prompt
from prompt_toolkit.shortcuts import (
    checkboxlist_dialog,
    radiolist_dialog,
    progress_dialog,
)
from prompt_toolkit.shortcuts import input_dialog
from prompt_toolkit.validation import Validator, ValidationError
from prompt_toolkit import prompt
from email_validator import validate_email, EmailNotValidError
from prompt_toolkit.styles import Style
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.shortcuts import (
    checkboxlist_dialog,
    radiolist_dialog,
    progress_dialog,
    input_dialog
)

__author__ = "Marco Mernberger"
__copyright__ = "Copyright (c) 2020 Marco Mernberger"
__license__ = "mit"


sys.path.append("/rose/opt/infrastructure/repos/illumina")


class RunDispatcher:
    MAXDAYS = 14

    def __init__(self):
        normal_path = Path("/rose/ffs/incoming")
        nextseq_path = Path("/rose/ffs/incoming/NextSeq")
        miseq_path = Path("/rose/ffs/incoming/MiSeq")
        self.public_path = Path("/mf/ffs/www/imtwww_nginx/webroot/public")
        self.all_paths = [normal_path, nextseq_path, miseq_path]
        self.__collect_ids()
        self.host = "smtp.staff.uni-marburg.de"
        self.port = 0
        self.to_default_recipients = True
        self.default_recipients = [
            "marco.mernberger@staff.uni-marburg.de",
            # "andrea.nist@imt.uni-marburg.de",
            # "katharina.humpert@uni-marburg.de"
        ]

    def __collect_ids(self):
        all_runs = {}
        for path in self.all_paths:
            for item in path.iterdir():
                if item.name[0].isdigit():
                    if len(item.name) > 4:
                        all_runs[item.name] = item
                    elif len(item.name) == 4:
                        for sub in item.iterdir():
                            if sub.name[0].isdigit() and len(sub.name) > 4:
                                all_runs[sub.name] = sub
                    else:
                        pass  # pragma: no cover
        self._run_ids = all_runs
        self._all_run_ids_and_folders_as_tuples = [(x, x) for x in sorted(self._run_ids.keys(), reverse=True)]

    @property
    def run_ids(self):
        return self._run_ids

    @property
    def all_run_ids_and_folders_as_tuples(self):
        return self._all_run_ids_and_folders_as_tuples

    def check_all_folders(self) -> str:
        result = "Checking all Run IDs:\n---------------------\n"
        for run_id in self.run_ids:
            try:
                folder = self.get_input_folder(self.run_ids[run_id], run_id)
                if folder is not None:
                    if not self.check_for_fastq(folder):
                        result += f"{run_id}: is empty ({folder})\n"
                    else:
                        result += f"{run_id}: is ok\n"
                else:
                    result += f"{run_id}: No run folder detected\n"
            except PermissionError:
                result += f"{run_id}: PermissionError for {folder}\n"
            except ValueError as e:
                if "No fastq folder found" in str(e):
                    result += f"{run_id}: No fastq folder for {folder}\n"
                else:
                    print(run_id, self.run_ids[run_id], e)  # pragma: co cover
                    raise  # pragma: co cover
        return result

    def print_check_all_folders(self):
        print(self.check_all_folders())

    def get_run_ids_string(self):
        outstr = "Existing run ids:\n-----------------------\n"
        for run_id in self.run_ids:
            outstr += f"{run_id}\n"
        return outstr

    def print_run_ids(self):
        print(self.get_run_ids_string())

    def generate_message(self, filename, md5sum, recipients, ag):
        message = f"""
Hi, a new Sequencing run has been completed for AG {ag} at the Genomics Core Facility, ZTI, Marburg.

You can download the data here:

https://mbf.imt.uni-marburg.de/public/{filename}.

md5sum={md5sum}

Login credentials are:
User=public
password=public

This link will expire in {self.MAXDAYS} days.

Best of luck!
    """
        msg = MIMEText(message)
        msg["Subject"] = "Sequencing run finished"
        msg["From"] = "IMT Bioinformatics system <imtseq@imt.uni-marburg.de>"
        msg["To"] = ",".join(recipients)
        return msg

    def send_email(self, filename, md5sum, recipients, ag):
        msg = self.generate_message(filename, md5sum, recipients, ag)
        s = smtplib.SMTP(self.host, self.port)
        s.starttls()
        s.login("imtseq", "mwq!mrb6")
        s.sendmail(msg["From"], recipients, msg.as_string())
        return s.quit()

    def _targz(self, infolder, name):
        if not (infolder / f"{name}.tar.gz").exists():
            command = ["tar", "-czvf", f"{name}.tar.gz", "*.fastq.gz"]
            try:
                subprocess.check_call(" ".join(command), cwd=str(infolder), shell=True)
            except subprocess.CalledProcessError:
                print(" ".join(command))  # pragma: co cover
                raise  # pragma: co cover

    def _get_md5sum(self, path_2_file):
        with path_2_file.open("rb") as inp:
            md5_hash = hashlib. md5()
            content = inp.read()
            md5_hash. update(content)
            md5sum = md5_hash.hexdigest()
        return md5sum

    def __check_path(self, path):
        return path.exists()

    def __move(self, new_archive, public_archive):
        shutil.move(new_archive, public_archive)

    def check_for_fastq(self, path_2_files):
        for filepath in path_2_files.iterdir():
            if "fastq" in filepath.name:
                return True
        return False

    def get_input_folder(self, run_folder, run_id):
        path_2_files = None
        sub = run_folder
        if (run_folder / run_id).exists():
            sub = run_folder / run_id
        alignments = []
        for s in sub.iterdir():
            if s.name.startswith("Alignment"):
                alignments.append(s)
        if len(alignments) > 0:
            alignments = sorted(alignments, reverse=True)
            for ss in alignments[0].iterdir():
                path_2_files = ss / "Fastq"
                break
        else:
            # this is the old stuff
            if (run_folder / "Unaligned").exists():
                path_2_files = run_folder / "Unaligned"
            elif (run_folder / "Data" / "Intensities" / "BaseCalls").exists():
                path_2_files = run_folder / "Data" / "Intensities" / "BaseCalls"
            else:
                raise ValueError(f"No fastq folder found for {str(run_folder)}.")
        return path_2_files

    def clear_archive(self, md5sum, archive_file):
        if archive_file.exists():
            checksum = self._get_md5sum(archive_file)
            if checksum != md5sum:
                os.unlink(archive_file)

    def dispatch(self, run_ids, ag, recipients):
        if self.to_default_recipients:
            recipients.extend(self.default_recipients)
        for run_id in run_ids:
            name = f"{run_id}_AG_{ag}"
            if run_id in self.run_ids:
                run_folder = self.run_ids[run_id]
                if run_folder.exists():
                    path_2_files = self.get_input_folder(run_folder, run_id)
                    print(f"Collecting data from {path_2_files} ...")
                    # now we know the path to fastq files
                    filename = f"{name}.tar.gz"
                    public_archive = Path(self.public_path) / filename
                    print("Creating tar.gz ...")
                    self._targz(path_2_files, name)
                    new_archive = path_2_files / filename
                    print("Calculating md5sum ...")
                    md5sum = self._get_md5sum(new_archive)
                    self.clear_archive(md5sum, public_archive)
                    if not (public_archive).exists():
                        print("Moving to public ...")
                        self.__move(new_archive, public_archive)
                    else:
                        print("Archive already exists ...")
                    print("Dispatching emails")
                    res = self.send_email(filename, md5sum, recipients, ag)
                    return res
                else:
                    raise ValueError(f"{run_folder} does not exist.")
            else:
                raise ValueError(f"Run {run_id} does not exist.")

    def cleanup(self):
        for filename in self.public_path.iterdir():
            print(f"Last modified: {time.ctime(os.path.getmtime(str(filename)))}")

    def request_run_ids(self):
        app = checkboxlist_dialog(
            title="Run IDs",
            text="Select the run ids:",
            values=self.all_run_ids_and_folders_as_tuples,
        )
        return app.run()

    def request_groups(self):

        def _get_app():
            app = input_dialog(
                title="Research Group",
                text="Please enter the research group name:"
            )
            return app

        app = _get_app()
        return app.run()
