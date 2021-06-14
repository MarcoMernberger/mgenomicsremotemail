#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""dispatcher.py: Contains ...."""

from pathlib import Path
from typing import Optional, Callable, List, Dict, Tuple, Any, Union
import pandas as pd
import pypipegraph as ppg

__author__ = "Marco Mernberger"
__copyright__ = "Copyright (c) 2020 Marco Mernberger"
__license__ = "mit"


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


sys.path.append("/rose/opt/infrastructure/repos/illumina")
try:
    import shared

    cleanup = True
except ModuleNotFoundError:
    cleanup = False


class RunDispatcher:
    def __init__(self):
        normal_path = Path("/rose/ffs/incoming")
        nextseq_path = Path("/rose/ffs/incoming/NextSeq")
        miseq_path = Path("/rose/ffs/incoming/MiSeq")
        self.public_path = "/mf/ffs/www/imtwww_nginx/webroot/public"
        self.max_days = 14
        self.all_paths = [normal_path, nextseq_path, miseq_path]
        self.__collect_ids()

    def __collect_ids(self):
        all_runs = {}
        for path in self.all_paths:
            for item in path.iterdir():
                if item.name[0].isdigit():
                    if len(item.name) > 4:
                        all_runs[item.name] = item
                    elif len(item.name) == 4:
                        for sub in item.iterdir():
                            all_runs[sub.name] = sub
                    else:
                        pass
        self.run_ids = all_runs

    def check_all_folders(self):
        for run_id in self.run_ids:
            try:
                folder = self.get_input_folder(self.run_ids[run_id], run_id)
                if not self.check_for_fastq(folder):
                    print(run_id, f"{folder} is empty")
                else:
                    print(run_id, "ok")
            except ValueError as e:
                print(run_id, self.run_ids[run_id], e)
                raise

    def get_run_ids_string(self):
        outstr = "Existing run ids:\n-----------------------\n"
        for run_id in self.run_ids:
            outstr += f"{run_id}\n"
        return outstr

    def print_run_ids(self):
        print(self.get_run_ids_string())

    def send_email(self, filename, md5sum, recipients, ag):
        message = f"""
Hi, a new Sequencing run has been completed for AG {ag} at the Genomics Core Facility, ZTI, Marburg.

You can download the data here:

https://mbf.imt.uni-marburg.de/public/{filename}.

md5sum={md5sum}

Login credentials are:
User=public
password=public

This link will expire in {self.max_days} days.

Best of luck!
    """
        msg = MIMEText(message)
        msg["Subject"] = "Sequencing run finished"
        msg["From"] = "%s <imtseq@imt.uni-marburg.de>" % "IMT Bioinformatics system"
        msg["To"] = ",".join(recipients)
        s = smtplib.SMTP("smtp.staff.uni-marburg.de")
        s.starttls()
        s.login("imtseq", "mwq!mrb6")
        s.sendmail(msg["From"], recipients, msg.as_string())
        s.quit()

    def __targz(self, infolder, name):
        if not (infolder / f"{name}.tar.gz").exists():
            command = ["tar", "-czvf", f"{name}.tar.gz", "*.fastq.gz"]
            subprocess.check_call(" ".join(command), cwd=str(infolder), shell=True)

    def __get_md5sum(self, path_2_file):
        command = ["md5sum", str(path_2_file)]
        md5sum = subprocess.check_output(command)
        md5sum = md5sum.decode().split()[0]
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
            for ss in alignments[-1].iterdir():
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
            checksum = self.__get_md5sum(archive_file)
            if checksum != md5sum:
                os.unlink(archive_file)

    def dispatch(self, run_ids, ag, recipients):
        # recipients.append("andrea.nist@imt.uni-marburg.de")
        recipients.append("marco.mernberger@staff.uni-marburg.de")
        # recipients.append("katharina.humpert@uni-marburg.de")
        for run_id in run_ids:
            name = f"{run_id}_AG_{ag}"
            run_folder = self.run_ids[run_id]
            if run_folder.exists():
                path_2_files = self.get_input_folder(run_folder, run_id)
                print(f"Collecting data from {path_2_files} ...")
                # now we know the path to fastq files
                filename = f"{name}.tar.gz"
                public_archive = Path(self.public_path) / filename
                print("Creating tar.gz ...")
                self.__targz(path_2_files, name)
                new_archive = path_2_files / filename
                print("Calculating md5sum ...")
                md5sum = self.__get_md5sum(new_archive)
                self.clear_archive(md5sum, public_archive)
                if not (public_archive).exists():
                    print("Moving to public ...")
                    self.__move(new_archive, public_archive)
                else:
                    print("Archive already exists ...")
                print("Dispatching emails")
                self.send_email(filename, md5sum, recipients, ag)

    def cleanup(self):
        for filename in self.public_path.iterdir():
            print(f"Last modified: {time.ctime(os.path.getmtime(str(filename)))}")
