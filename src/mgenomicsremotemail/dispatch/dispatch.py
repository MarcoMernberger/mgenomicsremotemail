#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dispatcher.py: Contains some functionality to create and automated
sequencing run email via the command line. This will be used for the Genomics
Core facility to inform the recipients of available fastq downloads after the run
is completed. It is intended to be used as a remote ssh call by configuring a
restricted ssh login for the head of the Core Facility.
"""
from pathlib import Path
from typing import List, Dict, Tuple, Union
from email.mime.text import MIMEText
import sys
import smtplib
import tempfile
import shutil
import os
import time
import hashlib
import re
import tarfile
from datetime import datetime
from prompt_toolkit.application import Application
from email_validator import validate_email, EmailNotValidError
from prompt_toolkit.styles import Style
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.shortcuts import (
    checkboxlist_dialog,
    input_dialog
)

__author__ = "Marco Mernberger"
__copyright__ = "Copyright (c) 2020 Marco Mernberger"
__license__ = "mit"


sys.path.append("/rose/opt/infrastructure/repos/illumina")


class RunDispatcher:
    """
    This is a RunDispatcher class that offers functionality to send
    automated emails for sequencing runs from mf via ssh.

    This is supposed to be called via a restricted ssh login.
    """
    MAXDAYS = 14

    def __init__(self):
        """Constructor"""
        normal_path = Path("/rose/ffs/incoming")
        nextseq_path = Path("/rose/ffs/incoming/NextSeq")
        miseq_path = Path("/rose/ffs/incoming/MiSeq")
        self.public_path = Path("/mf/ffs/www/imtwww_nginx/webroot/public")
        self.all_paths = [normal_path, nextseq_path, miseq_path]  # all paths where potenitally Sequencing runs can be found.
        self.__collect_ids()
        self.host = "smtp.staff.uni-marburg.de"
        self.port = 0
        self.to_default_recipients = True  # Send outgoing email to us as well
        self.default_recipients = [
            "marco.mernberger@staff.uni-marburg.de",
            "andrea.nist@imt.uni-marburg.de",
            "katharina.humpert@uni-marburg.de"
        ]
        self.do_clean_up = True  # wehter to clean the public folder

    def __collect_ids(self) -> None:
        """
        __collect_ids collects all run IDs that can be found in the known paths.

        This sets the field self._run_ids for all susequent methods and
        for convenience sets a list of (Run ID, fastq path) for all avbailable runs.
        """
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
    def run_ids(self) -> Dict[str, Path]:
        """
        Getter for run_ids.

        Returns
        -------
        Dict[str, Path]
            A dict of run_ids to run folders.
        """
        return self._run_ids

    @property
    def all_run_ids_and_folders_as_tuples(self) -> List[Tuple[str, Path]]:
        """
        Getter for all_run_ids_and_folders_as_tuples.

        Returns
        -------
        List[Tuple[str, Path]]
            A list of tuples (Run ID, fastq path) for all known runs.
        """
        return self._all_run_ids_and_folders_as_tuples

    def check_all_folders(self) -> str:
        """
        check_all_folders checks for all known folders wether fastq files can
        be found and the data it's supposed to be.

        This method can be called fgrom the command line tool vial the '--check'
        option.

        Returns
        -------
        str
            A string detailing the results for all checked run ids.
        """
        result = "Checking all Run IDs:\n---------------------\n"
        for run_id in self.run_ids:
            try:
                folder = self.get_input_folder(self.run_ids[run_id], run_id)
                if not self.check_for_fastq(folder):
                    result += f"{run_id}: is empty ({folder})\n"
                else:
                    result += f"{run_id}: is ok\n"
            except PermissionError:
                result += f"{run_id}: PermissionError for {folder}\n"
            except ValueError as e:
                if "No folder containing fastq files found in" in str(e):
                    result += f"{run_id}: No fastq folder for {folder}\n"
                else:
                    print(run_id, self.run_ids[run_id], e)  # pragma: no cover
                    raise   # pragma: no cover
        return result

    def print_check_all_folders(self) -> None:
        """
        print_check_all_folders prints the result of RunDispatcher.check_all_folders
        on screen.
        """
        print(self.check_all_folders())

    def get_run_ids_string(self) -> str:
        """
        get_run_ids_string Returns a list of all known Run IDs as a string.

        This is used to print the list on screen when invoked via the '--ids'
        option.

        Returns
        -------
        str
            A string representation with all known ids.
        """
        outstr = "Existing run ids:\n-----------------------\n"
        for run_id in self.run_ids:
            outstr += f"{run_id}\n"
        return outstr

    def print_run_ids(self) -> None:
        """
        print_run_ids prints the result of RunDispatcher.get_run_ids_string
        on screen wqhen invoked via the '--ids' option.
        """
        print(self.get_run_ids_string())

    def generate_message(self, filename: Path, md5sum: str, recipients: List[str], ag: str) -> MIMEText:
        """
        generate_message generates an automatic email text to be sent to the
        recipients.

        The message is returned as MIMEText object to be send to the recipients.

        Parameters
        ----------
        filename : Path
            The base name of the output archive as a path (no parents).
        md5sum : str
            The md5checksum of the archive file.
        recipients : List[str]
            A list of recipients email adresses.
        ag : str
            The research group of the recipient.

        Returns
        -------
        MIMEText
            The email message text.
        """
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

    def send_email(self, filename, md5sum, recipients, ag) -> Tuple[int, bytes]:
        """
        send_email sends the email to all recipients.

        This uses the MBF mail server to send the email.

        Parameters
        ----------
        filename : Path
            The base name of the output archive as a path (no parents).
        md5sum : str
            The md5checksum of the archive file.
        recipients : List[str]
            A list of recipients email adresses.
        ag : str
            The research group of the recipient.

        Returns
        -------
        Tuple[int, bytes]
            The return value of the SMTP QUIT command.
        """
        msg = self.generate_message(filename, md5sum, recipients, ag)
        s = smtplib.SMTP(self.host, self.port)
        s.starttls()
        s.login("imtseq", "mwq!mrb6")
        s.sendmail(msg["From"], recipients, msg.as_string())
        res = s.quit()
        return res

    def _targz(self, infolder: Path, public_archive: Path) -> None:
        """
        _targz creates the archive file with all fastq files to be send.

        This uses tar and calls it via subrocess in the folder, where
        the data is.

        Parameters
        ----------
        infolder : [Path]
            The fastq folder.
        public_archive : [Path]
            The created archive.
        """
        with tempfile.TemporaryDirectory(dir=os.getcwd()) as tmp_dir:
            temp_archive = Path(tmp_dir) / public_archive.name
            print(temp_archive.resolve())
            with tarfile.open(temp_archive, mode="w:gz") as op:
                for source in infolder.iterdir():
                    if ".fastq" in source.name and not source.is_dir():
                        op.add(source, arcname=source.name)
            self._move(str(temp_archive), str(public_archive))

    def _get_md5sum(self, path_2_file: Path) -> str:
        """
        _get_md5sum calculates the md5sum of a file and returns it.

        Parameters
        ----------
        path_2_file_Path : [Path]
            The file for which the md5sum is caalculated.

        Returns
        -------
        str
            md5 checksum of path_2_file.
        """
        with path_2_file.open("rb") as inp:
            md5_hash = hashlib. md5()
            content = inp.read()
            md5_hash. update(content)
            md5sum = md5_hash.hexdigest()
        return md5sum

    def _move(self, new_archive: str, public_archive: str) -> None:
        """
        _move moves a file new_archive to public_archive.

        Parameters
        ----------
        new_archive : str
            The source path (full path).
        public_archive : str
            The destination path.
        """
        shutil.move(new_archive, public_archive)

    def check_for_fastq(self, path_2_files: Path) -> bool:
        """
        check_for_fastq checks for a given folder whether fastq files can be
        found in the folder.

        Parameters
        ----------
        path_2_files : Path
            Folder to be checked.

        Returns
        -------
        bool
            True, if at least one file name contains 'fastq' in its name.
        """
        for filepath in path_2_files.iterdir():
            if ".fastq" in filepath.name:  # do not care if .fastq or .fastq.gz
                return True
        return False

    def get_input_folder(self, run_folder: Path, run_id) -> Path:
        """
        get_input_folder returns the path to the fastq file for a given run ID.

        If no folder with fastq files can be found in the usual locations, this
        returns None.

        Parameters
        ----------
        run_folder : Path
            Run folder to be checked for fastqw sub folders.
        run_id : [type]
            Th4e run ID of the sequencing run.

        Returns
        -------
        Path
            The full path to the folder containing the fastq files.

        Raises
        ------
        ValueError
            If no folder with fastq files can be found in the run folder.
        """
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
                raise ValueError(f"No folder containing fastq files found in {str(run_folder)}")
        if path_2_files is None:
            raise ValueError(f"No folder containing fastq files found in {str(run_folder)}")
        return path_2_files

    def clear_archive(self, md5sum: str, archive_file: Path) -> None:
        """
        clear_archive clears a file if it's md5sum does not check out.

        This is used to remove failed archives.

        Parameters
        ----------
        md5sum : str
            The expected md5sum.
        archive_file : Path
            the archive file.
        """
        if archive_file.exists():
            checksum = self._get_md5sum(archive_file)
            if checksum != md5sum:
                archive_file.unlink()

    def dispatch(self, run_ids: List[str], ag: str, recipients: List[str]) -> Tuple[int, bytes]:
        """
        dispatch performs all necessary steps to create the email and archive in
        the public download folder and finally sends the automated email.

        This is the main functionality of the dispatcher which ties all the pre
        steps together.

        Parameters
        ----------
        run_ids : List[str]
            List of run IDs to be selected.
        ag : str
            The research group of the main recipient.
        recipients : List[str]
            List of recipients which receive the email.

        Returns
        -------
        Tuple[int, bytes]
            The return value of the SMTP QUIT command.

        Raises
        ------
        ValueError
            If no fastq folder can be found within the run folder.
        ValueError
            If no run folder can be found.
        ValueError
            If the run id is not known.
        """
        if self.to_default_recipients:
            recipients.extend(self.default_recipients)
        res = -1, b"None"
        for run_id in run_ids:
            name = f"{run_id}_AG_{ag}"
            if run_id in self.run_ids:
                run_folder = self.run_ids[run_id]
                if run_folder.exists():
                    path_2_files = self.get_input_folder(run_folder, run_id)
                    for x in path_2_files.iterdir():
                        print(x)
                    if not self.check_for_fastq(path_2_files):
                        raise ValueError(f"Folder {str(path_2_files)} is empty for {run_id}")
                    else:
                        print(f"Collecting data from {path_2_files} ...")
                        # now we know the path to fastq files
                        filename = f"{name}.tar.gz"
                        public_archive = self.public_path / filename
                        if not public_archive.exists():
                            print("Creating tar.gz ...")
                            self._targz(path_2_files, public_archive)
                        else:
                            print("Archive already exists ...")
                        print("Calculating md5sum ...")
                        md5sum = self._get_md5sum(public_archive)
                        print("Dispatching emails ...")
                        res = self.send_email(filename, md5sum, recipients, ag)
                else:
                    raise ValueError(f"Folder {run_folder} does not exist.")
            else:
                raise ValueError(f"Run {run_id} does not exist.")
        return res

    def get_ctime(self, filepath: Path) -> datetime:
        """
        get_ctime returns the creation time of a file path as datetime.datetime
        object.

        Parameters
        ----------
        filepath : Path
            The file path to check.

        Returns
        -------
        datetime
            The creation time of filepath.
        """
        ctime = time.ctime(os.path.getmtime(str(filepath)))
        filetime = datetime.strptime(ctime, "%a %b %d %H:%M:%S %Y")
        return filetime

    def _get_old_files(self) -> List[Path]:
        """
        _get_old_files returns a list of file path objects whose creation time
        exceeds the day limit specified in RunDispatcher.MAXDAYS.

        Returns
        -------
        List[Path]
            List of file paths exceeding the day limit.
        """
        current_time = datetime.now()
        to_clean = []
        for filename in self.public_path.iterdir():
            delta = current_time - self.get_ctime(filename)
            if delta.days >= self.MAXDAYS:
                to_clean.append(filename)
        return to_clean

    def cleanup(self) -> None:
        """
        cleanup purges all files from the public_path that exceeds the day limit
        as specified by RunDispatcher.MAXDAYS.
        """
        for filename in self._get_old_files():
            filename.unlink()

    def _get_run_id_app(self) -> Application:
        """
        _get_run_id_app returns a prompt-toolkit App that queries the run IDs.

        This returns a checkbox dialog app for the user to select the appropriate run IDs.

        Returns
        -------
        Application
            The application that queries the run IDs.
        """
        app = checkboxlist_dialog(
            title="Run IDs",
            text="Select the run ids:",
            values=self.all_run_ids_and_folders_as_tuples,
        )
        return app

    def request_run_ids(self) -> List[str]:
        """
        request_run_ids requests the run ids from the user.

        Returns
        -------
        List[str]
            List of run IDs or None.
        """
        app = self._get_run_id_app()    # pragma: no cover
        res = app.run()    # pragma: no cover
        if res is None:
            sys.exit("Aborted!")
        if len(res) == 0:
            sys.exit("No run ID selected. Exit!")
        return res

    def _get_input_app(self) -> Application:
        """
        _get_input_app returns a prompt-toolkit app that queries the research group.

        This returns a simple text input dialog app for the user to enter the
        research group.

        Returns
        -------
        Application
            The application that queries the research group.

        """
        app = input_dialog(
            title="Research Group",
            text="Please enter the research group name:"
        )
        return app

    def request_groups(self) -> str:
        """
        request_groups requests the research group of the main recipient.

        Returns
        -------
        str
            The research group to be used.
        """
        app = self._get_input_app()
        res = app.run()
        if res is None:
            sys.exit("Aborted!")
        return res

    def _get_email_app(self, textr: str, style: Union[None, Style]) -> Application:
        """
        _get_email_app returns a prompt-toolkit app that queries the recipients.

        As in RunDispatcher._get_input_app, a simple text input dialog is used.
        Since this will be called again if an errouneous email adress is supplied,
        the text and style of the dialog can be supplied.

        Parameters
        ----------
        textr : str
            Test of the input dialog.
        style : Union[None, Style]
            Style of the input dialog.

        Returns
        -------
        Application
            The application that queries the recipients.
        """
        app = input_dialog(
            title="Recipients",
            text=textr,
            style=style,
        )
        return app

    def _request_emails_once(self, textr: str, style: Union[None, Style]) -> List[str]:
        """
        _request_emails_once requests the recipients from the user.

        Parameters
        ----------
        textr : str
            The text fo the input dialog.
        style : Union[None, Style]
            The style of the input dialog.

        Returns
        -------
        List[str]
            List of recipient email adresses. May be empty.
        """
        app = self._get_email_app(textr, style)
        received = app.run()
        if received is None:
            sys.exit("Aborted!")
        if received == "":
            recipients = []
        else:
            received = re.sub(r"\s+", "", received)
            recipients = received.split(",")
        return recipients

    def _validate_recipients(self, recipients: List[str]) -> Tuple[bool, str]:
        """
        _validate_recipients validates a supplied recipient lists and checks
        the email adress format.

        This is used to check for invalid email adresses. If there are some,
        the request will be repeated until all entries are valid.

        Parameters
        ----------
        recipients : List[str]
            List of recipient email adressses to be checked.

        Returns
        -------
        Tuple[bool, str]
            Check result and error message to be used in the new request.
        """
        for email in recipients:
            try:
                validate_email(email)
            except EmailNotValidError:
                return False, f"'{email}' is not a valid email."
        return True, ""

    def _get_formatted_text(self, text: str, red: bool = False) -> FormattedText:
        """
        _get_formatted_text returns a prompt_toolkit.FormattedText object to be
        used in the new request.

        Parameters
        ----------
        text : str
            The text fo the input dialog.
        red : bool, optional
            Wheter the text color should be red of black, by default False.

        Returns
        -------
        FormattedText
            Formatted text to be displayed in the input dialog.
        """
        if red:
            color = "#ff0000"
        else:
            color = "#000000"
        return FormattedText([(color, text)])

    def request_emails(self) -> List[str]:
        """
        request_emails requests the recipient emails from the user.

        Recipient email adresses are checked for validity afterwards and if
        an invalid email adress is supplied, the user is queried again until he gets it
        right or cancels.

        Returns
        -------
        List[str]
            List of valid recipient email adresses.
        """
        recipients_ok = False
        textr = self._get_formatted_text("Please enter the recipient emails as comma-separated list:")
        style = None
        recipients = []
        while not recipients_ok:
            recipients = self._request_emails_once(textr, style)
            recipients_ok, msg = self._validate_recipients(recipients)
            if not recipients_ok:
                textr = self._get_formatted_text(f"{msg}\n Please enter the recipient emails as comma-separated list:", True)
        return recipients

    def run(self) -> str:
        """
        run calls the main methid of the dispatcher.

        This is the entry point for the command line tool.

        Returns
        -------
        str
            Message to print at the end of excecution.
        """
        try:
            run_ids = self.request_run_ids()
        except SystemExit as e:
            return str(e)
        try:
            recipients = self.request_emails()
        except SystemExit as e:
            return str(e)
        try:
            groups = self.request_groups()
        except SystemExit as e:
            return str(e)
        self.dispatch(run_ids, groups, recipients)
        self.cleanup()
        return "Run completed!"
