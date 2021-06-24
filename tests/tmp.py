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
from prompt_toolkit.shortcuts import (
    checkboxlist_dialog,
    input_dialog
)


def get_patched_app(input, app_function):
    patched_app = app_function()
    patched_app.input = input
    patched_app.output = DummyOutput()
    return patched_app


def test_request_groups():
    dispatcher = RunDispatcher()
    inp = create_pipe_input()
    inp.send_text("\t\t\r\r")
    group = ""
    app = input_dialog(
            title="Research Group",
            text="Please enter the research group name:"
        )
    group = app.run()
    print(group)

test_request_groups()
