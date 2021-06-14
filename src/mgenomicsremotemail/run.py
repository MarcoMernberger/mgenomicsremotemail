# -*- coding: utf-8 -*-
"""
This is the executable run file for the dispatcher.
"""
import sys
import logging
from mgenomicsremotemail import __version__
from prompt_toolkit.shortcuts import (
    checkboxlist_dialog,
    radiolist_dialog,
    progress_dialog,
    input_dialog
)
from .dispatcher import RunDispatcher


__author__ = "MarcoMernberger"
__copyright__ = "MarcoMernberger"
__license__ = "mit"

_logger = logging.getLogger(__name__)


if __name__ == "__main__":
    dispatcher = RunDispatcher()
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        dispatcher.print_run_ids()
        dispatcher.check_all_folders()

    # sen_id = prompt("Enter run id: ")
    # group = prompt("Enter research group: ")
    # recipients = prompt("Enter recipients: ")

    run_ids = ["210525_M03491_0002_000000000-JK2M3"]
    group = "ME"
    recipients = ["ME"]
    run_id_values = [(x, x) for x in dispatcher.run_ids]
    run_ids = checkboxlist_dialog(
        title="Run IDs",
        text="Select the run ids:",
        values=run_id_values,
    ).run()
    print("run_ids", run_ids)
    group = input_dialog(
        title="Research Group", text="Please enter the research group name:"
    ).run()
    print("group", group)
    recipients_ok = False
    textr = "Please enter the recipient enails as comma-separated list:"
    style = None
    """
    while not recipients_ok:
        recipients_ok = True
        recipients = input_dialog(
            title="Recipients",
            text=textr,
            style=style,
        ).run()
        if recipients is not None:
            recipients = recipients.split(",")
            for email in recipients:
                try:
                    validate_email(email)
                except EmailNotValidError:
                    textr = FormattedText(
                        [
                            (
                                "#ff0000",
                                "One or more emails where not valid.\n Please enter the recipient enails as comma-separated list:",
                            )
                        ]
                    )
                    recipients_ok = False
    print("Recipients", recipients)
    """
#    dispatcher.dispatch(run_ids, group, recipients)
#    dispatcher.cleanup()
