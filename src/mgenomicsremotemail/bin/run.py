#!/usr/bin/env python3
"""
This is the entry point for the dispatcher.
"""
import click
import sys
from pathlib import Path
sys.path.append(str(Path(__file__, '..', '..', '..').resolve()))
from mgenomicsremotemail.dispatch import RunDispatcher


__author__ = "MarcoMernberger"
__copyright__ = "MarcoMernberger"
__license__ = "mit"


@click.group(invoke_without_command=True)
@click.option("--check", required=False, is_flag=True, help="Check input folders")
@click.option("--ids", required=False, is_flag=True, help="Show available Run IDs")
def run(check, ids):
    dispatcher = RunDispatcher()
    if check:
        click.echo(dispatcher.check_all_folders())
    elif ids:
        click.echo(dispatcher.get_run_ids_string())
    else:
        dispatcher.run()
        click.echo("Run completed")


if __name__ == "__main__":
    run()
