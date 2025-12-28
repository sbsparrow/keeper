"""Ace Archive Keeper tool.

A tool for participants (called "keepers") to host backups of Ace Archive.
"""
import argparse
import asyncio
import logging
import sys
from toml import TomlDecodeError
from typing import NoReturn

from pathvalidate.argparse import validate_filepath_arg
from pydantic import HttpUrl

from acearchive_keeper.configure import get_config_path, read_config, ValidationError
from acearchive_keeper.gui import main as run_gui
from acearchive_keeper.utils import setup_logging, valid_email
from acearchive_keeper.worker import main as worker_main, ACEARCHIVE_API_URI, ACEARCHIVE_BACKUPS_API_URI, ACEARCHIVE_CHECKSUM_API_URI


logger = logging.getLogger()


def get_args() -> argparse.Namespace:
    """Parse command line arguments.

    :return: The arguments
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-url", type=HttpUrl, default=ACEARCHIVE_API_URI,
                        help=f"URL of the Ace Archive API to backup. Defaults to {ACEARCHIVE_API_URI}")
    parser.add_argument("-z", "--archive-zip", type=validate_filepath_arg, default="ace-archive.zip",
                        help="Path to the archive zip file backup files to. Defaults to ace-archive.zip.")
    parser.add_argument("-e", "--email", type=valid_email, required=False,
                        help="""Optionally provide an email address.
                        This will only be used in a disaster recovery event. Not required.""")
    parser.add_argument("-l", "--log-file", type=validate_filepath_arg, required=False,
                        help="Log what's happening to the specified file. Defaults to no log file.")
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="""Increase logging verbosity to both the log file and stderr.
                        Can be specified multiple times for more verbosity. Defaults to logging errors only.""")
    parser.add_argument("-q", "--quiet", action="store_true", required=False,
                        help="Do not print log messages to stderr.")
    parser.add_argument("--config-file", type=validate_filepath_arg, default=get_config_path(),
                        help="""Config file to read the keeper ID and optional keeper email address from,
                        Defaults to 'keeper.conf'. This Should not generally be changed.""")
    parser.add_argument("-g", "--gui", action="store_true", default=False,
                        help="Start the GUI.")
    # Hidden arguments for internal testing only.
    parser.add_argument("--backup-api-url", type=HttpUrl, default=ACEARCHIVE_BACKUPS_API_URI,
                        help=argparse.SUPPRESS)
    parser.add_argument("--checksum-api-url", type=HttpUrl, default=ACEARCHIVE_CHECKSUM_API_URI,
                        help=argparse.SUPPRESS)
    return parser.parse_args()


def main_cli() -> NoReturn:
    """Run main script logic."""
    args = get_args()
    if args.gui:
        run_gui()
    else:
        try:
            config = read_config(args.config_file)
            keeper_id = config.Keeper.id
        except ValidationError as e:
            logger.error("Invalid config file")
            logger.error(e.errors)
            sys.exit(1)
        except TomlDecodeError as e:
            logger.error(e)
            sys.exit(1)

        setup_logging(logger,
                      log_file=args.log_file,
                      log_verbosity=args.verbose,
                      surpress_stderr=args.quiet)

        asyncio.run(worker_main(
            keeper_id=keeper_id,
            keeper_email=args.email,
            zip_path=args.archive_zip,
            src_url=args.src_url,
            backup_api_url=args.backup_api_url,
            checksum_api_url=args.checksum_api_url))


if __name__ == "__main__":
    main_cli()
