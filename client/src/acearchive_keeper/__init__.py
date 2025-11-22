"""Ace Archive Keeper tool.

A tool for participants (called "keepers") to host backups of Ace Archive.
"""
import argparse
from hashlib import sha256
import json
import logging
import os
import sys
from typing import NoReturn

import jcs
from pathvalidate.argparse import validate_filepath_arg

from acearchive_keeper.api import get_server_checksum, list_archive, tell_acearchive_about_this_backup
from acearchive_keeper.backup import format_backup_metadata
from acearchive_keeper.utils import configure, setup_logging

ACEARCHIVE_API_URI = "https://api.acearchive.lgbt/v0"

logger = logging.getLogger(__name__)


def get_args() -> argparse.Namespace:
    """Parse command line arguments.

    :return: The arguments
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-url", type=str, default=ACEARCHIVE_API_URI,
                        help=f"URL of the Ace Archive API to backup. Defaults to {ACEARCHIVE_API_URI}")
    parser.add_argument("-d", "--destination", type=validate_filepath_arg, default="ace-archive",
                        help="Local destination directory to backup files to. Defaults to ace-archive.")
    parser.add_argument("-e", "--email", type=str, required=False,
                        help="""Optionally provide an email address.
                        This will only be used in a disaster recovery event. Not required.""")
    parser.add_argument("-l", "--log-file", type=validate_filepath_arg, required=False,
                        help="Log what's happening to the specified file. Defaults to no log file.")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="""Increase logging verbosity to both the log file and stderr.
                        Can be specified multiple times for more verbosity. Defaults to logging errors only.""")
    parser.add_argument("-q", "--quiet", action="store_true", required=False,
                        help="Do not print log messages to stderr.")
    parser.add_argument("--config-file", type=validate_filepath_arg, default="keeper.conf",
                        help="""Config file to read the keeper ID and optional keeper email address from,
                        Defaults to 'keeper.conf'. This Should not generally be changed.""")
    # Hidden arguments for internal testing only.
    parser.add_argument("--backup-api-url", type=str, default=f"{ACEARCHIVE_API_URI}/backups",
                        help=argparse.SUPPRESS)
    parser.add_argument("--checksum-api-url", type=str, default=f"{ACEARCHIVE_API_URI}/checksum",
                        help=argparse.SUPPRESS)
    return parser.parse_args()


def main_cli() -> NoReturn:
    """Run main script logic."""
    args = get_args()
    keeper_id, keeper_email = configure(args.config_file, args.email)

    setup_logging(logger,
                  log_file=args.log_file,
                  log_verbosity=args.verbose,
                  surpress_stderr=args.quiet)

    server_checksum_format_version, server_checksum = get_server_checksum(checksum_api_url=args.checksum_api_url)

    backup_manifest = os.path.join(args.destination, "backup.json")
    try:
        with open(backup_manifest) as prior_manifest:
            if (server_checksum_format_version and server_checksum) is not None:
                manifest_data = json.loads(prior_manifest.read())
                prior_manifest_checksum = manifest_data.get("checksum")
                if prior_manifest_checksum == server_checksum:
                    logger.info("On-disk checksum of last backup matches server's checksum. Skipping backup run.")
                    sys.exit(0)
    except (FileNotFoundError):
        logger.info("Previous backup manifest not found. Unable to compare checksums.")
    except (json.JSONDecodeError, KeyError):
        logger.info("Previous backup manifest exists but checksum can not be read from it. Unable to compare checksums.")

    backup_size = 0
    ace_artifacts = list_archive(archive_url=args.src_url)
    ace_artifacts.sort(key=lambda x: x.id)
    artifacts_medatata = []
    for artifact in ace_artifacts:
        artifact.backup(backup_root=args.destination)
        artifacts_medatata.append(artifact.metadata())
        backup_size += artifact.size
    logger.info("Archive backup completed.")

    backup_checksum = sha256().update(jcs.canonicalize(artifacts_medatata)).hexidigest()
    archive_metadata = format_backup_metadata(keeper_id, backup_checksum, backup_size, keeper_email)
    with open(os.path.join(args.destination, "backup.json"), 'wb+') as manifest:
        manifest.write(archive_metadata)

    tell_acearchive_about_this_backup(
        backup_api_url=args.backup_api_url,
        keeper_id=keeper_id,
        keeper_email=keeper_email,
        backup_size=backup_size,
        backup_checksum=backup_checksum,
    )


if __name__ == "__main__":
    main_cli()
