"""Ace Archive Keeper tool.

A tool for participants (called "keepers") to host backups of Ace Archive.
"""
import argparse
import datetime
from hashlib import sha256
import logging
import os
import sys
from tempfile import TemporaryDirectory
from typing import NoReturn
from zipfile import BadZipfile, ZIP_STORED

import jcs
from pathvalidate.argparse import validate_filepath_arg

from acearchive_keeper.api import get_server_checksum, list_archive, tell_acearchive_about_this_backup
from acearchive_keeper.backup import BackupZip, format_backup_metadata, prune_dirs
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
    parser.add_argument("-z", "--archive-zip", type=validate_filepath_arg, default="ace-archive.zip",
                        help="Path to the archive zip file backup files to. Defaults to ace-archive.zip.")
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
    zip_path = args.archive_zip

    setup_logging(logger,
                  log_file=args.log_file,
                  log_verbosity=args.verbose,
                  surpress_stderr=args.quiet)

    logger.info("Getting current ace-archive checksum")
    server_checksum_format_version, server_checksum = get_server_checksum(checksum_api_url=args.checksum_api_url)

    files_fetched = 0

    logger.info("Creating temporary directory to hold in-progress backup")
    with TemporaryDirectory() as tempdir:
        logger.info(f"Created tempdir: {tempdir}")

        try:
            logger.info(f"Trying to get on-disk backup checksum from a prior backup zip: {zip_path}, if it exists")
            with BackupZip(zip_path) as backup_zip:
                if backup_zip.get_checksum() == server_checksum:
                    logger.info("On-disk checksum of prior backup matches server's checksum. Skipping backup run")
                    logger.info(f"Backup completed. {files_fetched} files downloaded.")
                    sys.exit(0)
                else:
                    logger.info("On-disk checksum of prior backup does not match server's checksum")
                    logger.info("Extracting prior backup zip to tempdir for incremental backup")
                    backup_zip.extractall(tempdir)

        except BadZipfile:
            logger.warning(f"{zip_path} exists but it is not a zip file. Moving aside so we don't overrite")
            new_path = f"{zip_path}.{datetime.now().timestamp()}.bakup"
            logger.warning(f"Moving {zip_path} to {new_path}")
            os.rename(zip_path, new_path)
        except FileNotFoundError:
            logger.info("No existing archive file found. Proceeding with full backup")

        backup_size = 0
        artifact_ids = []
        artifacts_medatata = []

        ace_artifacts = list_archive(archive_url=args.src_url)
        ace_artifacts.sort(key=lambda x: x.id)
        logger.info(f"Archive contains {len(ace_artifacts)} artifacts.")
        for artifact in ace_artifacts:
            artifact.backup(backup_root=tempdir)
            artifact_ids.append(artifact.id)
            artifacts_medatata.append(artifact.metadata())
            backup_size += artifact.size
            files_fetched += artifact.files_fetched
            artifact.prune_old_files(backup_root=tempdir)

        backup_checksum = sha256(jcs.canonicalize(artifacts_medatata)).hexdigest()
        archive_metadata = format_backup_metadata(keeper_id, backup_checksum, backup_size, keeper_email)
        with open(os.path.join(tempdir, "backup.json"), 'wb+') as manifest:
            manifest.write(archive_metadata)

        logger.info(f"Artifact backup completed. Backup checksum: {backup_checksum}")
        logger.info("Pruning deleted artifacts from backup")
        prune_dirs(artifact_ids=artifact_ids, backup_root=tempdir)

        logger.info(f"Zipping {tempdir} up into {zip_path}")

        with BackupZip(zip_path, 'w', compression=ZIP_STORED) as new_backup:
            new_backup.zip_dir(tempdir)

        logger.info("Zip completed")
        logger.info(f"Cleaning up temporary dir: {tempdir}")

    logger.info("Temporary dir cleaned up")

    logger.info("Notifying ace-archive backup api of completed backup")
    tell_acearchive_about_this_backup(
        backup_api_url=args.backup_api_url,
        keeper_id=keeper_id,
        keeper_email=keeper_email,
        backup_size=backup_size,
        backup_checksum=backup_checksum,
    )
    logger.info(f"Backup completed. {files_fetched} files downloaded.")


if __name__ == "__main__":
    main_cli()
