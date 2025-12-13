"""Ace Archive Keeper tool.

A tool for participants (called "keepers") to host backups of Ace Archive.
"""
import argparse
import asyncio
import datetime
import logging
import os
import sys
from tempfile import TemporaryDirectory
from toml import TomlDecodeError
from typing import NoReturn
from zipfile import BadZipfile, ZIP_STORED

from pathvalidate.argparse import validate_filepath_arg
from pydantic import HttpUrl, EmailStr

from acearchive_keeper.api import get_server_checksum, list_archive, tell_acearchive_about_this_backup
from acearchive_keeper.configure import empty_config, read_config, write_config, ValidationError
from acearchive_keeper.backup import Backup, BackupZip
from acearchive_keeper.utils import generate_keeper_id, setup_logging, valid_email

ACEARCHIVE_API_URI = "https://api.acearchive.lgbt/v0"

logger = logging.getLogger(__name__)


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
    parser.add_argument("--config-file", type=validate_filepath_arg, default="keeper.conf",
                        help="""Config file to read the keeper ID and optional keeper email address from,
                        Defaults to 'keeper.conf'. This Should not generally be changed.""")
    # Hidden arguments for internal testing only.
    parser.add_argument("--backup-api-url", type=HttpUrl, default=f"{ACEARCHIVE_API_URI}/backups",
                        help=argparse.SUPPRESS)
    parser.add_argument("--checksum-api-url", type=HttpUrl, default=f"{ACEARCHIVE_API_URI}/checksum",
                        help=argparse.SUPPRESS)
    return parser.parse_args()


def main_cli() -> NoReturn:
    """Run main script logic."""
    args = get_args()
    try:
        config = read_config(args.config_file)
        keeper_id = config.Keeper.id
    except FileNotFoundError:
        logger.info("No config file found")
        logger.info("Generating empty config with new keeper_id")
        keeper_id = generate_keeper_id()
        write_config(args.config_file, empty_config(keeper_id))
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

    asyncio.run(main(
        keeper_id=keeper_id,
        keeper_email=args.email,
        zip_path=args.archive_zip,
        src_url=args.src_url,
        backup_api_url=args.backup_api_url,
        checksum_api_url=args.checksum_api_url))


async def main(keeper_id: str,
               keeper_email: EmailStr,
               zip_path: str,
               src_url: HttpUrl,
               checksum_api_url: HttpUrl,
               backup_api_url: HttpUrl):
    logger.info("Getting current ace-archive checksum")
    server_checksum_format_version, server_checksum = get_server_checksum(checksum_api_url=checksum_api_url)

    logger.info("Creating temporary directory to hold in-progress backup")
    with TemporaryDirectory() as tempdir:
        logger.info(f"Created tempdir: {tempdir}")

        try:
            logger.info(f"Trying to get on-disk backup checksum from a prior backup zip: {zip_path}, if it exists")
            with BackupZip(zip_path) as backup_zip:
                if backup_zip.get_checksum() == server_checksum:
                    logger.info("On-disk checksum of prior backup matches server's checksum. Skipping backup run")
                    logger.info("Backup completed. 0 files downloaded.")
                    sys.exit(0)
                else:
                    logger.info("On-disk checksum of prior backup does not match server's checksum")
                    logger.info("Extracting prior backup zip to tempdir for incremental backup")
                    backup_zip.extractall(tempdir)

        except BadZipfile:
            logger.warning(f"{zip_path} exists but it is not a zip file. Moving aside so we don't overrite")
            new_path = f"{zip_path}.{datetime.datetime.now().timestamp()}.bakup"
            logger.warning(f"Moving {zip_path} to {new_path}")
            os.rename(zip_path, new_path)
        except FileNotFoundError:
            logger.info("No existing archive file found. Proceeding with full backup")

        logger.info(f"Geting artifacts from archive url: {src_url}.")
        ace_artifacts = list_archive(archive_url=src_url)
        logger.info(f"Archive contains {len(ace_artifacts)} artifacts.")

        expected_artifact_dirs = {a.get_artifact_dir() for a in ace_artifacts}

        backup = Backup(backup_root=tempdir)
        backup.find_moved_artifacts(expected_artifact_dirs)
        await asyncio.gather(
            *[backup.backup_artifact(artifact) for artifact in ace_artifacts]
        )
        backup.generate_backup_checksum()
        backup.write_backup_manifest(keeper_id=keeper_id, keeper_email=keeper_email)

        logger.info("Pruning deleted artifacts from backup")
        # We could pass unexpected_dirs into prune_dirs instead of re-checking the artifact dir contents,
        # But that let's not do that.
        pruned_from_artifacts_dir = backup.prune_artifacts(expected_artifact_dirs)
        pruned_from_backup_root = backup.prune_backup_root()

        logger.info(f"Zipping {backup.backup_root} up into {zip_path}")
        with BackupZip(zip_path, 'w', compression=ZIP_STORED) as new_backup:
            new_backup.zip_dir(backup.backup_root)

        logger.info("Zip completed")
        logger.info(f"Cleaning up temporary dir: {backup.backup_root}")

    logger.info("Temporary dir cleaned up")

    logger.info("Notifying ace-archive backup api of completed backup")
    tell_acearchive_about_this_backup(
        backup_api_url=backup_api_url,
        keeper_id=keeper_id,
        keeper_email=keeper_email,
        backup_size=backup.backup_size,
        backup_checksum=backup.checksum,
    )
    logger.info("Backup completed.")
    logger.info(f"{backup.files_fetched} files updated for {backup.artifacts_updated} artifacts")
    logger.info(f"Relocated {backup.artifacts_relocated} artifacts.")
    logger.info(f"Pruned {len(backup.pruned_from_artifacts)} files from existing artifacts.")
    logger.info(f"Pruned {len(pruned_from_artifacts_dir)} artifacts.")
    logger.info(f"Pruned {len(pruned_from_backup_root)} items from backup root.")
    logger.info(f"Backup contains a total of {len(ace_artifacts)} with a total size of {backup.backup_size} bytes")


if __name__ == "__main__":
    main_cli()
