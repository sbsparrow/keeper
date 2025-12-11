"""Ace Archive Keeper tool.

A tool for participants (called "keepers") to host backups of Ace Archive.
"""
import asyncio
from concurrent.futures import Future
import datetime
import logging
from multiprocessing import Queue
import os
import sys
from tempfile import TemporaryDirectory
import traceback
from zipfile import BadZipfile, ZIP_STORED

from pydantic import HttpUrl, EmailStr

from acearchive_keeper.api import get_server_checksum, list_archive, tell_acearchive_about_this_backup
from acearchive_keeper.backup import Backup, BackupZip
from acearchive_keeper.utils import setup_gui_logger

ACEARCHIVE_API_URI = "https://api.acearchive.lgbt/v0"
ACEARCHIVE_BACKUPS_API_URI = ACEARCHIVE_API_URI + "/backups"
ACEARCHIVE_CHECKSUM_API_URI = ACEARCHIVE_API_URI + "/checksum"

logger = logging.getLogger(__name__)


def run_gui_worker(
        keeper_id: str,
        keeper_email: str,
        zip_path: str,
        src_url: str,
        checksum_api_url: str,
        backup_api_url: str,
        cancel_event,
        log_queue: Queue,
        progress_queue: Queue | None = None,
        log_verbose: bool = False) -> None:
    """
    Worker entry point executed inside a separate process.
    """

    logger = logging.getLogger()
    logger.handlers.clear()
    if log_verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    setup_gui_logger(logger, log_queue, level)

    async def wrapper():
        try:
            keeper_task = asyncio.create_task(main(
                keeper_id=keeper_id,
                keeper_email=keeper_email,
                zip_path=zip_path,
                src_url=src_url,
                checksum_api_url=checksum_api_url,
                backup_api_url=backup_api_url,
                progress_queue=progress_queue))

            while not keeper_task.done():
                if cancel_event.is_set():
                    logger.warning("Stop requested. Cancelling keeper task")
                    keeper_task.cancel()
                    break

                await asyncio.sleep(0.1)

            # Await the task to propagate cancellation or result
            try:
                result = await keeper_task
                logger.info("Worker finished successfully.")
                return {"status": "success", "result": result}

            except asyncio.CancelledError:
                logger.warning("Worker cancelled.")
                return {"status": "cancelled"}

        except Exception as exc:
            tb = traceback.format_exc()
            logger.error(f"Worker crashed: {exc}\n{tb}")
            return {"status": "error", "message": str(exc), "traceback": tb}

    asyncio.run(wrapper())


async def main(keeper_id: str,
               keeper_email: EmailStr,
               zip_path: str,
               src_url: HttpUrl,
               checksum_api_url: HttpUrl,
               backup_api_url: HttpUrl,
               progress_queue: Queue | None = None):

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
        logger.info(msg="Checking for moved or renamed artifacts.")
        backup.find_moved_artifacts(expected_artifact_dirs)
        logger.info("Starting artifact download.")
        async with asyncio.TaskGroup() as tg:
            artifact_tasks = [tg.create_task(backup.backup_artifact(artifact)) for artifact in ace_artifacts]

            if progress_queue is not None:
                progress_queue.put({"type": "artifact_total", "value": len(ace_artifacts)})

                def artifact_callback(future: Future):
                    progress_queue.put({"type": "artifact_completed", "value": 1})

                [task.add_done_callback(artifact_callback) for task in artifact_tasks]

            await asyncio.gather(
                *artifact_tasks
            )
#        await asyncio.gather(
#            *[backup.backup_artifact(artifact) for artifact in ace_artifacts]
#        )
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
