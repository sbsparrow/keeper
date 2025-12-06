from datetime import datetime, timedelta, timezone
import json
import logging
import os
from shutil import rmtree
from tempfile import gettempdir
from time import localtime
from zipfile import ZipFile

import jcs

logger = logging.getLogger(__name__)


def format_backup_metadata(keeper_id: str,
                           checksum: str,
                           size: int,
                           email: str | None) -> bytes | str:
    """Format the bakup metadata as canonicallized json.

    :param keeper_id: The keeper ID
    :type keeper_id: str
    :param checksum: The checksum of the list of artifact metadata, sorted by ID
    :type checksum: str
    :param size: The total size of all artifact files in the backup
    :type size: int
    :param email: The keeper email address | None, optional
    :type email: str
    :return: UTF-8 encoded canonicallized json
    :rtype: bytes
    """
    utc_offset = localtime().tm_gmtoff
    machine_tz = timezone(timedelta(seconds=utc_offset))
    now = datetime.now(tz=machine_tz)
    uncanonicalized_metadata = {
        "format_version": 1,
        "keeper_id": keeper_id,
        "checksum": checksum,
        "size": size,
        "email": email,
        "created_at": now.strftime('%Y-%m-%dT%H:%M:%SZ')
    }
    if email is None:
        uncanonicalized_metadata.pop('email')
    return jcs.canonicalize(uncanonicalized_metadata)


def prune_dirs(artifact_ids: list[str], backup_root: str) -> set:
    """Remove untrackted files & directories from the backup root directory

    :param artifact_ids: The list of artifact ids, assumed to be allowable subdirs
    :type artifact_ids: list[str]
    :param backup_root: The root directory of the backup
    :type backup_root: str
    :return: The list of removed paths
    :rtype: list
    """
    pruned_items = []

    tempdir = gettempdir()
    if os.path.commonpath([tempdir, backup_root]) != tempdir:
        logger.error(f"Backup dir: {backup_root}, not within system temp: {tempdir}")
        logger.error("Declining to prune files outside system temp prevent possible unintended file deletion.")
        return pruned_items

    expected_paths = artifact_ids + ['backup.json']
    for item in os.listdir(backup_root):
        if item not in expected_paths:
            unexpected_path = os.path.join(backup_root, item)
            pruned_items.append(unexpected_path)
            logger.info(f"Pruning {unexpected_path}")
            if os.path.islink(unexpected_path):
                os.unlink(unexpected_path)
            elif os.path.isdir(unexpected_path):
                rmtree(unexpected_path, ignore_errors=True)  # unlinks symlinks; does not follow them
            else:
                os.remove(unexpected_path)
    return pruned_items


class BackupZip(ZipFile):
    """A zip archive class with additional functions helpful for backup workflows"""

    def get_checksum(self) -> str | None:
        """Read the checksum of backup from the zip's backup.json file.

        :return: The checksum from backup.json or None if unreadable
        :rtype: str | None
        """
        try:
            with self.open('backup.json') as prior_manifest:
                return json.loads(prior_manifest.read()).get("checksum")
        except (json.JSONDecodeError, FileNotFoundError, KeyError) as e:
            logger.warning("Backup checksum unreadable.")
            logger.debug(e)
            return None

    def zip_dir(self, dir: str) -> None:
        """Add an entire directory tree to the zip file

        :param dir: The directory tree to zip
        :type dir: str
        """
        logger.info(f"Zipping {dir} up into {self.filename}")
        for directory, child_dirs, files in os.walk(dir):
            for file in files:
                path = os.path.join(directory, file)
                self.write(path, arcname=os.path.relpath(path, dir))
