from datetime import datetime, timedelta, timezone
import json
import logging
import os
from time import localtime
from zipfile import BadZipFile, ZipFile

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
        except (json.JSONDecodeError, FileNotFoundError, KeyError):
            logger.warning("Backup checksum unreadable.")
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
