from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import logging
import os
from shutil import rmtree
from tempfile import gettempdir
from time import localtime
from typing import LiteralString
from zipfile import ZipFile

import jcs
from pydantic import EmailStr

from acearchive_keeper.artifact import AceArtifact
from acearchive_keeper.utils import canonicalize_pretty

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
    return canonicalize_pretty(uncanonicalized_metadata)


class BackupZip(ZipFile):
    """A zip archive class with additional functions helpful for backup workflows"""

    def get_checksum(self) -> str | None:
        """Read the checksum of backup from the zip's backup.json file.

        :return: The checksum from backup.json or None if unreadable
        :rtype: str | None
        """
        backup_manifest = Backup.get_backup_manifest_name()
        try:
            with self.open(backup_manifest) as prior_manifest:
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


class Backup():
    """An abstraction of the ace-archive backup.

    Holds the backup structure, such as the names the metadata files and location of the artifacts dir.
    """

    @staticmethod
    def get_artifact_dir_name() -> LiteralString:
        """Get the name of the subdirectory that holds artifacts

        :return: The name of the artifact directory
        :rtype: Literal['artifacts']
        """
        return "artifacts"

    @staticmethod
    def get_backup_manifest_name() -> LiteralString:
        """Get the name of the file that holds backup metadata

        :return:  The name of the backup metadata file
        :rtype: Literal['backup.json']
        """

        return "backup.json"

    @staticmethod
    def get_readme_name() -> LiteralString:
        """Get the name of the readme file

        :return: The name of the readme file
        :rtype: Literal['README.md']
        """
        return "README.md"

    @classmethod
    def get_expected_paths(cls) -> list[LiteralString]:
        """Get the list of expected top-level paths in the backups

        :return: The list of expected paths
        :rtype: list[LiteralString]
        """
        return [cls.get_artifact_dir_name(), cls.get_backup_manifest_name(), cls.get_readme_name()]

    def __init__(self, backup_root: str) -> None:
        """Create a backup structure at the given path

        :param backup_root: The to use as the root of the backup
        :type backup_root: str
        """
        self.backup_root = backup_root
        self.artifact_root = os.path.join(backup_root, self.get_artifact_dir_name())
        self.manifest = os.path.join(backup_root, self.get_backup_manifest_name())
        self.readme_file = os.path.join(backup_root, self.get_readme_name())
        self.backup_size = 0
        self.files_fetched = 0
        self.artifacts_updated = 0
        self.artifacts_relocated = 0
        self.pruned_from_artifacts = set()
        self.artifact_metadata = []

        if not os.path.exists(self.artifact_root):
            os.mkdir(self.artifact_root)

    def generate_backup_checksum(self) -> None:
        """Generate a checksum of this backup from the internal artifact metadata list"""
        self.checksum = sha256(jcs.canonicalize(sorted(self.artifact_metadata, key=lambda x: x.get('id')))).hexdigest()

    def write_backup_manifest(self, keeper_id: str, keeper_email: EmailStr | None) -> None:
        """Write the the backup manifest file.

        :param keeper_id: The keeper ID
        :type keeper_id: str
        :param keeper_email: The keeper email address, optional
        :type keeper_email: EmailStr | None
        """
        with open(self.manifest, 'wb+') as manifest:
            manifest.write(format_backup_metadata(
                keeper_id=keeper_id, email=keeper_email, checksum=self.checksum, size=self.backup_size))

    def get_artifact_dirs(self) -> set[str]:
        """Get the list of artifact directories on-disk

        :return: The list of on-disk artifact directories
        :rtype: set[str]
        """
        return {i for i in os.listdir(self.artifact_root)}

    def get_unexpected_dirs(self, expected_dirs: set[str]) -> set:
        """Get a list of unexpected artifact directories

        :param expected_dirs: The list of expected artifact directories
        :type expected_dirs: set[str]
        :return: The set of unexpected directories
        :rtype: set[str]
        """
        return self.get_artifact_dirs() - expected_dirs

    def _get_artifact_metadata_json_path(self, artifact_dirname: str) -> str:
        """Get the full path to an artifact metadata file from the artifact directory name

        Does not check if the metadata file exists.

        :param artifact_dirname: The artifact dir to check for metadata
        :type artifact_dirname: str
        :return: The full path to the artifact metadata file
        :rtype: str
        """
        return os.path.join(self.artifact_root, artifact_dirname, AceArtifact.get_metadata_path())

    def get_id_from_artifact_dirname(self, artifact_dirname: str) -> str:
        """Read the ID of an artifact from the metadata.json in that artifact dir, if it exists

        :param metadata_file: The path to a ace-archive artifact dir
        :type metadata_file: str
        :return: Artifact ID it can be read, None if it can't
        :rtype: str | None
        """
        try:
            with open(self._get_artifact_metadata_json_path(artifact_dirname)) as fh:
                return json.load(fh)['id']
        except NotADirectoryError:
            logger.warning(f"{artifact_dirname} is not an artifact dir")
        except (FileNotFoundError, json.JSONDecodeError):
            logger.warning(f"Could not read on-disk metadata for artifact at: {artifact_dirname}")
        except KeyError:
            logger.warning(f"Read metadata but could not get artifact ID for artifact at: {artifact_dirname}")

    def get_moved_artifacts(self, expected_dirs: set[str]) -> dict[str]:
        """Generate a mapping of artifact ID to dirname for unexpected artifact directories.

        :param expected_dirs: The set of expected directory names
        :type expected_dirs: set[str]
        :return: A mapping of artifact ID to dirname
        :rtype: dict[str]
        """
        unexpected_dirs = self.get_unexpected_dirs(expected_dirs)
        return {self.get_id_from_artifact_dirname(d): d for d in unexpected_dirs if self.get_id_from_artifact_dirname(d) is not None}

    def find_moved_artifacts(self, expected_dirs: set[dict]) -> None:
        """Update the backup's internal set of moved artifacts

        :param expected_dirs: The set of expected artifact dirs
        :type expected_dirs: set[dict]
        """
        self.moved_artifacts = self.get_moved_artifacts(expected_dirs)

    def _prune_dirs(self, expected_paths: set[str], root_dir: str) -> set[str]:
        """Remove unexpected files & directories from the given root directory

        :param expected_paths: The set of expected dirs
        :type expected_paths: set[str]
        :param root_dir: The directory to prune
        :type root_dir: str
        :return: The set of removed paths
        :rtype: set
        """
        pruned_items = []

        tempdir = gettempdir()
        if os.path.commonpath([tempdir, root_dir]) != tempdir:
            logger.error(f"Backup dir: {root_dir}, not within system temp: {tempdir}")
            logger.error("Declining to prune files outside system temp prevent possible unintended file deletion.")
            return pruned_items

        for item in os.listdir(root_dir):
            if item not in expected_paths:
                unexpected_path = os.path.join(root_dir, item)
                pruned_items.append(unexpected_path)
                logger.info(f"Pruning {unexpected_path}")
                if os.path.islink(unexpected_path):
                    os.unlink(unexpected_path)
                elif os.path.isdir(unexpected_path):
                    rmtree(unexpected_path, ignore_errors=True)  # unlinks symlinks; does not follow them
                else:
                    os.remove(unexpected_path)
        return pruned_items

    def prune_artifacts(self, expected_artifacts: set[str]) -> set[str]:
        """Prune unexpected artifacts from the artifacts directory

        :param expected_artifacts: The set of expected artifacts
        :type expected_artifacts: set[str]
        :return: The set of removed paths
        :rtype: set
        """
        return self._prune_dirs(expected_paths=expected_artifacts, root_dir=self.artifact_root)

    def prune_backup_root(self) -> set[str]:
        """Prune unexpected paths from the backup root

        :return: The set of removed paths
        :rtype: set"""
        return self._prune_dirs(self.get_expected_paths(), root_dir=self.backup_root)

    async def backup_artifact(self, artifact: AceArtifact) -> dict:
        """Backup the given artifact and return its metadata.

        Performs the following steps:

        1. Check if the artifact has been moved according to the backup's moved artifacts dict
            and call the artifact's relocate() it if it has.
        2. Call the artifact's backup() method.
        3. Call the artifact's prune_old_files() method.
        4. Update the backup's tracking info about changes made in steps 1-3.
        5. Return the artifact's metadata by calling its metadata() method.

        :param artifact: The artifact to backup
        :type artifact: AceArtifact
        :return: The artifact's metadata
        :rtype: dict
        """
        if artifact.id in self.moved_artifacts.keys():
            old_dir = self.moved_artifacts[artifact.id]
            logger.info(f"Artifact rename detected for: {artifact.id}")
            await artifact.relocate(old_dir=old_dir, backup_root=self.artifact_root)
            artifact_relocated = True
        else:
            artifact_relocated = False
        await artifact.backup(backup_root=self.artifact_root)
        self.pruned_from_artifacts |= await artifact.prune_old_files(backup_root=self.artifact_root)

        if artifact_relocated:
            self.artifacts_relocated += 1
        if artifact_relocated or artifact.files_fetched >= 0:
            self.artifacts_updated += 1
        self.files_fetched += artifact.files_fetched
        self.backup_size += artifact.size
        self.artifact_metadata.append(artifact.metadata())

        return artifact.metadata()
