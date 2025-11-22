from dataclasses import asdict, dataclass
from hashlib import sha256
import logging
import os
from typing import BinaryIO, NoReturn

import jcs
from pathvalidate import sanitize_filename
import requests
from requests.exceptions import HTTPError

from acearchive_keeper.utils import read_on_disk_hash

logger = logging.getLogger(__name__)


@dataclass
class LinkModel():
    """Used for input validation on 'links' field of ace-archive api response."""

    name: str
    url: str


@dataclass
class ArtifactFile():
    """Represents of a file in an ace-archive artifact.

    Used for input validation on 'files' field of ace-archive api response.
    Contains function to download file content.
    """

    filename: str
    name: str
    media_type: str
    hash: str  # noqa: A003
    hash_algorithm: str
    url: str
    hidden: bool
    lang: str | None = None  # Some files are missing this field

    def fetch_file(self, file: BinaryIO) -> int:
        """Download this file from the ace-archive api and write to file.

        :return: The size of the file in bytes.
        :rtype: int
        """
        chunk_size = 128 * 1024
        bytes_written = 0
        try:
            with requests.get(self.url, stream=True) as response:
                response.raise_for_status()
                for chunk in response.iter_content(chunk_size=chunk_size):
                    bytes_written += file.write(chunk)
        except HTTPError as e:
            logger.error(f"Failed to fetch file. {e.response.status_code} recived from URL: {e.request.url}")
            raise
        return bytes_written


@dataclass
class AceArtifact():
    """Represents an artifact in the ace-archive.

    Used for input validation on 'items' field of ace-archive api responses.
    Has methods to backup all contained in this artifact as well as helpers to generate
    artifact metadata and generate an artifact hash.
    """

    id: str  # noqa: A003
    title: str
    summary: str
    url: str
    url_aliases: list[str]
    files: list[dict] | list[ArtifactFile]
    links: list[dict] | list[LinkModel]
    people: list[str]
    identities: list[str]
    decades: list[int]
    collections: list[str]
    from_year: int
    to_year: int | None = None  # Some artifacts are missing this field
    description: str | None = None  # And this one
    size: int | None = None  # Calculated after backup

    def __post_init__(self) -> NoReturn:
        """Convert dicts for files & links into ArtifactFile & LinkModel respectively."""
        files = [ArtifactFile(**file) for file in self.files]
        links = [LinkModel(**link) for link in self.links]
        self.files = files
        self.links = links

    def metadata(self, format: int = 1) -> dict:
        """Generate metadata for this artifact.

        :return: Artifact metadata
        :rtype: dict
        """
        if format != 1:
            logger.warning(f"Unknown metadata format version: {format}. Using format version 1 instead.")
        metadata = asdict(self)
        for fields in ["size", "url_aliases"]:
            metadata.pop(fields)
        return metadata

    def get_hash(self) -> str:
        """Generate a sha256 hash of this artifact's metadata.

        :return: The hash of the metadata
        :rtype: str
        """
        item_sha = sha256()
        item_sha.update(jcs.canonicalize(self.metadata()))
        logger.debug(f"{self.id} : {item_sha.hexdigest()}")
        return item_sha.hexdigest()

    def write_metadata(self, metadata_path: str) -> NoReturn:
        """Write this artifact's metadata to the metadata_path.

        :param metadata_path: The file path to write the metadata to.
        :type metadata_path: str
        """
        with open(metadata_path, "wb+") as metadata_file:
            if metadata_file.read() != self.metadata():
                logger.info(f"Writting updated metadata for {self.id}")
                metadata_file.write(jcs.canonicalize(self.metadata()))
            else:
                logger.info(f"On-disk metadata for {self.id} already up-to-date.")

    def backup(self, backup_root: str) -> NoReturn:
        """Backup this artifact to a subdirectory under backup_root.

        Creates a artifact directory under backup_root to store files & metadata.
        Downloads all files contained in this artifact and saves them the artifact directory.
        Updates the on-disk metadata.json in artifact directory.

        This method will download a new copy of a file UNLESS:
            * The file already exists on disk AND
            * The on-disk file's sha256 hash matches the expected hash

        :param backup_root: The root directory to backup too.
        This artifact will be backed up to a subdirectory of backup_root.
        :type backup_root: str
        """
        artifact_size = 0
        logger.info(f"Starting backup for {self.id}")
        backup_path = os.path.join(backup_root, self.id)
        if not os.path.exists(backup_path):
            logger.info(f"Creating artifact directory: {backup_path}")
            os.mkdir(backup_path)
        elif os.path.exists(backup_path) and not os.path.isdir(backup_path):
            msg = f"Can't create directory to backup artifact. A file with the directory name already exists: {backup_path}"
            logger.error(msg)
            raise FileExistsError(msg)
        else:
            logger.info(f"Artifact directory already exists: {backup_path}")

        for archive_file in self.files:
            sanitized_filename = sanitize_filename(archive_file.filename)
            file_path = os.path.join(backup_path, sanitized_filename)
            logger.info(f"Starting backup of {self.id}:{sanitized_filename}")
            try:
                on_disk_hash = read_on_disk_hash(file_path)
                if archive_file.hash == on_disk_hash:
                    msg = f"""Skipping file download for {self.id}:{sanitized_filename}. On-disk of hash of file {file_path} matches archive hash: {archive_file.hash}"""
                    artifact_size += os.path.getsize(file_path)
                    logger.info(msg)
                    continue  # backup not needed; skip to loop iteration; i.e. next file
            except FileNotFoundError:
                pass  # no on disk copy of this file; that's fine

            try:
                with open(file_path, "wb") as file:
                    archive_file.fetch_file(file)
                    # bytes written to the file as the file size, but file size is what we want.
                artifact_size += os.path.getsize(file_path)

            except (FileNotFoundError, HTTPError, FileExistsError) as e:
                logger.error(f"Could not write artifact to {file_path}: {e}. Skipping this file.")

        self.size = artifact_size
        metadata_path = os.path.join(backup_path, "metadata.json")
        logger.info(f"Writing metadata.json for artifact {self.id} to {metadata_path}.")
        self.write_metadata(metadata_path)
