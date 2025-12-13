import aiofiles
import aiofiles.os as aos
import aiofiles.ospath as apath
import aiohttp
from dataclasses import asdict, dataclass
from hashlib import sha256
import logging
import os
from tempfile import gettempdir
from typing import Callable, LiteralString, NoReturn

import jcs
from pathvalidate import sanitize_filename
from requests.exceptions import HTTPError

from acearchive_keeper.utils import canonicalize_pretty, read_on_disk_hash

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
    short_url: str
    raw_url: str
    hidden: bool
    lang: str | None = None  # Some files are missing this field

    async def stream_file(self, filepath: str) -> int:
        """Download this file from the ace-archive api and write to file.

        :return: The size of the file in bytes.
        :rtype: int
        """
        chunk_size = 128 * 1024
        bytes_written = 0
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.raw_url) as response:
                    response.raise_for_status()
                    async with aiofiles.open(filepath, 'wb') as fh:
                        async for chunk in response.content.iter_chunked(chunk_size):
                            bytes_written += await fh.write(chunk)
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
    short_url: str
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
    files_fetched: int = 0  # Incremented during backup

    def __post_init__(self) -> None:
        """Convert dicts for files & links into ArtifactFile & LinkModel respectively."""
        files = [ArtifactFile(**file) for file in self.files]
        links = [LinkModel(**link) for link in self.links]
        self.files = files
        self.links = links

    @staticmethod
    def get_metadata_path() -> LiteralString:
        """Get the name of an artifact's metadata file

        :return: The name of an artifact's metadata file
        :rtype: LiteralString
        """
        return "metadata.json"

    def get_artifact_dir(self) -> str:
        """Get the artifact dir name

        :return: The artifact dir name
        :rtype: str
        """
        return self.get_slug()

    def metadata(self, format: int = 1) -> dict:
        """Generate metadata for this artifact.

        :param format: The artifact metadata format, defaults to 1
        :type format: int, optional
        :return: Artifact metadata
        :rtype: dict
        """
        if format != 1:
            logger.warning(f"Unknown metadata format version: {format}. Using format version 1 instead.")
        metadata = asdict(self)
        for fields in ["size", "files_fetched", "url_aliases"]:
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

    async def write_metadata(self, metadata_path: str) -> NoReturn:
        """Write this artifacts metadata to the metadata_path.

        :param metadata_path: The file path to write the metadata to.
        :type metadata_path: str
        """
        async with aiofiles.open(metadata_path, "wb+") as metadata_file:
            if await metadata_file.read() != self.metadata():
                logger.debug(f"Writting updated metadata for {self.id}")
                await metadata_file.write(canonicalize_pretty(self.metadata()))
            else:
                logger.debug(f"On-disk metadata for {self.id} already up-to-date.")

    def get_slug(self) -> str:
        """Get the current artifact slug from the current artifact URL

        :return: The artifact slug
        :rtype: str
        """
        return self.url.rsplit('/', 1)[-1]

    def get_slug_aliases(self) -> list[str]:
        """Get the list of artifact slug aliases

        :return: The list of artifact slug aliases
        :rtype: list[str]
        """
        return [url.rsplit('/', 1)[-1] for url in self.url_aliases]

    async def backup(self, backup_root: str) -> NoReturn:
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
        files_fetched = 0
        logger.debug(f"Starting backup for {self.id}")
        artifact_dir = self.get_artifact_dir()
        backup_path = os.path.join(backup_root, artifact_dir)
        if not await apath.exists(backup_path):
            logger.debug(f"Creating artifact directory: {backup_path}")
            await aos.mkdir(backup_path)
        elif await apath.exists(backup_path) and not await apath.isdir(backup_path):
            msg = f"Can't create directory to backup artifact. A file with the directory name already exists: {backup_path}"
            logger.error(msg)
            raise FileExistsError(msg)
        else:
            logger.debug(f"Artifact directory already exists: {backup_path}")

        for archive_file in self.files:
            sanitized_filename = sanitize_filename(archive_file.filename)
            file_path = os.path.join(backup_path, sanitized_filename)
            logger.debug(f"Starting backup of {self.id} to {os.path.join(artifact_dir, sanitized_filename)}")
            try:
                on_disk_hash = await read_on_disk_hash(file_path)
                if archive_file.hash == on_disk_hash:
                    msg = f"""Skipping file download for {self.id} to {os.path.join(artifact_dir, sanitized_filename)}. On-disk of hash of file {file_path} matches archive hash: {archive_file.hash}"""
                    artifact_size += await apath.getsize(file_path)
                    logger.debug(msg)
                    continue  # backup not needed; skip to loop iteration; i.e. next file
                else:
                    logger.info(f"Downloading updated copy of file: {os.path.relpath(file_path, backup_root)}")
            except FileNotFoundError:
                logger.info(f"Downloading file: {os.path.relpath(file_path, backup_root)}")
                pass  # no on disk copy of this file; that's fine

            try:
                bytes_written = await archive_file.stream_file(file_path)
                # bytes written to the file as the file size, but file size is what we want.
                files_fetched += 1
                artifact_size += await apath.getsize(file_path)

            except (FileNotFoundError, HTTPError, FileExistsError) as e:
                logger.error(f"Could not write artifact to {file_path}: {e}. Skipping this file.")

        self.files_fetched = files_fetched
        self.size = artifact_size
        metadata_path = os.path.join(backup_path, self.get_metadata_path())
        await self.write_metadata(metadata_path)

    async def prune_old_files(self, backup_root: str) -> set:
        """Remove untrackted files & directories from the artifact's backup dirv

        :param backup_root: backup_root
        :type backup_root: str
        :return: The set of removed files & directories
        :rtype: set
        """
        pruned_files = set()
        pruned_dirs = set()

        tempdir = gettempdir()
        backup_path = os.path.join(backup_root, self.get_artifact_dir())
        # If trying to prune files outside of the default temp something has gone wrong
        if os.path.commonpath([tempdir, backup_path]) != tempdir:
            logger.error(f"Artifact Dir: {backup_path}, not within system temp: {tempdir}")
            logger.error("Declining to prune files outside system temp prevent possible unintended file deletion.")
            return pruned_files

        artifact_filenames = [sanitize_filename(file.filename) for file in self.files]
        artifact_filenames.append(self.get_metadata_path())

        for dirpath, dirnames, filenames in os.walk(backup_path, topdown=False):
            for filename in filenames:
                if filename not in artifact_filenames:
                    unexpected_filepath = os.path.join(dirpath, filename)
                    logger.info(f"Pruning {unexpected_filepath}")
                    pruned_files.add(unexpected_filepath)
                    await aos.remove(unexpected_filepath)
            has_non_empty_subdirs = False
            for subdir in dirnames:
                # If there are subdirs that have not been pruned
                if os.path.join(dirpath, subdir) not in pruned_dirs:
                    has_non_empty_subdirs = True
            # If there are no files & no subdirs remaining in the dir after pruning
            if not any(set(filenames) - pruned_files) and not has_non_empty_subdirs:
                await aos.rmdir(dirpath)
                pruned_dirs.add(dirpath)
        return pruned_files | pruned_dirs

    async def relocate(self, old_dir: str, backup_root: str) -> str:
        """Relocate an artifact from it's old path to the expected path based on current artifact metadata

        :param old_dir: The existing artifact dir
        :type old_dir: str
        :param backup_root: The root directory of the backup
        :type backup_root: str
        :return: The new artifact path
        :rtype: str
        """
        artifact_dir = self.get_artifact_dir()
        old_path = os.path.join(backup_root, old_dir)
        new_path = os.path.join(backup_root, artifact_dir)
        if apath.exists(new_path):
            logger.warning(f"Can't move {old_dir} to {artifact_dir} Destination directory already exists: {new_path}")
            return old_path
        else:
            await aos.rename(src=old_path, dst=new_path)
            logger.info(f"Moved {old_dir} to {artifact_dir}")
            return new_path
