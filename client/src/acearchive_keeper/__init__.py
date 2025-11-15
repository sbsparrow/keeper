"""Ace Archive Keeper tool.

A tool for participants (called "keepers") to host backups of Ace Archive.
"""
import argparse
from dataclasses import asdict, dataclass
from hashlib import sha256
import logging
import os
import sys
from typing import BinaryIO, NoReturn

import jcs
import requests
from requests.exceptions import HTTPError, JSONDecodeError

ACEARCHIVE_API_URI = "https://api.acearchive.lgbt/v0"

logger = logging.getLogger(__name__)


def read_on_disk_hash(filepath: str) -> str:
    """Get the sha256 hash of a file located at a given path.

    :param filepath: The path of the file to calculate the hash for
    :type filepath: str
    :return: The sha256 hash
    :rtype: str
    """
    BUF_SIZE = 1024 * 1024
    filesha = sha256()
    with open(filepath, "rb") as fh:
        while True:
            byte_batch = fh.read(BUF_SIZE)

            if not byte_batch:
                break

            filesha.update(byte_batch)
    return filesha.hexdigest()


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

    def __post_init__(self) -> NoReturn:
        """Convert dicts for files & links into ArtifactFile & LinkModel respectively."""
        files = [ArtifactFile(**file) for file in self.files]
        links = [LinkModel(**link) for link in self.links]
        self.files = files
        self.links = links

    def metadata(self) -> str:
        """Generate metadata for this artifact.

        Generated metadata is the same as the artifact api-response.

        :return: Artifact metadata
        :rtype: str
        """
        return jcs.canonicalize(asdict(self))

    def get_hash(self) -> str:
        """Generate a sha256 hash of this artifact's metadata.

        :return: The hash of the metadata
        :rtype: str
        """
        item_sha = sha256()
        item_sha.update(self.metadata())
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
                metadata_file.write(self.metadata())
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
        logger.info(f"Starting backup for {self.id}")
        backup_path = f"{backup_root}/{self.id}"
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
            file_path = f"{backup_path}/{archive_file.filename}"
            logger.info(f"Starting backup of {self.id}:{archive_file.filename}")
            try:
                on_disk_hash = read_on_disk_hash(file_path)
                if archive_file.hash == on_disk_hash:
                    msg = f"""Skipping file download for {self.id}{archive_file.filename}. On-disk of hash of file {file_path} matches archive hash: {archive_file.hash}"""
                    logger.info(msg)
                    continue  # backup not needed; skip to loop iteration; i.e. next file
            except FileNotFoundError:
                pass  # no on disk copy of this file; that's fine

            try:
                # Some artifacts have files with filenames that contain '/'s.
                # For example {artifact_id}/{some_string}/index.html
                # For these artifacts, create the subdirectory. But don't recurse.
                if not os.path.isdir(os.path.dirname(file_path)):
                    logger.warning(f"""Trying to create containing directory for {file_path}. This artifact may have a '/' in the filename. Backing up this file may fail.""")
                    os.mkdir(os.path.dirname(file_path))

                with open(file_path, "wb") as file:
                    archive_file.fetch_file(file)

            except (FileNotFoundError, HTTPError, FileExistsError) as e:
                logger.error(f"Could not write artifact to {file_path}: {e}. Skipping this file.")

        metadata_path = f"{backup_path}/metadata.json"
        logger.info(f"Writing metadata.json for artifact {self.id} to {metadata_path}.")
        self.write_metadata(metadata_path)


def list_archive(archive_url: str, artifacts_per_page: int = 100) -> list[AceArtifact]:
    """Read all artifacts from the ace-archive api and returns them as a list of AceArtifacts.

    :param archive_url: The base url of the ace-archive api.
    :type archive_url: str
    :param artifacts_per_page: Number of artifacts_per_page
    :type artifacts_per_page: int
    :return: The artifacts in the ace-archive
    :rtype: list[AceArtifact]
    """
    artifacts = []
    artifact_url = f"{archive_url}/artifacts/"
    params = {'limit': artifacts_per_page}
    while True:
        logger.info(f"Getting next page of artifacts from {artifact_url}")
        response = requests.get(url=artifact_url, params=params)
        try:
            response.raise_for_status()
            json_response = response.json()

            logger.info(f"Got {len(json_response.get("items"))} artifacts on this page.")
            for artifact in json_response.get("items"):
                try:
                    artifacts.append(AceArtifact(**artifact))
                except TypeError:
                    logger.error(f"""Input validation failed for artifact: {artifact}. API returned something that doesn't look like an ace-archive artifact. Skipping backup of this artifact.""")

            try:
                params.update({"cursor": json_response["next_cursor"]})
                logger.debug(f"Next cursor is: {json_response.get("next_cursor", "")}")
            except KeyError:
                logger.info("next_cursor not found in response. This is the last page.")
                break

        except HTTPError as e:
            logger.critical(f"""Error getting artifacts from ace-archive api. {e.response.status_code} recived from URL: {e.request.url}. This is likely an issue with the API or your connection to it. Is the source url set correctly?""")
        except JSONDecodeError:
            logger.critical("""Failed to decode json response from API. This is likely an issue with the API or your connection to it. Is the source url set correctly?""")
    return artifacts


def setup_logging(logger: logging.Logger,
                  log_file: str | None,
                  log_verbosity: int,
                  surpress_stderr: bool) -> NoReturn:
    """Configure log handlers based.

    :param log_file: The file to log to
    :type log_file: str
    :param log_verbosity: How verbose should the log messages be.
    :type log_verbosity: int
    :param stderr_logging: Surpress stderr logs?
    :type stderr_logging: bool
    """
    handlers = []
    utcz_formatter = logging.Formatter("%(asctime)s,%(msecs)03dZ %(name)-14s %(levelname)-8s %(message)s",
                                       datefmt="%Y-%m-%dT%H:%M:%S")

    if log_file:
        file_handler = logging.FileHandler(log_file, 'a')
        file_handler.setFormatter(utcz_formatter)
        handlers.append(file_handler)

    if not surpress_stderr:
        stderr_handler = logging.StreamHandler(stream=sys.stderr)
        stderr_handler.setFormatter(utcz_formatter)
        handlers.append(stderr_handler)

    if log_verbosity == 0:
        log_level = logging.ERROR
    elif log_verbosity == 1:
        log_level = logging.WARNING
    elif log_verbosity == 2:
        log_level = logging.INFO
    elif log_verbosity >= 2:
        log_level = logging.DEBUG

    logger.setLevel(log_level)
    for handler in handlers:
        handler.setLevel(log_level)
        logger.addHandler(handler)
    return logger


def get_args() -> argparse.Namespace:
    """Parse command line arguments.

    :return: The arguments
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-url", type=str, default=ACEARCHIVE_API_URI,
                        help=f"URL of the Ace Archive API to backup. Defaults to {ACEARCHIVE_API_URI}")
    parser.add_argument("-d", "--destination", type=str, default=f"{os.getcwd()}/ace-archive",
                        help=f"Local destination directory to backup files to. Defaults to {os.getcwd()}/ace-archive.")
    parser.add_argument("-l", "--log-file", type=str, required=False,
                        help="Log what's happening to the specified file. Defaults to no log file.")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="""Increase logging verbosity to both the log file and stderr.
                        Can be specified multiple times for more verbosity. Defaults to logging errors only.""")
    parser.add_argument("-q", "--quiet", action="store_true", required=False,
                        help="Do not print log messages to stderr.")
    return parser.parse_args()


def main_cli() -> NoReturn:
    """Run main script logic."""
    args = get_args()

    setup_logging(logger,
                  log_file=args.log_file,
                  log_verbosity=args.verbose,
                  surpress_stderr=args.quiet)

    logger.info(f"Using backup dir: {args.destination}")
    if not os.path.exists(args.destination):
        logger.info(f"Creating backup dir: {args.destination}")
        os.mkdir(args.destination)

    archive_hash = sha256()
    ace_artifacts = list_archive(archive_url=args.src_url)
    ace_artifacts.sort(key=lambda x: x.id)
    for artifact in ace_artifacts:
        artifact.backup(backup_root=args.destination)
        logger.info(f"Adding {artifact.id}'s metadata hash to the running archive hash.")
        archive_hash.update(artifact.get_hash().encode("utf-8"))
    logger.info("Archive backup completed.")
    logger.info(f"Archive hash: {archive_hash.hexdigest()}")


if __name__ == "__main__":
    main_cli()
