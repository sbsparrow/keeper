"""Ace Archive Keeper tool.

A tool for participants (called "keepers") to host backups of Ace Archive.
"""
import configparser
from typing import NoReturn
from hashlib import sha256
import json
import logging
import os
import sys
from uuid import uuid4

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


def get_id_from_artifact_dir(artifact_dir: str) -> str:
    """Read the ID of an artifact from the metadata.json in that artifact dir

    :param metadata_file: The path to a ace-archive artifact dir
    :type metadata_file: str
    :return: Artifact ID
    :rtype: str
    """
    try:
        with open(os.path.join(artifact_dir, "metadata.json")) as fh:
            return json.load(fh)['id']
    except NotADirectoryError:
        logger.warning(f"{artifact_dir} is not an artifact dir")
    except (FileNotFoundError, json.JSONDecodeError):
        logger.warning(f"Could not read on-disk metadata for artifact at: {artifact_dir}")
    except KeyError:
        logger.warning(f"Read metadata but could not get artifact ID for artifact at: {artifact_dir}")


def setup_logging(logger: logging.Logger,
                  log_file: str | None,
                  log_verbosity: bool,
                  surpress_stderr: bool) -> NoReturn:
    """Configure log handlers based.

    :param log_file: The file to log to
    :type log_file: str
    :param log_verbosity: Increase logging verbosity?
    :type log_verbosity: bool
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

    if log_verbosity:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logger.setLevel(log_level)
    for handler in handlers:
        handler.setLevel(log_level)
        logger.addHandler(handler)


def configure(config_file: str, keeper_email: str | None = None) -> tuple[str, str | None]:
    """Update the keeper config file if needed and return the relavent keep info.

    :param config_file: The config file path to read & write. Will be created if absent.
    :type config_file: str
    :param keeper_email: Optionally update the email address in the config file, defaults to None
    :type keeper_email: str | None, optional
    :return: Tuple of keeper ID and keeper email adress. Email can be None.
    :rtype: tuple[str, str | None]
    """
    writeback_config = False
    config = configparser.ConfigParser()
    with open(config_file, 'w+') as fh:
        config.read_file(fh)
        if not config.has_section('Keeper'):
            config.add_section('Keeper')
        try:
            keeper_id = config.get('Keeper', 'ID')
        except configparser.NoOptionError:
            keeper_id = uuid4().hex
            config.set('Keeper', 'ID', keeper_id)
            writeback_config = True
        try:
            if keeper_email != config.get('Keeper', 'email'):
                config.set('Keeper', 'email', keeper_email)
                writeback_config = True
        except configparser.NoOptionError:
            if keeper_email:
                config.set('Keeper', 'email', keeper_email)
                writeback_config = True

        if writeback_config:
            config.write(fh)
    return keeper_id, keeper_email
