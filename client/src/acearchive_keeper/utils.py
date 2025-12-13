"""Ace Archive Keeper tool.

A tool for participants (called "keepers") to host backups of Ace Archive.
"""
import aiofiles
from argparse import ArgumentTypeError
from hashlib import sha256
import logging
import sys
from typing import Any, NoReturn
from uuid import uuid4

from email_validator import EmailNotValidError, validate_email
from jcs._jcs import JSONEncoder
from pydantic import HttpUrl, ValidationError


logger = logging.getLogger(__name__)


def generate_keeper_id() -> str:
    return str(uuid4())


async def read_on_disk_hash(filepath: str) -> str:
    """Get the sha256 hash of a file located at a given path.

    :param filepath: The path of the file to calculate the hash for
    :type filepath: str
    :return: The sha256 hash
    :rtype: str
    """
    BUF_SIZE = 1024 * 1024
    filesha = sha256()
    async with aiofiles.open(filepath, "rb") as fh:
        while True:
            byte_batch = await fh.read(BUF_SIZE)

            if not byte_batch:
                break

            filesha.update(byte_batch)
    return filesha.hexdigest()


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


def canonicalize_pretty(object: Any) -> bytes:
    """Canonicalize a json-serializable object, but with pretty printing

    :param object: Any json-serializable object
    :type object: Any
    :return: UTF-8 formatted canonicalized json, made pretty
    :rtype: bytes
    """
    return JSONEncoder(indent=4, sort_keys=True).encode(object).encode()


def valid_email(email: str) -> str:
    if email is None or email == "":
        return None
    try:
        emailinfo = validate_email(email, check_deliverability=False)
        return emailinfo.normalized
    except EmailNotValidError as e:
        raise ArgumentTypeError(e) from e


def valid_url(url: str) -> HttpUrl:
    try:
        validated_url = HttpUrl(url)
        return validated_url
    except ValidationError as e:
        raise ArgumentTypeError(e) from e
