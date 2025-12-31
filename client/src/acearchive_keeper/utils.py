"""Ace Archive Keeper tool.

A tool for participants (called "keepers") to host backups of Ace Archive.
"""
from argparse import ArgumentTypeError
import certifi
from hashlib import sha256
import logging
from logging.handlers import QueueHandler
import os
from queue import Queue
import sys
from typing import Any, NoReturn

import aiofiles
from email_validator import EmailNotValidError, validate_email
from jcs._jcs import JSONEncoder
from pydantic import HttpUrl, ValidationError


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def load_frozen_certs():
    if getattr(sys, "frozen", False):
        os.environ["SSL_CERT_FILE"] = os.path.join(sys._MEIPASS, "certifi")
    else:
        os.environ["SSL_CERT_FILE"] = certifi.where()


def get_resource_path(rel_path):
    """Get path to a resource inside a frozen pyinstaller app.

    :param rel_path: Relative path
    :type rel_path: str
    :return: Absolute path
    :rtype: str
    """
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, rel_path)
    else:
        return os.path.join(os.path.abspath("."), rel_path)


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
    utcz_formatter = logging.Formatter("%(asctime)s,%(msecs)03dZ %(name)-14s %(levelname)-8s %(message)s",
                                       datefmt="%Y-%m-%dT%H:%M:%S")

    if log_file:
        logger.addHandler( logging.FileHandler(log_file, 'a'))

    if not surpress_stderr:
        logger.addHandler(logging.StreamHandler(stream=sys.stderr))

    if log_verbosity:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logger.setLevel(log_level)

    for handler in logger.handlers:
        handler.setFormatter(utcz_formatter)
        handler.setLevel(log_level)


def setup_gui_logger(logger: logging.Logger, queue: Queue, level = logging.INFO) -> logging.Logger:
    logger.setLevel(level)
    gui_formatter = logging.Formatter(fmt="%(asctime)s %(message)s", datefmt="%x %X")
    queue_handler = QueueHandler(queue)
    queue_handler.setFormatter(gui_formatter)
    queue_handler.setLevel(level)
    logger.addHandler(queue_handler)
    return logger


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
