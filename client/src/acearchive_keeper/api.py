"""Ace Archive Keeper tool.

A tool for participants (called "keepers") to host backups of Ace Archive.
"""
import logging
from typing import NoReturn

import requests
from requests.exceptions import HTTPError, JSONDecodeError

from acearchive_keeper.artifact import AceArtifact

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_server_checksum(checksum_api_url: str) -> tuple[int, str] | tuple[None, None]:
    """Get the backup checksum from the ace archive backups api endpoint

    :param checksum_api_url: The URL of the checksum API
    :type checksum_api_url: str
    :return: The checksum version & checksum value, or None & None if unreadable
    :rtype: tuple[int, str] | tuple[None, None]
    """
    try:
        with requests.get(url=checksum_api_url) as response:
            response.raise_for_status()
            json_response = response.json()
            return json_response.get('format_version'), json_response.get('checksum')
    except HTTPError as e:
        logger.critical(f"""Error posting to ace-archive backups api. {e.response.status_code} recived from URL: {e.request.url}.""")
        return None, None


def tell_acearchive_about_this_backup(
        backup_api_url: str,
        keeper_id: str,
        backup_size: int,
        backup_checksum: str,
        keeper_email: str | None) -> NoReturn:
    """Post details of this backups run to the acearchive backups API.

    :param backup_api_url: The URL of the backups API to post to
    :type backup_api_url: str
    :param keeper_id: The keeper ID
    :type keeper_id: str
    :param backup_size: The size of all files in the backed-up archive, including up-to-date files
    :type backup_size: int
    :param keeper_email: An optional email address for the keeper of this backup
    :type keeper_email: str | None
    """
    format_version = 1
    backup_payload = {
        "format_version": format_version,
        "keeper_id": keeper_id,
        "checksum": backup_checksum,
        "size": backup_size,
        "email": keeper_email
    }
    if keeper_email is None:
        backup_payload.pop("email")
    try:
        response = requests.post(backup_api_url, json=backup_payload)
        response.raise_for_status()
    except HTTPError as e:
        logger.critical(f"""Error posting to ace-archive backups api. {e.response.status_code} recived from URL: {e.request.url}.""")


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
        logger.debug(f"Getting next page of artifacts from {artifact_url}")
        response = requests.get(url=artifact_url, params=params)
        try:
            response.raise_for_status()
            json_response = response.json()

            logger.debug(f"Got {len(json_response.get("items"))} artifacts on this page.")
            for artifact in json_response.get("items"):
                try:
                    artifacts.append(AceArtifact(**artifact))
                except TypeError:
                    logger.error(f"""Input validation failed for artifact: {artifact}. API returned something that doesn't look like an ace-archive artifact. Skipping backup of this artifact.""")

            try:
                params.update({"cursor": json_response["next_cursor"]})
                logger.debug(f"Next cursor is: {json_response.get("next_cursor", "")}")
            except KeyError:
                logger.debug("next_cursor not found in response. This is the last page.")
                break

        except HTTPError as e:
            logger.critical(f"""Error getting artifacts from ace-archive api. {e.response.status_code} recived from URL: {e.request.url}. This is likely an issue with the API or your connection to it. Is the source url set correctly?""")
        except JSONDecodeError:
            logger.critical("""Failed to decode json response from API. This is likely an issue with the API or your connection to it. Is the source url set correctly?""")
    return artifacts
