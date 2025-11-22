from datetime import datetime, timedelta, timezone
from time import localtime

import jcs


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
