import logging
import os
from pathlib import Path
import platform
import toml
from typing import Annotated, Literal

from pathvalidate import FilePathValidator
from pydantic import AfterValidator, BaseModel, EmailStr, ValidationError
from uuid import uuid4

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_config_path():
    system = platform.system()

    if system == "Darwin":  # macOS
        base = os.path.join(os.path.expanduser("~"), "Library", "Application Support", "Keeper")
    elif system == "Windows":
        base = os.path.join(os.environ.get("APPDATA"), "Keeper")
    else:  # Linux and others
        xdg = os.environ.get("XDG_CONFIG_HOME")
        if xdg:
            base = os.path.join(xdg, "Keeper")
        else:
            base = os.path.join(os.path.expanduser("~"), ".config", "Keeper")

    # Ensure directory exists
    if not os.path.isdir(base):
        os.makedirs(base, exist_ok=True)

    return os.path.join(base, "keeper.toml")


def is_valid_filepath_for_platform(file_path: str | Path) -> str | Path:
    """Returns true or false if the path is valid for the current OS

    :param file_path: The file path to validate
    :type file_path: str | Path
    :return: Is the file path valid for this OS?
    :rtype: bool
    """
    system = platform.system()
    if system == "Darwin":
        system = "macOS"
    os_validator = FilePathValidator(platform=system)
    if os_validator.validate(file_path) is None:
        return file_path


def is_optional_valid_filepath_for_platform(file_path: str | Path) -> str | Path:
    if file_path == "":
        return file_path
    else:
        return is_valid_filepath_for_platform(file_path)


ValidPath = Annotated[str | Path, AfterValidator(is_valid_filepath_for_platform)]
OptionalPath = Annotated[str | Path | Literal[""], AfterValidator(is_optional_valid_filepath_for_platform)]


class KeeperModel(BaseModel):
    id: str
    email: EmailStr | Literal[""] = ""


class BackupOptionsModel(BaseModel):
    zip_file: OptionalPath = ""
    log_file: OptionalPath = ""
    log_verbose: bool = False


class ConfigFileModel(BaseModel):
    Keeper: KeeperModel
    BackupOptions: BackupOptionsModel


def generate_keeper_id() -> str:
    return str(uuid4())


def read_config(config_file: str) -> ConfigFileModel | None:
    """Update the keeper config file if needed and return the relavent keep info.

    :param config_file: The config file path to read & write. Will be created if absent.
    :type config_file: str
    :param keeper_email: Optionally update the email address in the config file, defaults to None
    :type keeper_email: str | None, optional
    :return: Tuple of keeper ID and keeper email adress. Email can be None.
    :rtype: tuple[str, str | None]
    """
    logger.debug(f"Reading config file at: {config_file}")
    try:
        with open(file=config_file, mode='r') as fh:
            config = toml.load(fh)
    except FileNotFoundError:
        logger.warning(f"No config file found at {config_file}")
        keeper_id = generate_keeper_id()
        logger.info(f"Generating empty config with new keepe_id: {keeper_id}")
        write_config(config_file=config_file, config_data=empty_config(keeper_id))
        with open(file=config_file, mode='r') as fh:
            config = toml.load(fh)
    try:
        return ConfigFileModel.model_validate(config)
    except ValidationError as e:
        print(e)


def write_config(config_file: str | Path, config_data: ConfigFileModel):
    with open(file=config_file, mode='w') as fh:
        toml.dump(config_data.model_dump(), fh)


def empty_config(keeper_id: str) -> ConfigFileModel:
    return ConfigFileModel(
        Keeper=KeeperModel(id=keeper_id),
        BackupOptions=BackupOptionsModel()
    )
