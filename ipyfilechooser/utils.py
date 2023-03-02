"""Helper functions for ipyfilechooser."""
import fnmatch
import os
import string
import sys
from json import dump, load

from typing import List, Sequence, Iterable, Optional, Union
from .errors import InvalidPathError
from .utils_dbx import DbxMeta


def get_subpaths(path: str) -> List[str]:
    """Walk a path and return a list of subpaths."""
    if os.path.isfile(path):
        path = os.path.dirname(path)

    paths = [path]
    path, tail = os.path.split(path)

    while tail:
        paths.append(path)
        path, tail = os.path.split(path)

    return paths


def has_parent(path: str) -> bool:
    """Check if a path has a parent folder."""
    return os.path.basename(path) != ''


def has_parent_path(path: str, parent_path: Optional[str]) -> bool:
    """Verifies if path falls under parent_path."""
    check = True

    if parent_path:
        check = os.path.commonpath([path, parent_path]) == parent_path

    return check


def strip_parent_path(path: str, parent_path: Optional[str]) -> str:
    """Remove a parent path from a path."""
    stripped_path = path

    if parent_path and path.startswith(parent_path):
        stripped_path = path[len(parent_path):]

    return stripped_path


def match_item(item: str, filter_pattern: Sequence[str]) -> bool:
    """Check if a string matches one or more fnmatch patterns."""
    if not filter_pattern:
        return True

    if isinstance(filter_pattern, str):
        filter_pattern = [filter_pattern]

    idx = 0
    found = False

    while idx < len(filter_pattern) and not found:
        found |= fnmatch.fnmatch(item.lower(), filter_pattern[idx].lower())
        idx += 1

    return found


def get_dir_contents( # pylint: disable=too-many-arguments
        path: str,
        show_hidden: bool = False,
        show_only_dirs: bool = False,
        dir_icon: Optional[str] = None,
        dir_icon_append: bool = False,
        filter_pattern: Optional[Sequence[str]] = None,
        top_path: Optional[str] = None) -> List[str]:
    """Get directory contents."""
    files = []
    dirs = []

    if os.path.isdir(path):
        for item in os.listdir(path):
            append = True
            if item.startswith('.') and not show_hidden:
                append = False
            full_item = os.path.join(path, item)
            if append and os.path.isdir(full_item):
                dirs.append(item)
            elif append and not show_only_dirs:
                if filter_pattern:
                    if match_item(item, filter_pattern):
                        files.append(item)
                else:
                    files.append(item)
        if has_parent(strip_parent_path(path, top_path)):
            dirs.insert(0, os.pardir)

    if dir_icon:
        return prepend_dir_icons(sorted(dirs), dir_icon, dir_icon_append) + sorted(files)

    return sorted(dirs) + sorted(files)


def prepend_dir_icons( \
        dir_list: Iterable[str], \
        dir_icon: str, \
        dir_icon_append: bool=False) -> List[str]:
    """Prepend unicode folder icon to directory names."""
    if dir_icon_append:
        str_ = [dirname + f'{dir_icon}' for dirname in dir_list]
    else:
        str_ = [f'{dir_icon}' + dirname for dirname in dir_list]

    return str_


def get_drive_letters() -> List[str]:
    """Get all drive letters minus the drive used in path."""
    drives: List[str] = []

    if sys.platform == 'win32':
        # Windows has drive letters
        drives = [os.path.realpath(f'{d}:\\') \
                for d in string.ascii_uppercase if os.path.exists(f'{d}:')]

    return drives


def is_valid_filename(filename: str) -> bool:
    """Verifies if a filename does not contain illegal character sequences"""
    valid = True
    valid = valid and os.pardir not in filename
    valid = valid and os.sep not in filename

    if os.altsep:
        valid = valid and os.altsep not in filename

    return valid


def normalize_path(path: str) -> str:
    """Normalize a path string."""
    normalized_path = os.path.realpath(path)

    if not os.path.isdir(normalized_path):
        raise InvalidPathError(path)

    return normalized_path


def read_file(filepath: str, filename: str) -> bytes:
    """ Reads requested file.
    """
    data = None
    error= None

    try:
        with open(os.path.join(filepath, filename), 'rb') as fd: # pylint: disable=invalid-name
            data = fd.read()
    except IOError as ex:
        error = f"Reading file:{filename[:10]} error:{ex}"
    return (error, data)


def read_json(filepath: str, filename: str) -> bytes:
    """ Reads requested file.
    """
    data = None
    error= None

    try:
        with open(os.path.join(filepath, filename), 'rb') as fd: # pylint: disable=invalid-name
            data = load(fd)
    except IOError as ex:
        error = f"Reading file:{filename[:10]} error:{ex}"
    return (error, data)


def save_file(data: object, filename: str, abort_if_exist: bool=True) -> Union[None,str]:
    """ Writes data into specified file.
    """
    if abort_if_exist and os.path.exists(filename):
        return f"File {filename[:50]}"
    with open(filename, "w", encoding="utf-8") as fd: # pylint: disable=invalid-name
        fd.write(data)
    return None


def save_json(data: object, filename: str, abort_if_exist: bool=True) -> Union[None,str]:
    """ Writes data into specified file.
    """
    if abort_if_exist and os.path.exists(filename):
        return f"File {filename[:50]}"
    with open(filename, "w", encoding="utf-8") as fd: # pylint: disable=invalid-name
        dump(data, fd)
    return None


def save_dbx_meta( # pylint: disable=too-many-arguments
        data: dict, \
        filepath: str, \
        fileroot: str, \
        abort_if_exist: bool=True, \
        abort_if_missing: bool=True, \
        split: bool=False) -> Union[None,str]:
    """ Saves dbX metadata.
    """
    if not isinstance(data, dict):
        return f"Invalid dbX metadata - dictionary is required {type(data).__name__}"

    if split:
        error = {}
        for key in DbxMeta.get_dbx_suffixes():
            try:
                error[key] = save_json( \
                        data[key], \
                        os.path.join(filepath, f'{fileroot}_{key}.json'), \
                        abort_if_exist)
                if error[key] is not None:
                    break
            except (KeyError, AttributeError) as ex:
                error[key] = f"Invalid dbX metadata for key:{key}, error:{ex}"
                if abort_if_missing:
                    break
    else:
        error = save_json( \
                data, \
                os.path.join(filepath, f'{fileroot}_all.json'), \
                abort_if_exist)
    return error
