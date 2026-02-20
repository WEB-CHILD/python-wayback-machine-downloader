import os
import shutil
import hashlib
import pylibmagic
import magic


def check_nt():
    """
    Check if the OS is Windows.
    """
    return os.name == "nt"


def sanitize_filename(filename: str) -> str:
    """
    Sanitize a string to be used as (part of) a filename.
    """
    disallowed = ["<", ">", ":", '"', "/", "\\", "|", "?", "*"]
    for char in disallowed:
        filename = filename.replace(char, ".")
    filename = ".".join(filter(None, filename.split(".")))
    return filename


def sanitize_url(url: str) -> str:
    """
    Sanitize a url by encoding special characters.
    """
    special_chars = [":", "*", "?", "&", "=", "<", ">", "\\", "|"]
    for char in special_chars:
        url = url.replace(char, f"%{ord(char):02x}")
    return url


def url_get_timestamp(url):
    """
    Extract the timestamp from a wayback machine URL.
    """
    timestamp = url.split("web/")[1].split("/")[0]
    if "id_" in url:
        timestamp = timestamp.split("id_")[0]
    return timestamp


def url_split(url, index=False):
    """
    Split a URL into domain, subdir, and filename.

    Index:
    - [0] = domain
    - [1] = subdir
    - [2] = filename
    """
    if "://" in url:
        url = url.split("://")[1]
    domain = url.split("/")[0]
    path = url[len(domain) :]
    domain = domain.split("@")[-1].split(":")[0]  # remove mailto and port
    path_parts = path.split("/")
    path_end = path_parts[-1]
    if not url.endswith("/") or "." in path_end:
        filename = path_parts.pop()
    else:
        filename = "index.html" if index else ""
    subdir = "/".join(path_parts).strip("/")
    # sanitize subdir and filename for windows
    if check_nt():
        special_chars = [":", "*", "?", "&", "=", "<", ">", "\\", "|"]
        for char in special_chars:
            subdir = subdir.replace(char, f"%{ord(char):02x}")
            filename = filename.replace(char, f"%{ord(char):02x}")
    filename = filename.replace("%20", " ")
    return domain, subdir, filename


def move_index(existpath: str = None, existfile: str = None, filebuffer: bytes = None):
    """
    1. If existpath is given but can't be created because a file exists with the same name
        - moves the existing file to a temporary name
        - creates the existpath
        - moves the temporary file to the existpath
        - if existing file is text/html, renames it to index.html, else to basename

    2. If existfile is given but can't be created because a folder exists with the same name
        - sets existfile path to existing folder + index.html
        - if the new file is text/html, stores it as index.html, else as basename of target folder
    """
    if existpath:
        shutil.move(existpath, existpath + "_exist")
        os.makedirs(existpath, exist_ok=True)
        if not check_index_mime(existpath):
            new_file = os.path.join(existpath, os.path.basename(os.path.normpath(existpath)))
        else:
            new_file = os.path.join(existpath, "index.html")
        shutil.move(existpath + "_exist", new_file)
    elif existfile:
        if filebuffer:
            if not check_index_mime(filebuffer):
                return os.path.join(existfile, os.path.basename(os.path.normpath(existfile)))
            else:
                return os.path.join(existfile, "index.html")


def check_index_mime(filebuffer: bytes) -> bool:
    mime = magic.Magic(mime=True)
    mime_type = mime.from_buffer(filebuffer)
    if mime_type != "text/html":
        return False
    return True


def truncate_filename(filename: str, max_length: int = 200) -> str:
    """
    Truncate a filename if it exceeds the maximum length.

    Preserves file extension and adds a hash suffix to maintain uniqueness.
    Most filesystems have a 255-byte limit per component; we use 200 as a safe limit.

    Args:
        filename (str): The original filename to truncate.
        max_length (int): Maximum length for the filename (default 200 bytes).

    Returns:
        str: The truncated filename or original if within limits.
    """
    filename_bytes = filename.encode("utf-8")

    # If within limits, return as-is
    if len(filename_bytes) <= max_length:
        return filename

    # Split filename and extension
    if "." in filename:
        name_part, ext = filename.rsplit(".", 1)
        ext = "." + ext
    else:
        name_part = filename
        ext = ""

    # Create a hash of the original filename for uniqueness
    hash_suffix = "_" + hashlib.md5(filename.encode("utf-8")).hexdigest()[:8]

    # Truncate the name part to fit
    truncated_name = name_part
    while len((truncated_name + hash_suffix + ext).encode("utf-8")) > max_length and truncated_name:
        truncated_name = truncated_name[:-1]

    return truncated_name + hash_suffix + ext


def truncate_path_components(path: str, max_length: int = 200) -> str:
    """
    Truncate long path components while preserving directory structure.

    Each directory component is truncated independently if it exceeds max_length.

    Args:
        path (str): The directory path with "/" separators.
        max_length (int): Maximum length for each path component (default 200 bytes).

    Returns:
        str: Path with truncated components.
    """
    if not path:
        return path

    components = path.split("/")
    truncated_components = [truncate_filename(comp, max_length) if comp else comp for comp in components]
    return "/".join(truncated_components)
