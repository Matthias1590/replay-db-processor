import re
import struct


FILE_MAGIC = 0x43F4EFDD
NETWORK_MAGIC = 0x2CF5A13D
MAX_CUSTOM_VERSIONS = 1024
MAX_FSTRING_BYTES = 1024 * 1024
MAX_ENCRYPTION_KEY_BYTES = 1024 * 1024


class ReplayHeaderError(ValueError):
    pass


def _read_exact(stream, size, field):
    value = stream.read(size)
    if len(value) != size:
        raise ReplayHeaderError(f"Truncated replay while reading {field}")
    return value


def _unpack(stream, fmt, field):
    return struct.unpack(fmt, _read_exact(stream, struct.calcsize(fmt), field))[0]


def _skip(stream, size, field):
    if size < 0:
        raise ReplayHeaderError(f"Invalid {field} length: {size}")
    _read_exact(stream, size, field)


def _read_fstring(stream, field):
    length = _unpack(stream, "<i", f"{field} length")
    if length == 0:
        return ""
    if length > 0:
        if length > MAX_FSTRING_BYTES:
            raise ReplayHeaderError(f"{field} is too large")
        raw = _read_exact(stream, length, field)
        if raw[-1:] != b"\0":
            raise ReplayHeaderError(f"{field} is not null-terminated")
        try:
            return raw[:-1].decode("utf-8")
        except UnicodeDecodeError as error:
            raise ReplayHeaderError(f"{field} is not valid UTF-8") from error

    code_units = -length
    byte_length = code_units * 2
    if byte_length > MAX_FSTRING_BYTES:
        raise ReplayHeaderError(f"{field} is too large")
    raw = _read_exact(stream, byte_length, field)
    if raw[-2:] != b"\0\0":
        raise ReplayHeaderError(f"{field} is not null-terminated")
    try:
        return raw[:-2].decode("utf-16-le")
    except UnicodeDecodeError as error:
        raise ReplayHeaderError(f"{field} is not valid UTF-16") from error


def normalize_game_version(value):
    match = re.search(r"(\d+\.\d+(?:\.\d+)?)$", value.strip())
    if match is None:
        raise ReplayHeaderError(f"Replay branch has no game version: {value!r}")
    return match.group(1)


def read_replay_game_version(path):
    with open(path, "rb") as stream:
        magic = _unpack(stream, "<I", "file magic")
        if magic != FILE_MAGIC:
            raise ReplayHeaderError(f"Invalid replay file magic: 0x{magic:08x}")

        _skip(stream, 4, "file version")
        custom_version_count = _unpack(stream, "<i", "custom version count")
        if not 0 <= custom_version_count <= MAX_CUSTOM_VERSIONS:
            raise ReplayHeaderError(f"Invalid custom version count: {custom_version_count}")
        _skip(stream, custom_version_count * 20, "custom versions")

        _skip(stream, 12, "replay summary")
        _read_fstring(stream, "friendly name")
        _skip(stream, 20, "replay flags and timestamp")
        encryption_key_length = _unpack(stream, "<i", "encryption key length")
        if not 0 <= encryption_key_length <= MAX_ENCRYPTION_KEY_BYTES:
            raise ReplayHeaderError(f"Invalid encryption key length: {encryption_key_length}")
        _skip(stream, encryption_key_length, "encryption key")

        chunk_type = _unpack(stream, "<I", "first chunk type")
        if chunk_type != 0:
            raise ReplayHeaderError(f"First replay chunk is not a header: {chunk_type}")
        header_size = _unpack(stream, "<i", "header size")
        if header_size < 0:
            raise ReplayHeaderError(f"Invalid header size: {header_size}")

        header_start = stream.tell()
        network_magic = _unpack(stream, "<I", "network magic")
        if network_magic != NETWORK_MAGIC:
            raise ReplayHeaderError(f"Invalid replay network magic: 0x{network_magic:08x}")
        _skip(stream, 4, "network version")
        header_custom_version_count = _unpack(stream, "<i", "header custom version count")
        if not 0 <= header_custom_version_count <= MAX_CUSTOM_VERSIONS:
            raise ReplayHeaderError(
                f"Invalid header custom version count: {header_custom_version_count}"
            )
        _skip(stream, header_custom_version_count * 20, "header custom versions")
        _skip(stream, 38, "network metadata and replay version numbers")
        branch = _read_fstring(stream, "replay branch")

        if stream.tell() - header_start > header_size:
            raise ReplayHeaderError("Replay branch extends beyond the header chunk")
        return normalize_game_version(branch)
