"""TextValidator — binary/encoding/size rejection for edit content."""

from __future__ import annotations

from sas2pyspark.core.errors import FsError, FsErrorCode

MAX_EDIT_BYTES = 10 * 1024 * 1024


class TextValidator:
    """Reject binary, invalid UTF-8, and oversized content before editing."""

    __slots__ = ()

    @staticmethod
    def validate(*, content: bytes) -> str:
        if len(content) > MAX_EDIT_BYTES:
            raise FsError(
                FsErrorCode.FS_NOT_TEXT,
                f"content exceeds {MAX_EDIT_BYTES} bytes — edit not supported",
                remedy="use a smaller edit or read/stream the file instead",
            )
        if b"\x00" in content:
            raise FsError(
                FsErrorCode.FS_NOT_TEXT,
                "binary file (NUL byte detected) — edit not supported",
                remedy="use a binary-aware tool or convert to text first",
            )
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise FsError(
                FsErrorCode.FS_NOT_TEXT,
                f"invalid UTF-8: {exc}",
                remedy="fix the encoding or use a binary-aware tool",
            ) from exc
