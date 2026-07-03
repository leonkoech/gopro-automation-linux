"""
Shared filesystem path-safety helpers.

Centralizes the guard against path-traversal (`..`) and absolute-path-override
attacks so every call site uses one correct, audited implementation.
"""

import os
from typing import Optional


def safe_path_within(base: str, *parts: str) -> Optional[str]:
    """
    Safely join user-supplied path parts onto a trusted base directory.

    Guards against path-traversal (`..`) attacks. Any part that is absolute
    or contains a `..` segment is rejected outright, then the fully resolved
    path is confirmed to live *strictly inside* `base` (symlinks resolved).

    Args:
        base: Trusted base directory the result must stay within.
        *parts: Untrusted path components (e.g. filename, session_name).

    Returns:
        The resolved absolute path if it is safely inside `base`,
        otherwise None.
    """
    for part in parts:
        if part is None:
            return None
        # Reject absolute components and any `..` traversal segment.
        if os.path.isabs(part):
            return None
        if '..' in part.replace('\\', '/').split('/'):
            return None

    base_real = os.path.realpath(base)
    candidate = os.path.realpath(os.path.join(base_real, *parts))

    # Must be strictly within base (not base itself, not a sibling like
    # `<base>_evil` that merely shares the string prefix).
    if candidate.startswith(base_real + os.sep):
        return candidate
    return None
