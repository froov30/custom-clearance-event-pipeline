"""ISO 6346 container ID validation (TRD §4)."""

import re
import string

_FORMAT = re.compile(r"[A-Z]{3}[UJZ][0-9]{7}")


def _letter_values() -> dict[str, int]:
    values, v = {}, 10
    for letter in string.ascii_uppercase:
        if v % 11 == 0:  # the standard skips multiples of 11
            v += 1
        values[letter] = v
        v += 1
    return values


LETTER_VALUES = _letter_values()


def check_digit(first_ten: str) -> int:
    total = sum(
        (LETTER_VALUES[c] if c.isalpha() else int(c)) * 2**i for i, c in enumerate(first_ten)
    )
    return total % 11 % 10


def is_valid(container_id: str) -> bool:
    return bool(_FORMAT.fullmatch(container_id)) and check_digit(container_id[:10]) == int(
        container_id[10]
    )
