import pytest

from ccp.common.iso6346 import LETTER_VALUES, is_valid


def test_letter_values_skip_multiples_of_eleven():
    # ISO 6346 table: A=10, then multiples of 11 (11, 22, 33) are skipped.
    assert {k: LETTER_VALUES[k] for k in "AKLUVZ"} == {
        "A": 10,
        "K": 21,
        "L": 23,
        "U": 32,
        "V": 34,
        "Z": 38,
    }


@pytest.mark.parametrize(
    "cid",
    [
        "CSQU3054383",  # published ISO 6346 example: sum 6185 % 11 = 3
        "CSQU3054300",  # sum 2089 % 11 = 10, so the check digit is 0 (mod 10 rule)
    ],
)
def test_valid_ids(cid):
    assert is_valid(cid)


@pytest.mark.parametrize(
    "cid",
    [
        "CSQU3054384",  # wrong check digit
        "CSQU3054380",  # wrong check digit
        "csqu3054383",  # lowercase is not accepted
        "CSQX3054383",  # category must be U, J or Z
        "CSQU305438",  # too short
        "CSQU30543833",  # too long
        "CSQUA054383",  # letter in the serial
        "",
    ],
)
def test_invalid_ids(cid):
    assert not is_valid(cid)
