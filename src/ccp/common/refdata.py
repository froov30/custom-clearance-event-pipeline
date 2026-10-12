"""Curated reference subsets: UN/LOCODE ports and 6-digit HS codes (TRD §3)."""

import csv
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

PORT_PATTERN = r"^[A-Z]{2}[A-Z2-9]{3}$"
HS_PATTERN = r"^[0-9]{6}$"


@dataclass(frozen=True)
class RefData:
    ports: frozenset[str]
    hs_codes: frozenset[str]

    def unknown_in(self, fields: Mapping[str, object]) -> list[str]:
        unknown = []
        if "port_code" in fields and fields["port_code"] not in self.ports:
            unknown.append(f"port_code={fields['port_code']}")
        if "hs_code" in fields and fields["hs_code"] not in self.hs_codes:
            unknown.append(f"hs_code={fields['hs_code']}")
        return unknown


def _codes(path: Path, pattern: str) -> frozenset[str]:
    rx = re.compile(pattern)
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for line, row in enumerate(rows, start=2):
        if not rx.match(row["code"]):
            raise ValueError(f"{path.name}:{line}: bad code {row['code']!r}")
    return frozenset(row["code"] for row in rows)


def load(ref_dir: str | Path) -> RefData:
    ref_dir = Path(ref_dir)
    return RefData(
        ports=_codes(ref_dir / "ports.csv", PORT_PATTERN),
        hs_codes=_codes(ref_dir / "hs_codes.csv", HS_PATTERN),
    )
