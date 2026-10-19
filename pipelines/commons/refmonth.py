"""Reference month (the RFB publishes one snapshot per month): parsing, ordering and arithmetic."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

_PATTERN = re.compile(r"^\s*(\d{4})[-/]?(\d{2})\s*$")


@dataclass(frozen=True, order=True)
class RefMonth:
    year: int
    month: int

    def __post_init__(self) -> None:
        if not 1 <= self.month <= 12:
            raise ValueError(f"month out of range: {self.month}")
        if not 2000 <= self.year <= 2100:
            raise ValueError(f"year out of range: {self.year}")

    @classmethod
    def parse(cls, value: str | int) -> RefMonth:
        """Accepts 2026-09, 2026/09, 202609 (str or int)."""
        m = _PATTERN.match(str(value))
        if not m:
            raise ValueError(f"reference month must be YYYY-MM or YYYYMM, got {value!r}")
        return cls(int(m.group(1)), int(m.group(2)))

    @classmethod
    def from_date(cls, d: date) -> RefMonth:
        return cls(d.year, d.month)

    @property
    def yyyymm(self) -> int:
        """Integer form used as partition value and natural key (202609)."""
        return self.year * 100 + self.month

    @property
    def iso(self) -> str:
        """Form used by the RFB share folders (2026-09)."""
        return f"{self.year:04d}-{self.month:02d}"

    def shift(self, months: int) -> RefMonth:
        index = self.year * 12 + (self.month - 1) + months
        return RefMonth(index // 12, index % 12 + 1)

    def next(self) -> RefMonth:
        return self.shift(1)

    def prev(self) -> RefMonth:
        return self.shift(-1)

    def __str__(self) -> str:
        return self.iso


def month_range(start: RefMonth, end: RefMonth) -> list[RefMonth]:
    """Inclusive, ordered range: the order SCD2 must apply months in."""
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    out, cur = [], start
    while cur <= end:
        out.append(cur)
        cur = cur.next()
    return out
