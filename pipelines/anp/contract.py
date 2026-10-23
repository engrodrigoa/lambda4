"""ANP fuel price survey ("Série Histórica de Preços de Combustíveis e de GLP"): what the source publishes.

One page lists every file. Three kinds, all with the same 16-column layout (';' separated, UTF-8, quoted fields):

| kind      | URL pattern (under .../arquivos/shpc/)          | period       | product groups                   |
|-----------|-------------------------------------------------|--------------|----------------------------------|
| semestral | dsas/{ca,glp}/<name>YYYY-0S.{csv,zip}           | semester     | ca (automotive), glp             |
| mensal    | dsan/YYYY/<irregular name with the product>.csv | month        | diesel-gnv, gasolina-etanol, glp |
| semanal   | qus/ultimas-4-semanas-<product>.csv             | last 4 weeks | diesel-gnv, gasolina-etanol, glp |

Facts checked against the live page (2026-10-22), each one a rule below:
* Monthly file names are irregular (`precos-glp-12.csv`, `09-dados-abertos-precos-2026-09-glp.csv`, even a typo
  `02-cados-abertos-preco-...`): the product comes from a token of the name, the month from the link text
  ("Setembro de 2026"), cross-checked with the year folder.
* Some `.csv` links are ZIPs (`ca-2014-01.csv`): the format is decided by magic bytes when downloading.
* The weekly files keep the same URL every week: their identity is the page's "atualizado em" date.
* `precos-semestrais-ca.zip` bundles semesters that are also published one by one: ignored on purpose.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date, timedelta

PIPELINE = "anp"
PAGE_URL = ("https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
            "serie-historica-de-precos-de-combustiveis")

SOURCE_KINDS = ("semestral", "mensal", "semanal")          # also the load order (oldest, most consolidated first)
PRODUCT_GROUPS = ("ca", "glp", "diesel-gnv", "gasolina-etanol")

# the 16 source columns, normalised (see normalize_header); bronze keeps them in this order, all text
COLUMNS = (
    "regiao_sigla", "estado_sigla", "municipio", "revenda", "cnpj_da_revenda", "nome_da_rua", "numero_rua",
    "complemento", "bairro", "cep", "produto", "data_da_coleta", "valor_de_venda", "valor_de_compra",
    "unidade_de_medida", "bandeira",
)

MONTHS = {"janeiro": 1, "fevereiro": 2, "marco": 3, "abril": 4, "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
          "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12}

DATA_LINK = re.compile(r"/shpc/.+\.(csv|zip)$", re.IGNORECASE)
IGNORED = (re.compile(r"/precos-semestrais-ca\.zip$", re.IGNORECASE),)
# the product group comes from the folder: GLP names vary (glp-2025-02, precos-semestrais-glp-2022-01, ...glp2021-01)
_SEMESTRAL = re.compile(r"/shpc/dsas/(ca|glp)/[a-z-]*?(\d{4})-0?([12])\.(?:csv|zip)$", re.IGNORECASE)
_MENSAL = re.compile(r"/shpc/dsan/(\d{4})/([^/]+)\.csv$", re.IGNORECASE)
_SEMANAL = re.compile(r"/shpc/qus/ultimas-4-semanas-(diesel-gnv|gasolina-etanol|glp)\.csv$", re.IGNORECASE)
_MONTH_TEXT = re.compile(r"([a-zç]+)\s+de\s+(\d{4})", re.IGNORECASE)
WEEKLY_WINDOW_DAYS = 28


@dataclass(frozen=True)
class SourceFile:
    file_key: str
    source_kind: str
    product_group: str
    period_start: date
    period_end: date
    url: str
    link_text: str

    @property
    def label(self) -> str:
        """Last part of the key: 2014-S1, 2026-09, 2026-10-20 (used in landing file names)."""
        return self.file_key.rsplit("/", 1)[1]


def strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))


def normalize_header(name: str) -> str:
    """'﻿Regiao - Sigla' -> regiao_sigla, 'CNPJ da Revenda' -> cnpj_da_revenda (accents and BOM removed)."""
    clean = strip_accents(name.replace("﻿", "")).strip().lower()
    clean = re.sub(r"\s*-\s*", "_", clean)
    return re.sub(r"[^a-z0-9]+", "_", clean).strip("_")


def _month_end(year: int, month: int) -> date:
    return (date(year + (month == 12), month % 12 + 1, 1)) - timedelta(days=1)


def _product_of(name: str) -> str | None:
    name = name.lower()
    for group in ("diesel-gnv", "gasolina-etanol", "glp"):
        if group in name:
            return group
    return None


def _month_of(link_text: str, name: str, folder_year: int) -> int | None:
    """Month from the link text ("Setembro de 2026", year must match the folder); else from the file name."""
    m = _MONTH_TEXT.search(strip_accents(link_text))
    if m and m.group(1).lower() in MONTHS:
        return MONTHS[m.group(1).lower()] if int(m.group(2)) == folder_year else None
    # name fallback: a two-digit token 01..12 that is not part of the year
    for token in re.findall(r"(?<!\d)(\d{2})(?!\d)", name):
        if 1 <= int(token) <= 12:
            return int(token)
    return None


def classify(url: str, link_text: str = "", weekly_date: date | None = None) -> SourceFile | None:
    """Map one data link of the page to a SourceFile, or None when the contract does not know it."""
    text = " ".join(link_text.split())
    if m := _SEMESTRAL.search(url):
        group, year, sem = m.group(1).lower(), int(m.group(2)), int(m.group(3))
        start = date(year, 1 if sem == 1 else 7, 1)
        end = date(year, 6, 30) if sem == 1 else date(year, 12, 31)
        return SourceFile(f"semestral/{group}/{year}-S{sem}", "semestral", group, start, end, url, text)
    if m := _MENSAL.search(url):
        year, name = int(m.group(1)), m.group(2)
        group, month = _product_of(name), _month_of(text, name, year)
        if group is None or month is None:
            return None
        return SourceFile(f"mensal/{group}/{year}-{month:02d}", "mensal", group, date(year, month, 1),
                          _month_end(year, month), url, text)
    if (m := _SEMANAL.search(url)) and weekly_date is not None:
        group = m.group(1).lower()
        return SourceFile(f"semanal/{group}/{weekly_date.isoformat()}", "semanal", group,
                          weekly_date - timedelta(days=WEEKLY_WINDOW_DAYS - 1), weekly_date, url, text)
    return None


def is_ignored(url: str) -> bool:
    return any(p.search(url) for p in IGNORED)
