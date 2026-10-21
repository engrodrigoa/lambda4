"""The RFB source contract: which files a reference month has and the column layout of each entity.

Every month has the same 37 files (datalabs ADR 0009). The CSVs inside have no header, are ';'-separated and
ISO-8859-1 encoded; the column order below is the Receita's published layout. The per-entity column count is what the
bronze parser uses to detect and repair broken rows (a stray ';' inside a text field shifts every column after it).
"""
from __future__ import annotations

import re

SPLIT_ENTITIES = ("Empresas", "Estabelecimentos", "Socios")          # 10 parts each: Empresas0..Empresas9
SINGLE_FILES = ("Simples", "Cnaes", "Motivos", "Municipios", "Naturezas", "Paises", "Qualificacoes")

EXPECTED_FILES: tuple[str, ...] = tuple(
    [f"{entity}{i}.zip" for entity in SPLIT_ENTITIES for i in range(10)] + [f"{name}.zip" for name in SINGLE_FILES]
)
PROBE_FILE = "Cnaes.zip"   # smallest file of the contract (~22 KB): cheap even if a server ignores Range

_DOMAIN = ("codigo", "descricao")

COLUMNS: dict[str, tuple[str, ...]] = {
    "empresas": ("cnpj_basico", "razao_social", "natureza_juridica", "qualificacao_responsavel", "capital_social",
                 "porte_empresa", "ente_federativo_responsavel"),
    "estabelecimentos": ("cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial", "nome_fantasia",
                         "situacao_cadastral", "data_situacao_cadastral", "motivo_situacao_cadastral",
                         "nome_cidade_exterior", "pais", "data_inicio_atividade", "cnae_fiscal_principal",
                         "cnae_fiscal_secundaria", "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
                         "cep", "uf", "municipio", "ddd_1", "telefone_1", "ddd_2", "telefone_2", "ddd_fax", "fax",
                         "correio_eletronico", "situacao_especial", "data_situacao_especial"),
    "socios": ("cnpj_basico", "identificador_socio", "nome_socio", "cnpj_cpf_socio", "qualificacao_socio",
               "data_entrada_sociedade", "pais", "representante_legal", "nome_representante",
               "qualificacao_representante", "faixa_etaria"),
    "simples": ("cnpj_basico", "opcao_simples", "data_opcao_simples", "data_exclusao_simples", "opcao_mei",
                "data_opcao_mei", "data_exclusao_mei"),
    "cnaes": _DOMAIN,
    "motivos": _DOMAIN,
    "municipios": _DOMAIN,
    "naturezas": _DOMAIN,
    "paises": _DOMAIN,
    "qualificacoes": _DOMAIN,
}


# Fixed code lists from the Receita's layout document (not shipped as files): (dominio, codigo, descricao)
CODES: tuple[tuple[str, str, str], ...] = (
    ("situacao_cadastral", "01", "NULA"), ("situacao_cadastral", "02", "ATIVA"),
    ("situacao_cadastral", "03", "SUSPENSA"), ("situacao_cadastral", "04", "INAPTA"),
    ("situacao_cadastral", "08", "BAIXADA"),
    ("porte_empresa", "00", "NAO INFORMADO"), ("porte_empresa", "01", "MICRO EMPRESA"),
    ("porte_empresa", "03", "EMPRESA DE PEQUENO PORTE"), ("porte_empresa", "05", "DEMAIS"),
    ("identificador_matriz_filial", "1", "MATRIZ"), ("identificador_matriz_filial", "2", "FILIAL"),
    ("identificador_socio", "1", "PESSOA JURIDICA"), ("identificador_socio", "2", "PESSOA FISICA"),
    ("identificador_socio", "3", "ESTRANGEIRO"),
    ("faixa_etaria", "0", "NAO SE APLICA"), ("faixa_etaria", "1", "0 A 12 ANOS"), ("faixa_etaria", "2", "13 A 20 ANOS"),
    ("faixa_etaria", "3", "21 A 30 ANOS"), ("faixa_etaria", "4", "31 A 40 ANOS"), ("faixa_etaria", "5", "41 A 50 ANOS"),
    ("faixa_etaria", "6", "51 A 60 ANOS"), ("faixa_etaria", "7", "61 A 70 ANOS"), ("faixa_etaria", "8", "71 A 80 ANOS"),
    ("faixa_etaria", "9", "MAIS DE 80 ANOS"),
)

DOMAIN_ENTITIES = ("cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes")
ENTITY_GROUPS: dict[str, tuple[str, ...]] = {"domains": DOMAIN_ENTITIES}
ENTITIES = ("empresas", "estabelecimentos", "socios", "simples", *DOMAIN_ENTITIES)


def expand_entities(name: str) -> tuple[str, ...]:
    """'estabelecimentos' -> ('estabelecimentos',); 'domains' -> the 6 small tables; 'all' -> every entity."""
    if name == "all":
        return ENTITIES
    if name in ENTITY_GROUPS:
        return ENTITY_GROUPS[name]
    if name in COLUMNS:
        return (name,)
    raise KeyError(f"unknown RFB entity or group: {name} (valid: {', '.join((*ENTITIES, *ENTITY_GROUPS, 'all'))})")


def files_of(entity: str) -> tuple[str, ...]:
    """Contract ZIPs of one entity, in order: Estabelecimentos0..9.zip, or Cnaes.zip."""
    return tuple(f for f in EXPECTED_FILES if entity_of(f) == entity)


def entity_of(file_name: str) -> str:
    """'Estabelecimentos3.zip' -> 'estabelecimentos'."""
    stem = re.sub(r"\.zip$", "", file_name, flags=re.IGNORECASE)
    entity = re.sub(r"\d+$", "", stem).lower()
    if entity not in COLUMNS:
        raise KeyError(f"file outside the RFB contract: {file_name}")
    return entity


assert len(EXPECTED_FILES) == 37, "the RFB contract has 37 files per month"
assert len(COLUMNS["estabelecimentos"]) == 30
assert set(ENTITIES) == set(COLUMNS)
