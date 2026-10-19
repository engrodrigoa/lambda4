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


def entity_of(file_name: str) -> str:
    """'Estabelecimentos3.zip' -> 'estabelecimentos'."""
    stem = re.sub(r"\.zip$", "", file_name, flags=re.IGNORECASE)
    entity = re.sub(r"\d+$", "", stem).lower()
    if entity not in COLUMNS:
        raise KeyError(f"file outside the RFB contract: {file_name}")
    return entity


assert len(EXPECTED_FILES) == 37, "the RFB contract has 37 files per month"
assert len(COLUMNS["estabelecimentos"]) == 30
