"""Todos os schemas estão expostos aqui, importante para garantir que não
existam schemas duplicados. Não defina nenhum esquema aqui, utilize os outros
arquivos para isso.
"""

from .bulas_medicamentos import schema as schema_bulas_medicamentos
from .clientes import schema_clientes_conta_refined, schema_clientes_conta_standard
from .clusterizacao_lojas import schema as schema_clusterizacao_lojas
from .coluna_eh_medicamento_completo import (
    schema as schema_coluna_eh_medicamento_completo,
)
from .coluna_eh_medicamento_correcoes_manuais import (
    schema as schema_coluna_eh_medicamento_correcoes_manuais,
)
from .ean_alternativo_incompativel import schema as schema_ean_alternativo_incompativel
from .eans_processados_sara import schema as schema_eans_processados_sara
from .eval_puro_alternativos_recomendacoes_detalhes import (
    schema as schema_eval_puro_alternativos_recomendacoes_detalhes,
)
from .eval_puro_alternativos_recomendacoes_historico import (
    schema as schema_eval_puro_alternativos_recomendacoes_historico,
)
from .extract_product_info_nao_medicamentos_regex import (
    schema as schema_extract_product_info_nao_medicamentos_regex,
)
from .extract_product_info_regex import schema as schema_extract_product_info_regex
from .hyperflow import (
    schema_hyperflow_cadastros_base,
    schema_hyperflow_cadastros_por_loja,
    schema_hyperflow_cadastros_sheets,
    schema_hyperflow_nsm,
    schema_hyperflow_regua_alcance,
    schema_hyperflow_regua_opt_out,
)
from .ids_lojas_com_produto import schema as schema_ids_lojas_com_produto
from .ids_produtos import schema as schema_ids_produtos
from .mescla_tags_similaridade import schema as schema_mescla_tags_similaridade
from .principios_ativos_padronizados import (
    schema as schema_principios_ativos_padronizados,
)
from .produtos_loja_refined import schema as schema_produtos_loja_refined
from .produtos_raw import schema as schema_produtos_raw
from .produtos_refined import schema as schema_produtos_refined
from .produtos_standard import schema as schema_produtos_standard
from .refined_gpt_gera_tags import schema as schema_refined_gpt_gera_tags
from .score_aderencia import schema as schema_score_aderencia
from .score_qualidade_produto import schema as schema_score_qualidade_produto
from .sftp_tradefy import schema_sftp_tradefy
from .websearch_marca_fabricante import schema_websearch_marca_fabricante

__all__ = [
    "schema_clientes_conta_refined",
    "schema_bulas_medicamentos",
    "schema_clientes_conta_standard",
    "schema_clusterizacao_lojas",
    "schema_coluna_eh_medicamento_completo",
    "schema_coluna_eh_medicamento_correcoes_manuais",
    "schema_eans_processados_sara",
    "schema_ids_produtos",
    "schema_principios_ativos_padronizados",
    "schema_produtos_loja_refined",
    "schema_produtos_raw",
    "schema_produtos_refined",
    "schema_produtos_standard",
    "schema_refined_gpt_gera_tags",
    "schema_mescla_tags_similaridade",
    "schema_extract_product_info_regex",
    "schema_ids_lojas_com_produto",
    "schema_extract_product_info_nao_medicamentos_regex",
    "schema_sftp_tradefy",
    "schema_websearch_marca_fabricante",
    "schema_ean_alternativo_incompativel",
    "schema_score_aderencia",
    "schema_score_qualidade_produto",
    "schema_eval_puro_alternativos_recomendacoes_detalhes",
    "schema_eval_puro_alternativos_recomendacoes_historico",
    "schema_hyperflow_cadastros_sheets",
    "schema_hyperflow_cadastros_base",
    "schema_hyperflow_cadastros_por_loja",
    "schema_hyperflow_nsm",
    "schema_hyperflow_regua_alcance",
    "schema_hyperflow_regua_opt_out",
]
