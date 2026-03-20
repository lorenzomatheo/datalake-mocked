# Databricks notebook source
# MAGIC %md
# MAGIC # Cálculo de Score de Qualidade de Produtos
# MAGIC Este notebook calcula indíces de qualidade para cada produto na tabela `produtos_refined`. O objetivo é quantificar quão completo e correto (compliance) está o cadastro de cada item.
# MAGIC ### Principais Regras de Negócio
# MAGIC 1.  **Pesos por Campo**:
# MAGIC     *   **Peso 2 (Críticos)**: Campos essenciais como `bula`, `tarja`, `principio_ativo` (para medicamentos) e `categorias`, `tags`, `marca` (para não medicamentos)
# MAGIC     *   **Peso 1 (Padrão)**: Demais campos informativos.
# MAGIC 2.  **Fórmula de Pontuação (Granular)**:
# MAGIC     *   **Campos com Regra de Compliance** (ex: Tarja não pode ser inválida):
# MAGIC         *   Preenchido e Válido: **100%** dos pontos.
# MAGIC         *   Preenchido mas Inválido: **50%** dos pontos.
# MAGIC         *   Vazio: **0%**.
# MAGIC     *   **Campos sem Regra**:
# MAGIC         *   Preenchido: **100%** dos pontos.
# MAGIC         *   Vazio: **0%**.
# MAGIC 3.  **Score Final**:
# MAGIC     *   É a soma dos pontos obtidos dividida pela soma total dos pesos possíveis para aquele tipo de produto.
# MAGIC     *   Gera classificação (Tier): **EXCELENTE** (>=80), **BOM** (>=60), **REGULAR** (>=40), **RUIM** (<40).

# COMMAND ----------

from dataclasses import dataclass, field
from typing import List

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import Column

from maggulake.enums import (
    FaixaEtaria,
    FormaFarmaceutica,
    QualityTier,
    SexoRecomendado,
    TipoMedicamento,
    TiposReceita,
    TiposTarja,
    ViaAdministracao,
)
from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.pipelines.quality_score import is_field_compliant, is_field_filled
from maggulake.schemas import schema_score_qualidade_produto
from maggulake.utils.pyspark import to_schema
from maggulake.utils.time import agora_em_sao_paulo_str

# COMMAND ----------

# Configuração do Ambiente
env = DatabricksEnvironmentBuilder.build(
    "calcula_score_qualidade", dbutils, spark_config={"spark.sql.caseSensitive": "true"}
)
spark = env.spark
catalog = env.settings.catalog

# COMMAND ----------

# Definição de Pesos e Campos
PESO_2_MEDICAMENTOS = [
    "categorias",
    "descricao",
    "dosagem",
    "eh_controlado",
    "eh_medicamento",
    "eh_otc",
    "eh_tarjado",
    "fabricante",
    "forma_farmaceutica",
    "idade_recomendada",
    "indicacao",
    "marca",
    "nome",
    "nome_comercial",
    "principio_ativo",
    "sexo_recomendado",
    "tags_agregadas",
    "tags_atenuam_efeitos",
    "tags_complementares",
    "tags_potencializam_uso",
    "tags_substitutos",
    "tarja",
    "tipo_medicamento",
    "via_administracao",
]

PESO_2_NAO_MEDICAMENTOS = [
    "nome_comercial",
    "idade_recomendada",
    "sexo_recomendado",
    "categorias",
    "descricao",
    "eh_medicamento",
    "marca",
    "fabricante",
    "tags_agregadas",
    "tags_atenuam_efeitos",
    "tags_complementares",
    "tags_potencializam_uso",
    "tags_substitutos",
]

CAMPOS_EXCLUIDOS = [
    "id",
    "atualizado_em",
    "informacoes_para_embeddings",
    "imagem_url",
]

# Definição de Compliance
ENUM_MAPPINGS = {
    "tipo_receita": TiposReceita.tuple(),
    "forma_farmaceutica": FormaFarmaceutica.tuple(),
    "via_administracao": ViaAdministracao.tuple(),
    "idade_recomendada": FaixaEtaria.tuple(),
    "sexo_recomendado": SexoRecomendado.tuple(),
    "tarja": TiposTarja.tuple(),
    "tipo_medicamento": TipoMedicamento.tuple(),
}

# COMMAND ----------

# Garantir existência das tabela score
env.create_table_if_not_exists(
    Table.score_qualidade_produto,
    schema_score_qualidade_produto,
)
print(
    f"[{agora_em_sao_paulo_str()}] ✅ Tabela Delta criada/verificada: {Table.score_qualidade_produto.value}"
)

# COMMAND ----------

# Leitura dos dados
df = env.table(Table.produtos_em_processamento).drop('gerado_em')

# COMMAND ----------


@dataclass
class QualityScoreAccumulator:
    # Info de pesos atribuidos
    total_points: Column = field(default_factory=lambda: F.lit(0))
    total_weight: Column = field(default_factory=lambda: F.lit(0))

    # Info Completude
    filled_count: Column = field(default_factory=lambda: F.lit(0))
    completeness_points: Column = field(default_factory=lambda: F.lit(0))

    # Info Compliance
    compliant_count: Column = field(default_factory=lambda: F.lit(0))
    compliance_points: Column = field(default_factory=lambda: F.lit(0))
    compliance_total: Column = field(default_factory=lambda: F.lit(0))

    # Listas com colunas vazias (debug)
    non_compliant_cases: List[Column] = field(default_factory=list)
    missing_critical_cases: List[Column] = field(default_factory=list)


# COMMAND ----------


# Definindo expressões e cálculo score

all_columns = [c for c in df.columns if c not in CAMPOS_EXCLUIDOS]
cols_with_compliance = set(ENUM_MAPPINGS.keys())

acc = QualityScoreAccumulator()

is_medicamento_col = F.col("eh_medicamento") == True

print(f"Iniciando análise de qualidade para {len(all_columns)} colunas...")

for col_name in all_columns:
    # 1. Definição do Peso
    weight_val_med = 2 if col_name in PESO_2_MEDICAMENTOS else 1
    weight_val_nm = 2 if col_name in PESO_2_NAO_MEDICAMENTOS else 1

    print(f"--- Coluna: {col_name} ---")
    print(f"  Peso (Med/NãoMed): {weight_val_med}/{weight_val_nm}")

    # Spark expression
    weight_expr = F.when(is_medicamento_col, F.lit(weight_val_med)).otherwise(
        F.lit(weight_val_nm)
    )

    # 2. Preenchimento
    filled_expr = is_field_filled(df, col_name)  # retorna 1 ou 0

    # Acumula
    acc.filled_count = acc.filled_count + filled_expr
    acc.completeness_points = acc.completeness_points + filled_expr

    # 3. Compliance e Pontos
    if col_name in cols_with_compliance:
        print("Regra: Enum -> 50% Completude + 50% Compliance")

        enum_vals = ENUM_MAPPINGS.get(col_name)

        compliant_expr = is_field_compliant(df, col_name, enum_vals)

        # peso atribuido: 50% compliance e 50% completude
        points_expr = weight_expr * (
            (F.lit(0.5) * filled_expr) + (F.lit(0.5) * compliant_expr)
        )

        acc.compliance_points = acc.compliance_points + compliant_expr
        acc.compliance_total = acc.compliance_total + F.lit(1)
        acc.compliant_count = acc.compliant_count + compliant_expr

        cond_non_compliant = (filled_expr == 1) & (compliant_expr == 0)
        acc.non_compliant_cases.append(
            F.when(cond_non_compliant, F.lit(col_name)).otherwise(F.lit(None))
        )

    else:
        print("Regra: Apenas Completude -> 100% prenchida")
        # Sem regra: 100% completude
        points_expr = weight_expr * filled_expr

    # Acumula totais
    acc.total_points = acc.total_points + points_expr
    acc.total_weight = acc.total_weight + weight_expr

    # Erro: Peso 2 e vazio
    cond_missing_critical = (weight_expr == 2) & (filled_expr == 0)
    acc.missing_critical_cases.append(
        F.when(cond_missing_critical, F.lit(col_name)).otherwise(F.lit(None))
    )

print("Finalizando a análise de qualidade")

# Adiciona colunas para debug
non_compliant_list_col = (
    F.filter(F.array(acc.non_compliant_cases), lambda x: x.isNotNull())
    if acc.non_compliant_cases
    else F.array().cast("array<string>")
)
missing_critical_list_col = (
    F.filter(F.array(acc.missing_critical_cases), lambda x: x.isNotNull())
    if acc.missing_critical_cases
    else F.array().cast("array<string>")
)

# COMMAND ----------

# Aplicação ao DataFrame (Single Pass SELECT)
df_scores = df.select(
    "*",
    acc.total_points.alias("peso_atingido"),
    acc.total_weight.alias("peso_total"),
    # Contagens
    F.lit(len(all_columns)).alias("qtd_total_colunas_avaliadas"),
    acc.filled_count.alias("qtd_colunas_preenchidas"),
    acc.compliant_count.alias("qtd_colunas_em_compliance"),
    # Arrays de Erros
    non_compliant_list_col.alias("colunas_fora_compliance"),
    missing_critical_list_col.alias("colunas_sem_informacao"),
    # Métricas Intermediárias para Scores
    acc.completeness_points.alias("_completeness_points"),
    acc.compliance_points.alias("_compliance_points"),
    acc.compliance_total.alias("_compliance_total"),
)

# Calcula Percentuais e tier do score

df_final = (
    df_scores.withColumn(
        "quality_score",
        F.when(
            F.col("peso_total") > 0,
            F.round((F.col("peso_atingido") / F.col("peso_total")) * 100, 2),
        ).otherwise(0.0),
    )
    .withColumn(
        "completeness_score",
        F.round((F.col("_completeness_points") / F.lit(len(all_columns))) * 100, 2),
    )
    .withColumn(
        "compliance_score",
        F.when(
            F.col("_compliance_total") > 0,
            F.round(
                (F.col("_compliance_points") / F.col("_compliance_total")) * 100, 2
            ),
        ).otherwise(100.0),
    )
    .withColumn(
        "quality_tier",
        F.when(F.col("quality_score") >= 80, QualityTier.EXCELENTE.value)
        .when(F.col("quality_score") >= 60, QualityTier.BOM.value)
        .when(F.col("quality_score") >= 40, QualityTier.REGULAR.value)
        .otherwise(QualityTier.RUIM.value),
    )
    .withColumn('gerado_em', F.current_timestamp())
    .drop("_completeness_points", "_compliance_points", "_compliance_total")
)

# COMMAND ----------

# Garante schema
df_salvar = to_schema(df_final, schema_score_qualidade_produto)

# COMMAND ----------

# Salvando resultados (Histórico)
# TODO: Criar uma mecânica para limpar os dados antigos

df_salvar.write.mode("append").saveAsTable(Table.score_qualidade_produto.value)
print(
    f"Scores salvos na tabela analytics (histórico): {Table.score_qualidade_produto.value}"
)

# Salvando resultados mais recentes
# Importante pq a tabela com o histórico tende a crescer muito e em algum momento vai ficar pesado

# Garante que a tabela latest existe
env.create_table_if_not_exists(
    Table.score_qualidade_produto_latest,
    schema_score_qualidade_produto,
)

print(f"Atualizando tabela latest: {Table.score_qualidade_produto_latest.value}")
delta_table = DeltaTable.forName(spark, Table.score_qualidade_produto_latest.value)

(
    delta_table.alias("target")
    .merge(df_salvar.alias("source"), "target.ean = source.ean")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
print("Merge concluído com sucesso na tabela latest.")
