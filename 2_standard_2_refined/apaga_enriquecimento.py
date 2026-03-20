# Databricks notebook source
import ast

from maggulake.environment import DatabricksEnvironmentBuilder, Table

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "apaga_enriquecimento",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={"eans": ""},
)

spark = env.spark

# ambiente
s3_bucket_name = env.settings.bucket

# pastas s3
s3_extra_info_folder = env.full_s3_path("3-refined-layer/gpt_extract_product_info")
s3_respostas_folder = env.full_s3_path("3-refined-layer/gpt_descricoes_curtas_produtos")
s3_indicacoes_categorias_folder = env.full_s3_path(
    "3-refined-layer/indicacoes_categorias_gpt"
)
s3_tags_folder = env.full_s3_path("3-refined-layer/gpt_tags_produtos")

eans_input = dbutils.widgets.get("eans")


def parse_eans(input_str):
    try:
        # Se for uma lista
        eans = ast.literal_eval(input_str)
        if isinstance(eans, str):
            return [eans]
        return list(eans)
    except (ValueError, SyntaxError):
        # Se for vários eans separados por vírgula ou um ean único
        if "," in input_str:
            return [ean.strip() for ean in input_str.split(",")]
        return [input_str.strip()]


eans = parse_eans(eans_input)

# COMMAND ----------


def processa_dataframe(df, s3_path, tipo_enriquecimento):
    eans_quoted = [f"'{ean}'" for ean in eans]
    eans_str = ",".join(eans_quoted)

    filtered_df = df.filter(f"ean IN ({eans_str})")

    if not filtered_df.isEmpty():
        df.filter(f"ean NOT IN ({eans_str})").write.mode("overwrite").parquet(s3_path)
        print(f"{tipo_enriquecimento} salvo com sucesso para todos os EANs")
    else:
        print(f"Nenhum {tipo_enriquecimento} encontrado para nenhum dos EANs")


# COMMAND ----------


dfs = {
    "Extra info": (spark.read.parquet(s3_extra_info_folder), s3_extra_info_folder),
    "Respostas": (env.table(Table.gpt_descricoes_curtas_produtos), s3_respostas_folder),
    "Tags": (spark.read.parquet(s3_tags_folder), s3_tags_folder),
    "Indicacoes": (
        spark.read.parquet(s3_indicacoes_categorias_folder),
        s3_indicacoes_categorias_folder,
    ),
}

for enriquecimento, (df, path) in dfs.items():
    print(f"\nProcessando {enriquecimento}")
    print("-" * 50)
    processa_dataframe(df, path, enriquecimento)
    print("-" * 50)
