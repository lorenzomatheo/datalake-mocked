# Databricks notebook source
# MAGIC %md
# MAGIC # Agrega diferentes enriquecimentos de dados na tabela de produtos

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.produtos.dosagem import extrai_dosagem_do_nome_udf, normaliza_dosagem_udf
from maggulake.schemas import match_medicamentos as schemas_match_medicamentos
from maggulake.schemas import (
    match_nao_medicamentos as schemas_match_nao_medicamentos,
)
from maggulake.schemas import schema_coluna_eh_medicamento_completo

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "agrega_enriquecimentos",
    dbutils,
    widgets={"clear_cache": ["false", "true", "false"]},
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurações

# COMMAND ----------

# ambiente e cliente
stage = env.settings.name_short
catalog = env.settings.catalog
s3_bucket_name = env.settings.bucket
DEBUG = dbutils.widgets.get("debug") == "true"

spark = env.spark

guia_farmacia_folder = env.full_s3_path("1-raw-layer/maggu/guiadafarmacia")

# COMMAND ----------

clear_cache = dbutils.widgets.get("clear_cache")

if clear_cache == "true":
    spark.catalog.clearCache()
    print("Cache limpo")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Carrega tabela consolidada de eh_medicamento

# COMMAND ----------

env.create_table_if_not_exists(
    Table.coluna_eh_medicamento_completo,
    schema_coluna_eh_medicamento_completo,
)

# COMMAND ----------

# Lê a tabela consolidada de eh_medicamento (prioriza múltiplas fontes)
eh_medicamento_completo = env.table(Table.coluna_eh_medicamento_completo).select(
    "ean",
    F.col("eh_medicamento").alias("eh_medicamento_consolidado"),
)

# COMMAND ----------

if DEBUG:
    print(
        f"✅ Tabela consolidada eh_medicamento carregada: {eh_medicamento_completo.count()} registros"
    )
    eh_medicamento_completo.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adiciona informações vindas do enriquecimento com LLM

# COMMAND ----------

produtos = env.table(Table.produtos_standard).cache()
extra_info = env.table(Table.extract_product_info).drop("id").cache()

# Enriquecimentos específicos por macro categoria (não-medicamentos)
# Cada categoria tem campos específicos, então usamos unionByName com allowMissingColumns
extra_info_perfumaria = env.table(Table.extract_product_info_perfumaria).cache()
extra_info_suplementos = env.table(Table.extract_product_info_suplementos).cache()
extra_info_materiais_saude = env.table(
    Table.extract_product_info_materiais_saude
).cache()
extra_info_produtos_casa = env.table(Table.extract_product_info_produtos_casa).cache()
extra_info_produtos_animais = env.table(
    Table.extract_product_info_produtos_animais
).cache()

# Consolida enriquecimentos de não-medicamentos (cada EAN está em apenas UMA categoria)
extra_info_nao_med_consolidado = (
    extra_info_perfumaria.unionByName(extra_info_suplementos, allowMissingColumns=True)
    .unionByName(extra_info_materiais_saude, allowMissingColumns=True)
    .unionByName(extra_info_produtos_casa, allowMissingColumns=True)
    .unionByName(extra_info_produtos_animais, allowMissingColumns=True)
).cache()

# COMMAND ----------

if DEBUG:
    extra_info.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adiciona info de eans alternativos

# COMMAND ----------

eans_alternativos = env.table(Table.refined_eans_alternativos).cache()

# COMMAND ----------

# Materializa para que o cache funcione
print(f"✅ Tabela produtos standard carregada: {produtos.count()}")
print(f"✅ Tabela Enriquecimento LLM (medicamentos) carregada: {extra_info.count()}")
print(
    f"✅ Tabela Enriquecimento LLM (não medicamentos consolidado) carregada: {extra_info_nao_med_consolidado.count()}"
)
print(
    f"✅ EANs alternativos carregados: {eans_alternativos.count()} produtos com mapeamento"
)

# COMMAND ----------

# Faz join com a coluna consolidada de eh_medicamento
produtos = produtos.join(eh_medicamento_completo, on=["ean"], how="left")

# Usa eh_medicamento consolidado quando disponível
produtos = produtos.withColumn(
    "eh_medicamento",
    F.coalesce(F.col("eh_medicamento_consolidado"), F.col("eh_medicamento")),
).drop("eh_medicamento_consolidado")

print("✅ Coluna eh_medicamento consolidada aplicada aos produtos")

# COMMAND ----------

if DEBUG:
    produtos.filter(F.col("eh_medicamento").isNull()).limit(100).display()

# COMMAND ----------

if DEBUG:
    _total_med = produtos.filter(F.col("eh_medicamento") == True).count()
    _com_dosagem_antes = produtos.filter(
        (F.col("eh_medicamento") == True)
        & F.col("dosagem").isNotNull()
        & (F.col("dosagem") != "")
    ).count()
    print(
        f"[dosagem] ANTES — preenchido: {_com_dosagem_antes}/{_total_med} "
        f"({100 * _com_dosagem_antes / _total_med:.1f}%) | "
        f"faltando: {_total_med - _com_dosagem_antes} ({100 * (1 - _com_dosagem_antes / _total_med):.1f}%)"
    )

produtos_enriquecidos = (
    produtos.alias("p")
    .join(extra_info.alias("e"), on=["ean"], how="left")
    .join(extra_info_nao_med_consolidado.alias("nm"), on=["ean"], how="left")
    .join(eans_alternativos.alias("ea"), on=["ean"], how="left")
    .withColumn(
        "atualizado_em_novo",
        F.coalesce(
            F.greatest(
                "p.atualizado_em", "e.atualizado_em", "nm.atualizado_em"
            ),  # NM tambem tem timestamp
            F.current_timestamp(),
        ),
    )
    # Prioriza dosagem de fontes determinísticas (ANVISA, raw) sobre extração por LLM.
    # Fallback final: extrai do nome do produto via regex.
    # Normaliza formato ao final (remove espaços número-unidade, padroniza separador).
    .withColumn(
        "dosagem_novo",
        normaliza_dosagem_udf(
            F.coalesce(
                F.nullif(F.col("p.dosagem"), F.lit("")),
                F.nullif(F.col("e.dosagem"), F.lit("")),
                F.nullif(F.col("nm.dosagem"), F.lit("")),
                extrai_dosagem_do_nome_udf(F.col("p.nome")),
            )
        ),
    )
    .drop("dosagem")
    .withColumnRenamed("dosagem_novo", "dosagem")
    .withColumn(
        "eh_controlado_novo",
        F.coalesce("p.eh_controlado", "e.eh_controlado").cast("boolean"),
    )
    .drop("eh_controlado")
    .withColumnRenamed("eh_controlado_novo", "eh_controlado")
    # Prioriza principio_ativo de fontes determinísticas (ANVISA, genericos, raw)
    # sobre extração por LLM (e) ou match por similaridade (nm)
    .withColumn(
        "principio_ativo_novo",
        F.initcap(
            F.coalesce("p.principio_ativo", "e.principio_ativo", "nm.principio_ativo")
        ),
    )
    .drop("principio_ativo")
    .withColumnRenamed("principio_ativo_novo", "principio_ativo")
    .drop("product_info", "atualizado_em")
    .withColumnRenamed("atualizado_em_novo", "atualizado_em")
    .withColumn(
        "categorias_novo",
        F.when(
            F.col("eh_medicamento") == True,
            F.coalesce(F.col("e.categorias"), F.col("p.categorias")),
        ).otherwise(F.col("p.categorias")),
    )
    .drop("categorias")
    .withColumnRenamed("categorias_novo", "categorias")
    .withColumn(
        "categorias", F.transform(F.col("categorias"), lambda x: F.initcap(x))
    )  # Garante que todas as categorias estão em Title Case
    .withColumn(
        "eh_otc_novo",
        F.when(
            F.col("eh_medicamento") == True,
            F.coalesce(F.col("p.eh_otc"), F.col("e.eh_otc")),
        ).otherwise(F.col('p.eh_otc')),
    )
    .drop("eh_otc")
    .withColumnRenamed("eh_otc_novo", "eh_otc")
    .withColumn(
        "eans_alternativos_novo",
        F.coalesce("ea.eans_alternativos", "p.eans_alternativos"),
    )
    .drop("eans_alternativos")
    .withColumnRenamed("eans_alternativos_novo", "eans_alternativos")
    .withColumn("status_novo", F.coalesce("ea.status", "p.status"))
    .drop("status")
    .withColumnRenamed("status_novo", "status")
    .withColumn(
        "via_administracao_novo",
        F.coalesce("p.via_administracao", "e.via_administracao"),
    )
    .drop("via_administracao")
    .withColumnRenamed("via_administracao_novo", "via_administracao")
    .withColumn(
        "idade_recomendada_novo",
        F.coalesce(
            "p.idade_recomendada", "e.idade_recomendada", "nm.idade_recomendada"
        ),
    )
    .drop("idade_recomendada")
    .withColumnRenamed("idade_recomendada_novo", "idade_recomendada")
    # Novos campos de enriquecimento
    .withColumn("indicacao_nova", F.coalesce("p.indicacao", "nm.indicacao"))
    .drop('indicacao')
    .withColumnRenamed("indicacao_nova", "indicacao")
    .withColumn(
        "instrucoes_de_uso_nova",
        F.coalesce("e.instrucoes_de_uso", "nm.instrucoes_de_uso"),
    )
    .drop("instrucoes_de_uso")
    .withColumnRenamed("instrucoes_de_uso_nova", "instrucoes_de_uso")
    .withColumn(
        "contraindicacoes_nova",
        F.coalesce("e.contraindicacoes", "nm.contraindicacoes"),
    )
    .drop("contraindicacoes")
    .withColumnRenamed("contraindicacoes_nova", "contraindicacoes")
    .withColumn(
        "advertencias_e_precaucoes_nova",
        F.coalesce(
            "e.advertencias_e_precaucoes",
            "nm.advertencias_e_precaucoes",
        ),
    )
    .drop("advertencias_e_precaucoes")
    .withColumnRenamed("advertencias_e_precaucoes_nova", "advertencias_e_precaucoes")
    .withColumn(
        "condicoes_de_armazenamento_novo",
        F.coalesce(
            "e.condicoes_de_armazenamento",
            "nm.condicoes_de_armazenamento",
        ),
    )
    .drop("condicoes_de_armazenamento")
    .withColumnRenamed("condicoes_de_armazenamento_novo", "condicoes_de_armazenamento")
    .withColumn(
        "validade_apos_abertura_nova",
        F.coalesce(
            "e.validade_apos_abertura",
            "nm.validade_apos_abertura",
        ),
    )
    .drop("validade_apos_abertura")
    .withColumnRenamed("validade_apos_abertura_nova", "validade_apos_abertura")
    .withColumn(
        "volume_quantidade_novo",
        F.coalesce(
            "e.volume_quantidade",
            "nm.volume_quantidade",
        ),
    )
    .drop("volume_quantidade")
    .withColumnRenamed("volume_quantidade_novo", "volume_quantidade")
    .withColumn(
        "sexo_recomendado_novo",
        F.coalesce("e.sexo_recomendado", "nm.sexo_recomendado"),
    )
    .drop("sexo_recomendado")
    .withColumnRenamed("sexo_recomendado_novo", "sexo_recomendado")
    .withColumn(
        "nome_comercial_novo", F.coalesce('e.nome_comercial', 'nm.nome_comercial')
    )
    .drop('nome_comercial')
    .withColumnRenamed("nome_comercial_novo", "nome_comercial")
)

# TODO: essa parte de initcap das categorias poderia ser feita direto no notebook de categorias
produtos_enriquecidos = produtos_enriquecidos.dropDuplicates(["ean"])

# COMMAND ----------

portal_obm = (
    env.table(Table.produtos_portal_obm)
    .filter(F.col("encontrou") == True)
    .select("ean", F.col("nome").alias("nome_comercial_obm"))
)

produtos_enriquecidos = (
    produtos_enriquecidos.join(portal_obm, "ean", "left")
    .withColumn(
        "nome_comercial",
        F.coalesce(F.col("nome_comercial"), F.col("nome_comercial_obm")),
    )
    .drop("nome_comercial_obm")
)

# COMMAND ----------

if DEBUG:
    produtos_enriquecidos.limit(100).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Adiciona categorias geradas pelo notebook de categorização em cascata

# COMMAND ----------

# Lê categorias consolidadas geradas pelo notebook agrega_categorias.py
# Esta tabela já contém:
# - Apenas o enriquecimento mais recente por EAN (uma linha por EAN)
# - Categorias no formato array de strings flat (Super -> Meso -> Micro)
# - Campo eh_medicamento atualizado baseado nas categorias
categorias_cascata_agg = env.table(Table.categorias_cascata_agregado)

# Renomeia coluna para ficar fácil de identificar depois
categorias_cascata_agg = categorias_cascata_agg.select(
    "ean", F.col("categorias").alias("categorias_cascata")
)


# COMMAND ----------

if DEBUG:
    print(
        f"✅ Categorias cascata carregadas: {categorias_cascata_agg.count()} produtos com categorias"
    )
    categorias_cascata_agg.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adiciona enriquecimentos de match medicamentos e não-medicamentos

# COMMAND ----------

env.create_table_if_not_exists(
    Table.match_medicamentos,
    schemas_match_medicamentos.schema,
)


# COMMAND ----------

# Lê tabela de match medicamentos (pega o match mais recente por ean)
match_medicamentos = env.table(Table.match_medicamentos)

# Seleciona apenas o match mais recente por ean
window_spec_match = Window.partitionBy("ean").orderBy(F.desc("match_medicamentos_em"))
match_medicamentos_recente = (
    match_medicamentos.withColumn("row_num", F.row_number().over(window_spec_match))
    .filter(F.col("row_num") == 1)
    .drop(
        "row_num", "nome", "atualizado_em"
    )  # Remove colunas que já existem em produtos
    .select(
        "ean",
        F.col("principio_ativo").alias("principio_ativo_match"),
        F.col("eh_medicamento").alias("eh_medicamento_match"),
        F.col("eh_controlado").alias("eh_controlado_match"),
        F.col("eh_tarjado").alias("eh_tarjado_match"),
        F.col("tipo_de_receita_completo").alias("tipo_de_receita_completo_match"),
        F.col("tipo_medicamento").alias("tipo_medicamento_match"),
        F.col("categorias").alias("categorias_match_med"),
        F.col("classes_terapeuticas").alias("classes_terapeuticas_match"),
        F.col("especialidades").alias("especialidades_match"),
    )
)
print(f"✅ Match medicamentos carregado: {match_medicamentos_recente.count()} produtos")

# COMMAND ----------

if DEBUG:
    match_medicamentos_recente.limit(10).display()

# COMMAND ----------

env.create_table_if_not_exists(
    Table.match_nao_medicamentos,
    schemas_match_nao_medicamentos.schema,
)

# COMMAND ----------

# Lê tabela de match não-medicamentos (pega o match mais recente por ean)
match_nao_medicamentos = env.table(Table.match_nao_medicamentos)

# Seleciona apenas o match mais recente por ean
window_spec_match_nao_med = Window.partitionBy("ean").orderBy(
    F.desc("match_nao_medicamentos_em")
)
match_nao_medicamentos_recente = (
    match_nao_medicamentos.withColumn(
        "row_num", F.row_number().over(window_spec_match_nao_med)
    )
    .filter(F.col("row_num") == 1)
    .drop(
        "row_num", "nome", "atualizado_em"
    )  # Remove colunas que já existem em produtos
    .select(
        "ean",
        F.col("eh_medicamento").alias("eh_medicamento_match_nao_med"),
        F.col("categorias").alias("categorias_match_nao_med"),
    )
)
print(
    f"✅ Match não-medicamentos carregado: {match_nao_medicamentos_recente.count()} produtos"
)

# COMMAND ----------

if DEBUG:
    match_nao_medicamentos_recente.limit(10).display()

# COMMAND ----------

# Faz merge com produtos_enriquecidos aplicando a lógica de priorização
produtos_enriquecidos = (
    produtos_enriquecidos.alias("p")
    .join(categorias_cascata_agg.alias("cc"), F.col("p.ean") == F.col("cc.ean"), "left")
    .join(match_medicamentos_recente.alias("mm"), ["ean"], "left")
    .join(match_nao_medicamentos_recente.alias("mnm"), ["ean"], "left")
)

# COMMAND ----------

# Aplica lógica de priorização para cada campo:
# PRIORIDADE: produtos_enriquecidos (extract_product_info) > match (fallback)

# Mapeamento de colunas: {coluna_original: coluna_match_fallback}
colunas_para_coalesce = {
    "principio_ativo": "principio_ativo_match",
    "eh_controlado": "eh_controlado_match",
    "eh_tarjado": "eh_tarjado_match",
    "tipo_de_receita_completo": "tipo_de_receita_completo_match",
    "tipo_medicamento": "tipo_medicamento_match",
    "classes_terapeuticas": "classes_terapeuticas_match",
    "especialidades": "especialidades_match",
}

# Aplica coalesce para todas as colunas do mapeamento
for coluna_original, coluna_match in colunas_para_coalesce.items():
    if coluna_original == "eh_controlado":
        # Caso especial: cast para boolean
        produtos_enriquecidos = produtos_enriquecidos.withColumn(
            coluna_original,
            F.coalesce(F.col(coluna_original), F.col(coluna_match)).cast("boolean"),
        )
    else:
        # Caso padrão: apenas coalesce
        produtos_enriquecidos = produtos_enriquecidos.withColumn(
            coluna_original,
            F.coalesce(F.col(coluna_original), F.col(coluna_match)),
        )

# Remove todas as colunas de match de uma vez
produtos_enriquecidos = produtos_enriquecidos.drop(*colunas_para_coalesce.values())

# COMMAND ----------

# Categorias: aplica lógica complexa baseada em eh_medicamento
# PRIORIDADE: categorias_cascata > produtos_enriquecidos > match (fallback)
produtos_enriquecidos = (
    produtos_enriquecidos.withColumn(
        "categorias_match_consolidado",
        F.when(
            F.col("eh_medicamento") == True, F.col("categorias_match_med")
        ).otherwise(F.col("categorias_match_nao_med")),
    )
    .withColumn(
        "categorias",
        F.coalesce(
            F.col("categorias_cascata"),
            F.col("categorias"),  # categorias vindas de extract_product_info
            F.col("categorias_match_consolidado"),  # match como fallback
        ),
    )
    .drop(
        "categorias_cascata",
        "categorias_match_med",
        "categorias_match_nao_med",
        "categorias_match_consolidado",
    )
)

# Garante que todas as categorias estão em Title Case
produtos_enriquecidos = produtos_enriquecidos.withColumn(
    "categorias", F.transform(F.col("categorias"), lambda x: F.initcap(x))
)

# Remove colunas temporárias de match
produtos_enriquecidos = produtos_enriquecidos.drop(
    "eh_medicamento_match", "eh_medicamento_match_nao_med"
)

# Seleciona apenas as colunas originais (remove duplicatas do join)
produtos_enriquecidos = produtos_enriquecidos.select(
    "p.*",
    "eh_controlado",
    "tipo_de_receita_completo",
    "principio_ativo",
    "tipo_medicamento",
    "eh_tarjado",
    "classes_terapeuticas",
    "especialidades",
    "categorias",
)

print("✅ Enriquecimentos de categorias e matches aplicados com sucesso!")

# COMMAND ----------

if DEBUG:
    produtos_enriquecidos.limit(200).display()

# COMMAND ----------

# Remove duplicatas e garante integridade
produtos_enriquecidos = produtos_enriquecidos.dropDuplicates(["ean"])
produtos_enriquecidos = produtos_enriquecidos.filter(
    produtos_enriquecidos.id.isNotNull()
)

if DEBUG:
    _total_med_depois = produtos_enriquecidos.filter(
        F.col("eh_medicamento") == True
    ).count()
    _com_dosagem_depois = produtos_enriquecidos.filter(
        (F.col("eh_medicamento") == True)
        & F.col("dosagem").isNotNull()
        & (F.col("dosagem") != "")
    ).count()
    print(
        f"[dosagem] DEPOIS — preenchido: {_com_dosagem_depois}/{_total_med_depois} "
        f"({100 * _com_dosagem_depois / _total_med_depois:.1f}%) | "
        f"faltando: {_total_med_depois - _com_dosagem_depois} ({100 * (1 - _com_dosagem_depois / _total_med_depois):.1f}%) | "
        f"ganho: +{_com_dosagem_depois - _com_dosagem_antes} produtos ({100 * (_com_dosagem_depois - _com_dosagem_antes) / _total_med:.1f}pp)"
    )
print(f"✅ Total de produtos enriquecidos: {produtos_enriquecidos.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adiciona indicações e indicações_categorias

# COMMAND ----------

# Adiciona a descrição curta de indicação gerada pelo notebook gpt_resume_indicacao_produtos
# Esta descrição substitui o campo 'indicacao' original, que é muito longa ou genérica em muitos casos
respostas = env.table(Table.gpt_descricoes_curtas_produtos)
produtos_enriquecidos = (
    produtos_enriquecidos.drop("indicacao")
    .alias("p")
    .join(respostas.alias("r"), produtos_enriquecidos.ean == respostas.ean, "left")
    .selectExpr("p.*", "r.descricao_curta AS indicacao")
)

# COMMAND ----------

categorias_de_complementares_por_indicacao = env.table(Table.indicacoes_categorias)
produtos_enriquecidos = (
    produtos_enriquecidos.drop("categorias_de_complementares_por_indicacao")
    .alias("p")
    .join(
        categorias_de_complementares_por_indicacao.alias("r"),
        produtos_enriquecidos.ean == categorias_de_complementares_por_indicacao.ean,
        "left",
    )
    .selectExpr(
        "p.*",
        "r.categorias_de_complementares_por_indicacao AS categorias_de_complementares_por_indicacao",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adiciona tags

# COMMAND ----------

tags = env.table(Table.enriquecimento_tags_produtos).drop("match_tags_em")
tags = tags.withColumnRenamed("atualizado_em", "tags_atualizado_em")

produtos_enriquecidos = produtos_enriquecidos.join(tags, on=["ean"], how="left")

produtos_enriquecidos = produtos_enriquecidos.withColumn(
    "atualizado_em", F.greatest(F.col("atualizado_em"), F.col("tags_atualizado_em"))
)

produtos_enriquecidos = produtos_enriquecidos.drop("tags_atualizado_em")

# COMMAND ----------

if DEBUG:
    print("🔍 Produtos com tags:")
    produtos_enriquecidos.filter(F.col("tags_complementares").isNotNull()).select(
        "ean", "nome", "tags_complementares"
    ).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adiciona info de ids_lojas_com_produtos

# COMMAND ----------

# Lê ids_lojas_com_produto pré-calculado e junta ao DataFrame final
ids_lojas_df = env.table(Table.ids_lojas_com_produto).select(
    "ean", "lojas_com_produto", "ids_lojas_com_produto"
)

produtos_enriquecidos = produtos_enriquecidos.join(ids_lojas_df, on=["ean"], how="left")

# COMMAND ----------

if DEBUG:
    print("🔍 Produtos com estoque em lojas:")
    produtos_enriquecidos.filter(F.col("lojas_com_produto").isNotNull()).select(
        "ean", "nome", "lojas_com_produto"
    ).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adiciona info scrape guia da farmacia

# COMMAND ----------

info_guia_farmacia = spark.read.parquet(guia_farmacia_folder)
info_guia_farmacia = info_guia_farmacia.filter(
    F.col("product_url").isNotNull() & (F.col("product_url") != "")
)
info_guia_farmacia = info_guia_farmacia.select(
    [F.col(c).alias("info_" + c) for c in info_guia_farmacia.columns]
)

if DEBUG:
    info_guia_farmacia.limit(100).display()

# COMMAND ----------

produtos_enriquecidos = produtos_enriquecidos.join(
    info_guia_farmacia,
    produtos_enriquecidos.ean == info_guia_farmacia.info_ean,
    how="left",
)

relacoes_colunas = {
    "info_quando_nao_devo_usar_este_medicamento": "contraindicacoes",
    "info_como_devo_usar_este_medicamento": "instrucoes_de_uso",
    "info_para_que_esse_medicamento_e_indicado": "indicacao",
    "info_quais_os_males_que_este_medicamento_pode_me_causar": "efeitos_colaterais",
    "info_como_esse_medicamento_funciona": "descricao",
    "info_onde_como_e_por_quanto_tempo_posso_guardar_esse_medicamento": "condicoes_de_armazenamento",
}

for coluna_origem, coluna_destino in relacoes_colunas.items():
    produtos_enriquecidos = produtos_enriquecidos.withColumn(
        coluna_destino, F.coalesce(F.col(coluna_destino), F.col(coluna_origem))
    )

colunas_concatenadas = [
    "info_o_que_devo_saber_antes_de_usar_este_medicamento",
    "info_o_que_fazer_se_alguem_usar_uma_quantidade_maior_do_que_a_indicada_deste_medicamento",
    "info_o_que_devo_fazer_quando_eu_me_esquecer_de_usar_este_medicamento",
]

produtos_enriquecidos = produtos_enriquecidos.withColumn(
    "advertencias_e_precaucoes",
    F.coalesce(
        F.col("advertencias_e_precaucoes"),
        F.when(
            F.concat_ws("", *[F.col(col) for col in colunas_concatenadas]) != "",
            F.concat_ws(
                " ",
                *[F.coalesce(F.col(col), F.lit("")) for col in colunas_concatenadas],
            ),
        ),
    ),
)

colunas_para_remover = list(info_guia_farmacia.columns)
produtos_enriquecidos = produtos_enriquecidos.drop(*colunas_para_remover)

# COMMAND ----------

if DEBUG:
    print("🔍 Produtos enriquecidos com info do Guia da Farmácia:")
    produtos_enriquecidos.filter(F.col("contraindicacoes").isNotNull()).select(
        "ean", "nome", "contraindicacoes", "efeitos_colaterais"
    ).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adiciona `power_phrase` do enriquecimento

# COMMAND ----------


power_phrase = env.table(Table.power_phrase_produtos).select('ean', 'power_phrase')
produtos_enriquecidos = (
    produtos_enriquecidos.alias("pe")
    .join(
        power_phrase.alias("p"),
        produtos_enriquecidos.ean == power_phrase.ean,
        "left",
    )
    .selectExpr("pe.*", "power_phrase")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Formata `categorias_de_complementares_por_indicacao` e `bula` para json

# COMMAND ----------

final_df = produtos_enriquecidos.withColumn(
    "categorias_de_complementares_por_indicacao",
    F.to_json("categorias_de_complementares_por_indicacao"),
)

# COMMAND ----------

if DEBUG:
    print("🔍 Produtos com power_phrase:")
    final_df.filter(F.col("power_phrase").isNotNull()).select(
        "ean", "nome", "power_phrase"
    ).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Atualiza colunas validadas pelo LLM as a Judge

# COMMAND ----------

df_llm_judge = (
    env.table(Table.gpt_judge_evaluator_product)
    # Ignora resultados inválidos
    .filter(F.col("is_valid") == True)
    # Seleciona a avaliação mais recente de cada EAN
    .orderBy(F.col("data_execucao").desc())
    .dropDuplicates(["ean"])
    .cache()
)

# COMMAND ----------

# NOTE: mantido fora do debug para que o cache funcione
print(f"✅ Tabela llm_as_a_judge carregada: {df_llm_judge.count()} registros")

# COMMAND ----------

if DEBUG:
    print("🔍 Avaliações do LLM Judge mais recentes por EAN:")
    df_llm_judge.limit(100).display()

# COMMAND ----------

# Concatena listas em strings separadas por '|', para manter compatibilidade com o schema original
df_llm_judge = (
    df_llm_judge.withColumn(
        'tags_complementares', F.concat_ws("|", F.col('tags_complementares'))
    )
    .withColumn(
        'tags_potencializam_uso', F.concat_ws("|", F.col('tags_potencializam_uso'))
    )
    .withColumn('tags_atenuam_efeitos', F.concat_ws("|", F.col('tags_atenuam_efeitos')))
    .withColumn('tags_substitutos', F.concat_ws("|", F.col('tags_substitutos')))
    .withColumn('tags_agregadas', F.concat_ws("|", F.col('tags_agregadas')))
)

# COMMAND ----------

# Transforma string '' em None
df_llm_judge = df_llm_judge.replace("", None)

# COMMAND ----------


def update_same_columns(
    original_df: DataFrame, update_df: DataFrame, join_keys: list[str]
) -> DataFrame:
    """
    Update original DataFrame with values from update DataFrame,
    but only for records where join_keys match and the original
    DataFrame column is null. No duplication of records.
    """

    original_cols = set(original_df.columns)
    update_cols = set(update_df.columns)

    # Lista de colunas em comum, removendo as variaveis de join
    common_cols = (original_cols & update_cols) - set(join_keys)

    if DEBUG:
        print(f"Common columns to potentially update: {sorted(common_cols)}")

    # Criando alias para facilitar lá na frente
    original_aliased = original_df.alias("orig")
    update_aliased = update_df.alias("upd")

    joined_df = original_aliased.join(update_aliased, on=join_keys, how="left")

    select_expressions = []

    for col_name in original_cols:
        if col_name in common_cols:
            # Usa sempre o valor do Judge (upd) se existir, senão mantém o original
            # Note que já removemos anteriormente as avaliações inválidas
            select_expressions.append(
                F.coalesce(F.col(f"upd.{col_name}"), F.col(f"orig.{col_name}")).alias(
                    col_name
                )
            )
        else:
            # colunas que não estão no df_update, mantêm igual
            select_expressions.append(F.col(f"orig.{col_name}").alias(col_name))

    # Aplica as expressões todas de uma vez, evitando múltiplos selects
    result_df = joined_df.select(*select_expressions)

    return result_df


# COMMAND ----------

# Faz uso da tabela LLM as a Judge para atualizar colunas que estejam vazias
final_df = update_same_columns(final_df, df_llm_judge, ["ean"])

# COMMAND ----------

if DEBUG:
    print("🔍 Dataframe de produtos depois de atualizar com o LLM Judge:")
    final_df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Atualiza `marca` e `fabricante` com resultados do websearch

# COMMAND ----------


df_websearch = env.table(Table.enriquecimento_websearch).drop('websearch_em')

final_df = (
    final_df.join(df_websearch, on=["ean"], how="left")
    .withColumn(
        'marca', F.coalesce('marca', 'marca_websearch')
    )  # prioriza as fontes da verdade
    .withColumn('fabricante', F.coalesce('fabricante', 'fabricante_websearch'))
    .drop('marca_websearch', 'fabricante_websearch')
)

# COMMAND ----------

if DEBUG:
    print("🔍 Produtos com marca/fabricante do websearch:")
    final_df.filter(
        F.col("marca").isNotNull() | F.col("fabricante").isNotNull()
    ).select("ean", "nome", "marca", "fabricante").limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva no delta table

# COMMAND ----------

final_df = final_df.withColumn(
    "gerado_em", F.coalesce("gerado_em", F.current_timestamp())
)

# NOTE: hoje essas colunas não vão para camada refined, por isso o drop
# TODO: essas colunas deixarão de existir na camada standard e refined.
final_df = final_df.drop("match_medicamentos_em", "match_nao_medicamentos_em")

# COMMAND ----------

if DEBUG:
    display(final_df.limit(100).select('ean', 'eans_alternativos'))

# COMMAND ----------

if DEBUG:
    print("📊 Resumo final do DataFrame:")
    print(f"Total de linhas: {final_df.count()}")
    final_df.limit(10).display()

# COMMAND ----------

# TODO: esse `overwriteSchema` acho que está matando o schema definido na `maggulake/utils/schemas/produtos_refined.py`
# Por exemplo não achei a coluna "power_phrase" no schema, mas está nesse notebook
# O ideal seria importar o schema aqui e usar ele pra salvar o dataframe.
final_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    Table.produtos_em_processamento.value
)
