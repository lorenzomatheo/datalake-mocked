# Databricks notebook source
# MAGIC %md
# MAGIC # Match medicamentos
# MAGIC
# MAGIC Esse notebook faz o seguinte:
# MAGIC - Pega produtos que nao sabemos se sao medicamentos ou nao
# MAGIC - Usa o nome desse produto como query para o vector search (hoje no milvus)
# MAGIC - Pega o retorno do vector search, se houver, e puxa os dados
# MAGIC
# MAGIC
# MAGIC Ou seja, se eu ainda nao tenho informacoes sobre esse produto, busco no vector store se tem algum produto com nome parecido e herdo as informacoes dele.
# MAGIC - Obs.: A coluna `eh_medicamento` eh a mais importante de ser enriquecida, pois a partir dela montamos 2 branches no enriquecimento

# COMMAND ----------

# MAGIC %pip install mlflow==2.15.1 mlflow[databricks] databricks-vectorsearch==0.40 databricks-sdk==0.30.0 langchain==0.2.15 langchain-core==0.2.35 langchain-community==0.2.13 langchain_milvus langchain_openai pymilvus openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment

# COMMAND ----------

import time
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas import match_medicamentos as schemas_match_medicamentos
from maggulake.utils.iters import create_batches
from maggulake.utils.time import agora_em_sao_paulo_str as agora
from maggulake.vector_search_retriever import MilvusRetriever, get_milvus_retriever

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "match_medicamentos",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
)

# COMMAND ----------

dbutils.widgets.text("max_products", "100000")

# COMMAND ----------

# Configurações
DEBUG = dbutils.widgets.get("debug") == "true"
spark = env.spark

# Acesso aos recursos do Environment
OPENAI_ORGANIZATION = env.settings.openai_organization
OPENAI_API_KEY = env.settings.openai_api_key
MILVUS_URI = env.settings.milvus_uri
MILVUS_TOKEN = env.settings.milvus_token

# COMMAND ----------

# Configuracoes do match medicamentos:
BATCH_SIZE: int = 100
DELAY_BETWEEN_BATCHES: float = 0.3  # segundos
CUTOFF_DAYS: int = 30  # Dias para considerar o match como antigo e executar novamente
SCORE_THRESHOLD: float = 0.6  # Score mínimo para considerar um match válido
MAX_PRODUCTS: int = int(dbutils.widgets.get("max_products"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lendo produtos da camada standard

# COMMAND ----------

produtos = env.table(Table.produtos_standard).cache()

# COMMAND ----------

# Lê a tabela consolidada de eh_medicamento
eh_medicamento_completo = env.table(Table.coluna_eh_medicamento_completo).select(
    "ean", F.col("eh_medicamento").alias("eh_medicamento_consolidado")
)

# Faz join com produtos para obter a classificação consolidada
produtos = produtos.join(eh_medicamento_completo, on=["ean"], how="left")

if DEBUG:
    print("📊 Produtos com classificação consolidada:")
    print(f"   - Total: {produtos.count()}")
    print(
        f"   - Com eh_medicamento_consolidado: {produtos.filter(F.col('eh_medicamento_consolidado').isNotNull()).count()}"
    )

    qtd_sem_info_antes = produtos.filter(
        F.coalesce(
            F.col("eh_medicamento_consolidado"), F.col("eh_medicamento")
        ).isNull()
    ).count()
    print(f"   - Produtos sem informação eh_medicamento: {qtd_sem_info_antes}")

# COMMAND ----------

if DEBUG:
    produtos.limit(10).display()

# COMMAND ----------

# filtra produtos para teste
# produtos = produtos.filter(F.col('eh_medicamento').isNull()).limit(500)
# produtos.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define retriever para fazer vector search


# COMMAND ----------


# NOTE: nao pode adicionar campos que nao estao na camada "standard"
TEXT_COLUMNS = [
    "ean",
    "nome",  # Esta aqui so para debug
    "informacoes_para_embeddings",  # NOTE: precisa dessa coluna pra usar o retriever
    "principio_ativo",
    "eh_medicamento",
    "eh_controlado",
    "eh_tarjado",
    "tipo_de_receita_completo",
    "tipo_medicamento",
    "categorias",
    "classes_terapeuticas",
    "especialidades",
]

filtro = "eh_medicamento == true"


def get_vector_search_retriever(score_threshold: float) -> MilvusRetriever:
    return get_milvus_retriever(
        OPENAI_ORGANIZATION,
        OPENAI_API_KEY,
        MILVUS_URI,
        MILVUS_TOKEN,
        filtro=filtro,
        max_results=1,
        score_threshold=score_threshold,
        campos_output=TEXT_COLUMNS,
        format_return=True,
    )


# COMMAND ----------

if DEBUG:  # Testando o retriever
    test_products = [
        "Vitamina A-z Suplemento 60 Cápsulas - Neo Química",
        "Latolise Caixa Com 30 Comprimidos",
        "Neolefrin Dia 800mg + 20mg, Caixa Com 20 Comprimidos",
        "Polivitamínico Neo Química Homem Cabelos E Barbas Saudáveis 60 Comprimidos",
        "Neogermina",
        "sinvastatina",
        "metformina",
        "dorflex",
        "dipirona",
        "rivotril",
    ]

    resultados = []
    retriever = get_vector_search_retriever(SCORE_THRESHOLD)

    print(f"\n{agora()} - Testando retriever com {len(test_products)} produtos:")
    produtos_com_resultado = 0

    for produto in test_products:
        result = retriever.invoke(produto)
        tem_resultado = result is not None and len(result) > 0
        if tem_resultado:
            produtos_com_resultado += 1
            match_nome = result[0].metadata.get('nome')
            print(f"- Deu match! {produto}  -> {match_nome}")
        else:
            match_nome = '❌'
            print(f"- Sem match. {produto}  -> {match_nome}")
        resultados.append((produto, SCORE_THRESHOLD, match_nome))
        time.sleep(0.1)  # pequeno delay para não sobrecarregar

    print(
        f"\nResumo: {produtos_com_resultado}/{len(test_products)} produtos retornaram documentos"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define funcoes para realizar operacoes de match

# COMMAND ----------


def process_batches(nomes, retriever: MilvusRetriever, batch_size: int) -> dict:
    print(f"{agora()} - inicia processamento em batch")

    batch_results_dict = {}
    qtd_para_executar = len(nomes)

    for batch in create_batches(nomes, batch_size):
        batch_results = retriever.batch(batch)
        batch_results_dict.update(dict(zip(batch, batch_results)))

        print(
            f"{agora()} - {len(batch_results_dict)} de {qtd_para_executar} produtos concluídos"
        )
        time.sleep(DELAY_BETWEEN_BATCHES)
    return batch_results_dict


# COMMAND ----------


def seleciona_produtos_para_fazer_o_match(
    df_standard: DataFrame, df_match_historico: DataFrame
) -> list[str]:
    """
    Seleciona produtos que precisam de atualização:
    - eh_medicamento está nulo OU
    - eh_medicamento é True mas tem colunas importantes vazias (principio_ativo, eh_tarjado, etc.)
    - nunca passaram pelo match ou o match é antigo (> CUTOFF_DAYS)
    - Possui o nome que nao seja nulo e tem tamanho razoavel.
    """

    print(f"{agora()} - inicia seleção de produtos")

    cutoff_date = datetime.now() - timedelta(days=CUTOFF_DAYS)

    # O campo "nome" sera usado como vector search query, entao ele precisa estar
    # presente e ser representativo. O numero 10 foi escolha arbitraria.
    df_filter = df_standard.filter(
        F.col("nome").isNotNull() & (F.length(F.col("nome")) >= 10)
    )

    # Condições para selecionar produtos que precisam de match:
    # 1. eh_medicamento está nulo (prioriza consolidado, fallback para original)
    # 2. eh_medicamento é True mas tem colunas importantes vazias

    # Usa eh_medicamento consolidado se disponível, senão usa o original
    eh_med = F.coalesce(F.col("eh_medicamento_consolidado"), F.col("eh_medicamento"))

    condicao_eh_medicamento_nulo = eh_med.isNull()

    # Para medicamentos confirmados, verifica se tem informações faltando
    condicao_medicamento_sem_info = (eh_med == True) & (
        F.col("principio_ativo").isNull()
        | F.col("eh_tarjado").isNull()
        | F.col("eh_controlado").isNull()
        | F.col("tipo_medicamento").isNull()
        | F.col("tipo_de_receita_completo").isNull()
        | (F.size(F.col("especialidades")) == 0)
        | (F.size(F.col("classes_terapeuticas")) == 0)
        | (F.size(F.col("categorias")) == 0)
    )

    # Combina as duas condições
    condicao_precisa_match = (
        condicao_eh_medicamento_nulo | condicao_medicamento_sem_info
    )

    # Faz left join com histórico de match para verificar se já foi processado
    df_com_historico = (
        df_filter.filter(condicao_precisa_match)
        .alias("produtos")
        .join(
            df_match_historico.select("ean", "match_medicamentos_em").alias(
                "historico"
            ),
            on="ean",
            how="left",
        )
    )

    # Condição de tempo: nunca passou pelo match ou o match é antigo
    condicao_tempo = F.col("historico.match_medicamentos_em").isNull() | (
        F.col("historico.match_medicamentos_em") <= F.lit(cutoff_date)
    )

    produtos_to_update: list[str] = (
        df_com_historico.filter(condicao_tempo)
        .select("produtos.nome")
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    total_original = len(produtos_to_update)
    if len(produtos_to_update) > MAX_PRODUCTS:
        produtos_to_update = produtos_to_update[:MAX_PRODUCTS]
        print(
            f"{agora()} - ⚠️  Limitando processamento devido a max_products: {total_original} → {MAX_PRODUCTS} produtos"
        )

    print(f"{agora()} - Total de produtos a atualizar: {len(produtos_to_update)}")

    return produtos_to_update


# COMMAND ----------


def processa_resultado_do_milvus(
    raw,
) -> list[schemas_match_medicamentos.MedicamentoRetrieverResponse]:
    results: list[schemas_match_medicamentos.MedicamentoRetrieverResponse] = []

    for nome, docs in raw.items():
        if not docs:
            continue

        # NOTE: Se multiplos documentos forem retornados, pega o primeiro.
        # Isso eh uma limitacao atual, conseguimos dar conta somente do documento mais similar.
        # No futuro poderiamos calcular uma media semantica dos N documentos mais similares.
        doc = docs[0] if isinstance(docs, list) and docs else docs
        meta = doc.metadata or {}

        results.append(
            schemas_match_medicamentos.MedicamentoRetrieverResponse(
                nome=nome,
                principio_ativo=meta.get("principio_ativo"),
                eh_medicamento=meta.get("eh_medicamento"),
                eh_tarjado=meta.get("eh_tarjado"),
                tipo_medicamento=meta.get("tipo_medicamento"),
                tipo_de_receita_completo=meta.get("tipo_de_receita_completo"),
                eh_controlado=meta.get("eh_controlado"),
                especialidades=list(meta.get("especialidades") or []),
                classes_terapeuticas=list(meta.get("classes_terapeuticas") or []),
                categorias=list(meta.get("categorias") or []),
            )
        )
    return results


# COMMAND ----------


def process_dataframe(
    df_standard: DataFrame,
    df_match_historico: DataFrame,
    score_threshold: float,
) -> DataFrame:
    """
    Processa produtos da camada standard e retorna DataFrame com novos matches.

    Retorna apenas os produtos que foram atualizados nesta execução, não todos os produtos.
    """

    nomes_to_update = seleciona_produtos_para_fazer_o_match(
        df_standard, df_match_historico
    )

    if not nomes_to_update:
        print(f"{agora()} - Nenhum produto para atualizar")
        return spark.createDataFrame([], schemas_match_medicamentos.schema)

    # Realiza os matches
    vs_retriever = get_vector_search_retriever(score_threshold)
    raw = process_batches(nomes_to_update, vs_retriever, BATCH_SIZE)
    results = processa_resultado_do_milvus(raw)
    results_tuples = [r.to_tuple() for r in results]

    # cria DataFrame a partir dos resultados do match
    match_df = spark.createDataFrame(
        results_tuples, schemas_match_medicamentos.schema_match_result
    )

    # Junta com os dados da standard para pegar ean
    df_novos_matches = (
        df_standard.select("ean", "nome", "atualizado_em")
        .join(match_df, on="nome", how="inner")
        .select(
            "ean",
            "nome",
            F.col("principio_ativo_match").alias("principio_ativo"),
            F.col("eh_medicamento_match").alias("eh_medicamento"),
            F.col("eh_tarjado_match").alias("eh_tarjado"),
            F.col("tipo_de_receita_completo_match").alias("tipo_de_receita_completo"),
            F.col("eh_controlado_match").alias("eh_controlado"),
            F.col("tipo_medicamento_match").alias("tipo_medicamento"),
            F.col("especialidades_match").alias("especialidades"),
            F.col("classes_terapeuticas_match").alias("classes_terapeuticas"),
            F.col("categorias_match").alias("categorias"),
            F.current_timestamp().alias("match_medicamentos_em"),
            "atualizado_em",
        )
    )

    print(f"{agora()} - {df_novos_matches.count()} novos matches encontrados")
    return df_novos_matches


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria tabela de enriquecimento se não existir

# COMMAND ----------

env.create_table_if_not_exists(
    Table.match_medicamentos,
    schemas_match_medicamentos.schema,
)

print(f"{agora()} - ✅ Tabela {Table.match_medicamentos.value} criada/verificada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carrega histórico de matches

# COMMAND ----------

match_historico = env.table(Table.match_medicamentos)

if DEBUG:
    print(
        f"{agora()} - Tabela de match possui {match_historico.count()} registros históricos"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Realiza o match

# COMMAND ----------

novos_matches = process_dataframe(
    df_standard=produtos,
    df_match_historico=match_historico,
    score_threshold=SCORE_THRESHOLD,
)

# COMMAND ----------

if DEBUG:
    print("\n📋 Amostra dos novos matches:")
    novos_matches.limit(20).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando resultado

# COMMAND ----------

# Faz merge dos novos matches com a tabela existente
if not novos_matches.count() > 0:
    dbutils.notebook.exit("Nenhum novo match para salvar")

# COMMAND ----------

# Usa merge para fazer upsert (atualiza se existe, insere se não existe)
delta_table = DeltaTable.forName(spark, Table.match_medicamentos.value)

delta_table.alias("target").merge(
    novos_matches.alias("source"),
    "target.ean = source.ean",
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

if DEBUG:
    print(
        f"{agora()} - ✅ Merge concluído: {novos_matches.count()} produtos atualizados/inseridos em {Table.match_medicamentos.value}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação dos resultados

# COMMAND ----------

if DEBUG:
    print("\n📊 Estatísticas finais da tabela de match:")
    match_final = env.table(Table.match_medicamentos)

    total = match_final.count()
    print(f"Total de produtos na tabela: {total}")

    # Distribuição por eh_medicamento
    print("\nDistribuição por eh_medicamento:")
    match_final.groupBy("eh_medicamento").count().orderBy("eh_medicamento").display()

    print("\n📋 Amostra aleatória dos dados:")
    match_final.orderBy(F.rand()).limit(20).display()
