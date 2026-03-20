# Databricks notebook source
# MAGIC %pip install langchain langchain-google-genai google-genai tiktoken langchain-google-vertexai langsmith
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Gerar categorias em 2 Níveis
# MAGIC
# MAGIC Este notebook implementa a estratégia de categorização em **apenas 2 chamadas ao LLM**:
# MAGIC 1. **Nível 1**: Determina a super categoria (Medicamentos, Perfumaria, etc.)
# MAGIC 2. **Nível 2**: Para cada super categoria, determina "Meso -> Micro" de uma vez
# MAGIC

# COMMAND ----------

# MAGIC %md ### Task Inputs

# COMMAND ----------

import asyncio
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import pyspark.sql.functions as F
import pytz
from pyspark.sql.functions import rand
from pyspark.sql.types import Row
from pyspark.sql.window import Window

from maggulake.enums.categorias import CategoriasWrapper
from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.llm.models import get_model_name
from maggulake.llm.tagging_processor import TaggingProcessor
from maggulake.llm.vertex import (
    get_location_by_model,
    setup_langsmith,
    setup_vertex_ai_credentials,
)
from maggulake.pipelines.schemas.categoriza_produtos import (
    CategorizacaoNivel1,
    CategorizacaoNivel2,
    categorias_cascata_schema_string,
)
from maggulake.prompts import get_prompt_categorias
from maggulake.utils.iters import batched
from maggulake.utils.strings import truncate_text_to_tokens
from maggulake.utils.time import agora_em_sao_paulo_str

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "categoriza_produtos_em_cascata",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={
        "batch_size": "100",
        "max_produtos": "1000",
        "debug": ["true", "false"],
        "llm_provider": ["gemini_vertex"],
        "model_size": ["SMALL", "LARGE"],
    },
)

# COMMAND ----------

setup_langsmith(dbutils, env.spark, stage=env.settings.stage.value)

# COMMAND ----------

# Env variables
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
MAX_PRODUTOS = int(dbutils.widgets.get("max_produtos"))
DEBUG = dbutils.widgets.get("debug").lower() == "true"
LLM_PROVIDER = dbutils.widgets.get("llm_provider")
MODEL_SIZE = dbutils.widgets.get("model_size")

# COMMAND ----------

# Configuração Vertex AI

PROJECT_ID, _ = setup_vertex_ai_credentials(spark)
MODEL_NAME = get_model_name(provider=LLM_PROVIDER, size=MODEL_SIZE)
LOCATION = get_location_by_model(MODEL_NAME)

if DEBUG:
    print(f"[{agora_em_sao_paulo_str()}] 🤖 Configuração do LLM:")
    print(f"  - Provider: {LLM_PROVIDER}")
    print(f"  - Model: {MODEL_NAME}")
    print(f"  - Location: {LOCATION}")
    print(f"  - Size: {MODEL_SIZE}")

# COMMAND ----------

# MAGIC %md ### Cria tabela Delta se não existir

# COMMAND ----------

env.create_table_if_not_exists(
    Table.categorias_cascata,
    categorias_cascata_schema_string,
)

print(
    f"[{agora_em_sao_paulo_str()}] ✅ Tabela Delta criada/verificada: {Table.categorias_cascata.value}"
)

# COMMAND ----------

# Instancia a classe de categorias
categorias_wrapper = CategoriasWrapper()

# COMMAND ----------

# MAGIC %md ### Funções Auxiliares

# COMMAND ----------


def formata_info_produto(produto, max_tokens: int = 13000) -> str:
    campos = [
        ("EAN (Código de barras)", produto.ean),
        ("Nome", produto.nome),
        ("Nomes Alternativos", produto.nomes_alternativos),
        ("Descrição", produto.descricao),
        ("Marca", produto.marca),
        ("Fabricante", produto.fabricante),
        (
            "Público Alvo (Idade/Sexo)",
            f"{produto.idade_recomendada or ''} {produto.sexo_recomendado or ''}".strip(),
        ),
        ("Para que esse produto é indicado", produto.indicacao),
        ("Categorias, de acordo com websites especializados", produto.categorias),
    ]

    if produto.eh_medicamento:
        campos.extend(
            [
                ("Princípio Ativo", produto.principio_ativo),
                ("Classes Terapêuticas", produto.classes_terapeuticas),
                ("Forma Farmacêutica", produto.forma_farmaceutica),
            ]
        )

    # Junta sem usar linhas vazias
    texto = "\n".join([f"{label}: {valor}" for label, valor in campos if valor])

    return truncate_text_to_tokens(texto, max_tokens, "cl100k_base")


# COMMAND ----------

# MAGIC %md ### Inicialização dos TaggingProcessors

# COMMAND ----------

# TaggingProcessor para Nível 1 (Super Categoria)
tagging_processor_nivel1 = TaggingProcessor(
    provider=LLM_PROVIDER,
    project_id=PROJECT_ID,
    location=LOCATION,
    model=MODEL_NAME,
    prompt_template=get_prompt_categorias(),
    output_schema=CategorizacaoNivel1,
    temperature=0.1,
)


# COMMAND ----------

# MAGIC %md ### Pipeline de Categorização em 2 Níveis com TaggingProcessor

# COMMAND ----------


@dataclass
class CategoriaCompleta:
    super_categoria: str
    meso_categoria: str
    micro_categoria: Optional[str]


@dataclass
class ResultadoCategorizacao:
    ean: str
    categorias: List[CategoriaCompleta]


# COMMAND ----------


async def processar_categoria_nivel2(
    produto,
    super_categoria: str,
) -> List[CategoriaCompleta]:
    """
    Processa o nível 2 de categorização (meso->micro) para uma super categoria.
    """
    categorias_resultado: list[CategoriaCompleta] = []

    # Monta o prompt com as opções disponíveis
    opcoes_formatadas = categorias_wrapper.get_opcoes_meso_micro_formatadas(
        super_categoria
    )
    if opcoes_formatadas == "Nenhuma categoria disponível":
        if DEBUG:
            print(
                f"[{agora_em_sao_paulo_str()}] ⚠️  EAN {produto.ean}: "
                f"Nenhuma subcategoria disponível para '{super_categoria}'"
            )
        return []

    prompt_n2 = get_prompt_categorias(
        opcoes_meso_micro=f"OPÇÕES DISPONÍVEIS para {super_categoria}:\n{opcoes_formatadas}"
    )

    tagging_processor_nivel2 = TaggingProcessor(
        provider=LLM_PROVIDER,
        project_id=PROJECT_ID,
        location=LOCATION,
        model=MODEL_NAME,
        prompt_template=prompt_n2,
        output_schema=CategorizacaoNivel2,
        temperature=0.1,
    )

    info_produto = (
        formata_info_produto(produto)
        + f"\n\nSuper Categoria identificada: {super_categoria}"
    )

    # Executa a categorização de nível 2
    resposta_n2 = await tagging_processor_nivel2.executa_tagging_chain_async(
        info_produto, timeout=600
    )

    meso_micros_str = resposta_n2.get('categorias_meso_micro', [])
    meso_micros = categorias_wrapper.parse_meso_micro_lista(meso_micros_str)

    # Processa cada par meso->micro
    for meso_cat, micro_cat in meso_micros:
        # 1. Valida e normaliza meso categoria
        meso_normalizada = categorias_wrapper.validar_e_normalizar_meso_categoria(
            meso_cat, super_categoria
        )
        if not meso_normalizada:
            if DEBUG:
                print(
                    f"[{agora_em_sao_paulo_str()}] ⚠️  EAN {produto.ean}: "
                    f"Meso inválida '{meso_cat}' para '{super_categoria}' (ignorada)"
                )
            continue  # Pula se meso for inválida

        # 2. Valida e normaliza micro categoria (pode ser None)
        micro_normalizada = categorias_wrapper.validar_e_normalizar_micro_categoria(
            micro_cat, super_categoria, meso_normalizada
        )
        if micro_cat and not micro_normalizada:
            if DEBUG:
                print(
                    f"[{agora_em_sao_paulo_str()}] ⚠️  EAN {produto.ean}: "
                    f"Micro inválida '{micro_cat}' para '{super_categoria} -> {meso_normalizada}' "
                    f"(salvando apenas até meso)"
                )

        # 3. Adiciona categoria (com ou sem micro)
        categorias_resultado.append(
            CategoriaCompleta(
                super_categoria=super_categoria,
                meso_categoria=meso_normalizada,
                micro_categoria=micro_normalizada,
            )
        )

    return categorias_resultado


# COMMAND ----------


async def categoriza_produto_2_niveis(produto) -> ResultadoCategorizacao:
    """
    Categoriza um produto em 2 níveis sequenciais usando TaggingProcessor.

    Nível 1: Determina super categoria(s) - Usa CategorizacaoNivel1
    Nível 2: Para cada super categoria, determina meso->micro categoria(s) - Usa CategorizacaoNivel2
    """
    categorias_resultado: List[CategoriaCompleta] = []

    # NÍVEL 1: Determina super categoria(s)
    info_produto = formata_info_produto(produto)
    resposta_n1 = await tagging_processor_nivel1.executa_tagging_chain_async(
        info_produto, timeout=600
    )
    super_categorias: list[str] = resposta_n1.get('super_categorias', [])

    # if not super_categorias:
    #     if DEBUG:
    #         print(
    #             f"[{agora_em_sao_paulo_str()}] ⚠️  EAN {produto.ean} Nome '{produto.nome}' Resposta LLM '{resposta_n1}': "
    #             "Nao encontrou Super categoria!"
    #         )

    # Processa cada super categoria
    for super_categoria in super_categorias:
        # 1. Valida e normaliza super categoria
        super_normalizada = categorias_wrapper.validar_e_normalizar_super_categoria(
            super_categoria
        )
        if not super_normalizada:
            if DEBUG:
                print(
                    f"[{agora_em_sao_paulo_str()}] ⚠️  EAN {produto.ean}: "
                    f"Super categoria inválida '{super_categoria}' (ignorada)"
                )
            continue  # Pula se super for inválida

        # 2. Processa nível 2 (meso->micro) para esta super categoria
        categorias_nivel2 = await processar_categoria_nivel2(produto, super_normalizada)

        # 3. Adiciona as categorias validadas ao resultado
        categorias_resultado.extend(categorias_nivel2)

    return ResultadoCategorizacao(ean=produto.ean, categorias=categorias_resultado)


# COMMAND ----------


async def categoriza_produtos_batch(produtos: List) -> List[ResultadoCategorizacao]:
    return await asyncio.gather(
        *[categoriza_produto_2_niveis(p) for p in produtos],
        return_exceptions=True,
    )


# COMMAND ----------

# MAGIC %md ### Carregamento e Filtragem de Produtos

# COMMAND ----------

# NOTE: shuffle das linhas usando rand()
produtos_refined_table = env.table(Table.produtos_refined).orderBy(rand()).cache()

print(
    f"[{agora_em_sao_paulo_str()}] 📊 Total de produtos na tabela refined: {produtos_refined_table.count():}"
)

# COMMAND ----------

# Lê categorias já geradas
categorias_ja_feitas = env.table(Table.categorias_cascata)

total_enriquecimentos = categorias_ja_feitas.count()
eans_unicos = categorias_ja_feitas.select('ean').distinct().count()
print(
    f"[{agora_em_sao_paulo_str()}] 📊 Enriquecimentos existentes: {total_enriquecimentos:} registros"
)
print(f"[{agora_em_sao_paulo_str()}] 📊 EANs únicos já categorizados: {eans_unicos:}")

# COMMAND ----------

# Identifica produtos que ainda não foram categorizados
# NOTE: Com a nova estrutura, cada produto pode ser reprocessado múltiplas vezes
# Para evitar reprocessamento, filtramos apenas produtos que nunca foram categorizados
# TODO: definir um "tempo de vida util" para o enriquecimento. Começar a refazer depois
# que o ultimo enriquecimento do EAN for mais antigo que X dias. Sugestão: 365 dias
produtos_sem_categoria = produtos_refined_table.join(
    categorias_ja_feitas.select("ean").distinct(), on="ean", how="left_anti"
)

# Fazer um sort pela coluna "eh_medicamento", para conseguir deixar os "eh_medicamento" nulos primeiro
produtos_sem_categoria = produtos_sem_categoria.sort("eh_medicamento").cache()

# COMMAND ----------

# NOTE: Fora do debug para poder ver a contagem pelo job run
print(
    f"[{agora_em_sao_paulo_str()}] 📊 Produtos pendentes (nunca categorizados): {produtos_sem_categoria.count():}"
)

# COMMAND ----------

if MAX_PRODUTOS > 0:
    produtos_a_processar = produtos_sem_categoria.limit(MAX_PRODUTOS)
    print(
        f"[{agora_em_sao_paulo_str()}] ⚙️ limitando para {MAX_PRODUTOS} produtos (MAX_PRODUTOS definido)"
    )
else:
    produtos_a_processar = produtos_sem_categoria
    print(f"[{agora_em_sao_paulo_str()}] ⚙️ processando todos os produtos pendentes")

# COMMAND ----------

produtos_a_processar: list[Row] = produtos_a_processar.collect()

print(
    f"[{agora_em_sao_paulo_str()}] 📊 Produtos selecionados para este job: {len(produtos_a_processar)}"
)

# COMMAND ----------

# MAGIC %md ### Teste com Produto Individual

# COMMAND ----------


# Teste com um produto para validar o pipeline
async def testa_produto() -> Optional[ResultadoCategorizacao]:
    if len(produtos_a_processar) < 1:
        return

    produto_teste = produtos_a_processar[0]
    print(formata_info_produto(produto_teste))

    resultado_teste = await categoriza_produto_2_niveis(produto_teste)
    print("\nResultado:")
    # print(resultado_teste)

    return resultado_teste


# Executa o teste
print(await testa_produto())


# COMMAND ----------

# MAGIC %md
# MAGIC ### Processamento em Lote

# COMMAND ----------


def _cria_row(resultado: ResultadoCategorizacao) -> Row:
    categorias_dict = [
        {
            "super_categoria": cat.super_categoria,
            "meso_categoria": cat.meso_categoria,
            "micro_categoria": cat.micro_categoria,
        }
        for cat in resultado.categorias
    ]
    return Row(
        ean=resultado.ean,
        categorias=categorias_dict,
        gerado_em=datetime.now(pytz.timezone("America/Sao_Paulo")),
    )


def _reporta_sem_categoria(eans_sem_categoria: list):
    if not eans_sem_categoria:
        return

    print(
        f"[{agora_em_sao_paulo_str()}] ⚠️  {len(eans_sem_categoria)} produto(s) "
        f"sem categoria válida (LLM não atribuiu nenhuma categoria)"
    )
    if len(eans_sem_categoria) <= 10:
        print(f"       EANs: {', '.join(eans_sem_categoria)}")


def _reporta_erros(erros: list):
    if not erros:
        return

    print(
        f"[{agora_em_sao_paulo_str()}] ❌ {len(erros)} produto(s) com erro durante categorização"
    )
    tipos_erro = Counter([type(e).__name__ for e in erros])
    for tipo, count in tipos_erro.most_common():
        print(f"       {tipo}: {count} ocorrência(s)")
    if len(erros) <= 3:
        for i, erro in enumerate(erros, 1):
            print(f"       Detalhe erro {i}: {str(erro)[:150]}")


# COMMAND ----------


def salva_categorias(resultados: List[ResultadoCategorizacao]):
    """
    Salva os resultados das categorizações na tabela Delta usando append.
    """
    rows, eans_sem_categoria, erros = [], [], []

    for resultado in resultados:
        if not isinstance(resultado, ResultadoCategorizacao):
            erros.append(resultado)
            continue
        if not resultado.categorias:
            eans_sem_categoria.append(resultado.ean)
            continue
        rows.append(_cria_row(resultado))

    # Reporta resumo do batch
    total_batch = len(resultados)
    print(
        f"[{agora_em_sao_paulo_str()}] 📊 Resumo do batch: "
        f"{len(rows)} salvos | {len(eans_sem_categoria)} sem categoria | "
        f"{len(erros)} erros | {total_batch} total"
    )

    _reporta_sem_categoria(eans_sem_categoria)
    _reporta_erros(erros)

    if not rows:
        print(f"[{agora_em_sao_paulo_str()}] ⚠️  Nenhum produto para salvar neste batch")
        return

    df = spark.createDataFrame(rows, schema=categorias_cascata_schema_string)
    df.write.mode("append").saveAsTable(Table.categorias_cascata.value)
    print(
        f"[{agora_em_sao_paulo_str()}] ✅ {len(rows)} produto(s) salvos com sucesso na tabela Delta"
    )


# COMMAND ----------


async def processa_todos_produtos(produtos: List[Row]):
    total = len(produtos)
    processados = 0

    print(
        f"[{agora_em_sao_paulo_str()}] 🚀 Iniciando categorização de {total} produtos "
        f"(batch size: {BATCH_SIZE})"
    )

    num_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    for i, batch in enumerate(batched(produtos, BATCH_SIZE), 1):
        print(
            f"\n[{agora_em_sao_paulo_str()}] 📦 Batch {i}/{num_batches} "
            f"({len(batch)} produtos)..."
        )

        resultados = await categoriza_produtos_batch(batch)
        salva_categorias(resultados)

        processados += len(batch)
        progresso = (processados / total) * 100
        print(
            f"[{agora_em_sao_paulo_str()}] 📈 Progresso geral: {processados}/{total} "
            f"({progresso:.1f}%)"
        )

    print(
        f"\n[{agora_em_sao_paulo_str()}] ✅ Categorização concluída! "
        f"{processados} produtos processados"
    )


# COMMAND ----------

# Executa o processamento
if len(produtos_a_processar) < 1:
    dbutils.notebook.exit("Nenhum produto para processar.")

# COMMAND ----------

await processa_todos_produtos(produtos_a_processar)


# COMMAND ----------

# MAGIC %md ### Validação dos Resultados

# COMMAND ----------

# Lê os resultados salvos da tabela Delta
categorias_geradas = env.table(Table.categorias_cascata)


# COMMAND ----------

if DEBUG:
    print(
        f"[{agora_em_sao_paulo_str()}] 📊 Total de registros na tabela: {categorias_geradas.count()}"
    )
    print(
        f"[{agora_em_sao_paulo_str()}] 📊 EANs únicos categorizados: {categorias_geradas.select('ean').distinct().count()}"
    )

# COMMAND ----------

if DEBUG:
    print("\nAmostra dos resultados (estrutura com array de categorias):")
    categorias_geradas.limit(20).display()

# COMMAND ----------

if DEBUG:
    print("\nHistórico de enriquecimentos por EAN (múltiplas execuções):")
    categorias_geradas.groupBy("ean").count().filter(F.col("count") > 1).orderBy(
        F.desc("count")
    ).limit(20).display()

# COMMAND ----------

if DEBUG:
    print("\nÚltimo enriquecimento por EAN:")

    window_spec = Window.partitionBy("ean").orderBy(F.desc("gerado_em"))

    ultimos_enriquecimentos = (
        categorias_geradas.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    ultimos_enriquecimentos.limit(30).display()

# COMMAND ----------

if DEBUG:
    print("\nDistribuição de categorias (último enriquecimento por EAN):")

    window_spec = Window.partitionBy("ean").orderBy(F.desc("gerado_em"))

    ultimos_enriquecimentos = (
        categorias_geradas.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Explode das categorias para contar
    categorias_exploded = ultimos_enriquecimentos.select(
        "ean", F.explode("categorias").alias("categoria")
    ).select(
        "ean",
        F.col("categoria.super_categoria").alias("super_categoria"),
        F.col("categoria.meso_categoria").alias("meso_categoria"),
        F.col("categoria.micro_categoria").alias("micro_categoria"),
    )

    print("\nDistribuição por Super Categoria:")
    categorias_exploded.groupBy("super_categoria").count().orderBy(
        F.desc("count")
    ).limit(30).display()

    print("\nDistribuição por Meso Categoria:")
    categorias_exploded.groupBy("super_categoria", "meso_categoria").count().orderBy(
        F.desc("count")
    ).limit(30).display()
