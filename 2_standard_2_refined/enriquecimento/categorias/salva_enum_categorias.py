# Databricks notebook source
# MAGIC %md
# MAGIC # Salva Enum Categorias
# MAGIC
# MAGIC - Cria ou atualiza a collection `categorias` no Milvus (aka ZillizCloud).
# MAGIC - Usamos esse notebook quando há alguma alteração no enum de categorias

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from uuid import uuid4

from pymilvus import DataType

from maggulake.enums.categorias import CategoriasWrapper
from maggulake.environment import DatabricksEnvironmentBuilder
from maggulake.utils.iters import batched

# COMMAND ----------

MODEL = "text-embedding-3-large"
DIMENSION = 1536
COLLECTION_NAME = "categorias"
BATCH_SIZE = 100

env = DatabricksEnvironmentBuilder.build(
    "gera_embeddings_produtos",
    dbutils,
)

spark = env.spark

categorias_wrapper = CategoriasWrapper()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cria collection

# COMMAND ----------


def create_collection():
    client = env.vector_search.milvus
    client.drop_collection(collection_name=COLLECTION_NAME)

    schema = client.create_schema(
        auto_id=False,
        enable_dynamic_field=True,
    )

    schema.add_field(
        field_name="id", datatype=DataType.VARCHAR, max_length=36, is_primary=True
    )
    schema.add_field(
        field_name="eh_recomendacao",
        datatype=DataType.BOOL,
    )
    schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=DIMENSION)

    index_params = client.prepare_index_params()
    index_params.add_index(field_name="id")
    index_params.add_index(field_name="eh_recomendacao")
    index_params.add_index(
        field_name="vector",
        index_type="IVF_FLAT",
        metric_type="COSINE",
    )

    client.create_collection(
        collection_name=COLLECTION_NAME,
        schema=schema,
        index_params=index_params,
    )


# COMMAND ----------

create_collection()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define as categorias

# COMMAND ----------

categorias_flat = [
    {
        "id": str(uuid4()),
        "macro_categoria": macro_categoria,
        "categoria": categoria,
        "micro_categoria": (
            micro_categoria[0]
            if isinstance(micro_categoria, tuple)
            else micro_categoria
        ),
        "eh_recomendacao": (
            micro_categoria[1] if isinstance(micro_categoria, tuple) else True
        ),
        "categoria_flat": categorias_wrapper.flatten_categoria(
            macro_categoria, categoria, micro_categoria
        ),
    }
    for macro_categoria, categorias in categorias_wrapper.get_arvore_categorias().items()
    for categoria, micro_categorias in categorias.items()
    for micro_categoria in micro_categorias or [None]
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mexe no Milvus

# COMMAND ----------


def get_prompt(categorias):
    return [
        f"""
Categoria de produtos vendidos em uma farmácia.
Macro categoria: {categoria["macro_categoria"] or "Geral"}
Categoria: {categoria["categoria"]}
Micro categoria: {categoria["micro_categoria"] or "Nenhuma"}
        """.strip()
        for categoria in categorias
    ]


# get_prompt(categorias_flat[:10])

# COMMAND ----------


def gera_embeddings_categorias(categorias):
    prompts = get_prompt(categorias)
    response = env.openai.embeddings.create(
        model=MODEL,
        input=prompts,
        dimensions=DIMENSION,
    )
    embeddings = [e.embedding for e in response.data]

    return [
        {
            **categoria,
            "vector": embedding,
        }
        for categoria, embedding in zip(categorias, embeddings)
    ]


# COMMAND ----------


def insert_milvus(categorias):
    env.vector_search.milvus.insert(
        collection_name=COLLECTION_NAME, data=list(categorias)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Envia categorias pro milvus

# COMMAND ----------

total = len(categorias_flat)

for i, batch in enumerate(batched(categorias_flat, BATCH_SIZE), 1):
    print(f"Fazendo batch {i}")

    categorias_com_embeddings = gera_embeddings_categorias(batch)
    insert_milvus(categorias_com_embeddings)

    updated = i * BATCH_SIZE if i * BATCH_SIZE < total else total
    print(f"Sucesso! {updated} categorias atualizadas ({updated * 100 / total:.2f}%)")

print(f"Processo concluído. Total de {total} produtos atualizadas.")
