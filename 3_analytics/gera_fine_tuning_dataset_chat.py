# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("gera_fine_tuning_dataset_chat").getOrCreate()

VALIDATION_SIZE = 5000  # máximo recomendado na documentação
RANDOM_SEED = 42
STAGE = "prod"
CATALOG = "production"
S3_BUCKET_NAME = f"maggu-datalake-{STAGE}"
S3_OUTPUT_FOLDER = f"s3://{S3_BUCKET_NAME}/3-refined-layer/fine_tuning_chat_dataset"

training_path = f"{S3_OUTPUT_FOLDER}/training"
validation_path = f"{S3_OUTPUT_FOLDER}/validation"
chat_agno_table = f"{CATALOG}.raw.chat_agno_{CATALOG}_payload"
chat_milvus_table = f"{CATALOG}.raw.chat_milvus_{CATALOG}_payload"

# COMMAND ----------

# União das tabelas agno e milvus
df_fonte = (
    spark.table(chat_agno_table)
    .select("request", "response")
    .unionAll(spark.table(chat_milvus_table).select("request", "response"))
)

# TODO selecionar apenas pares de respostas considerados bons e validados por um especialista no domínio

# COMMAND ----------

# Parse dos JSONs
df_parse = df_fonte.select(
    F.from_json(
        F.col("request"),
        "struct<client_request_id:string,messages:array<struct<content:string,role:string>>>",
    ).alias("req"),
    F.from_json(F.col("response"), "array<struct<answer:string>>").alias("resp"),
)

# COMMAND ----------

# Normaliza roles e extrai respostas válidas
df_norm = df_parse.select(
    F.transform(
        F.col("req.messages"),
        lambda x: F.struct(
            F.when(x.role == "assistant", "model").otherwise(x.role).alias("role"),
            x.content.alias("content"),
        ),
    ).alias("msgs_req"),
    F.element_at(F.col("resp"), 1).answer.alias("model_answer"),
).filter(
    # Filtra apenas registros com dados válidos
    F.col("msgs_req").isNotNull()
    & (F.size(F.col("msgs_req")) > 0)
    & F.col("model_answer").isNotNull()
    & (F.col("model_answer") != "")
    & (F.trim(F.col("model_answer")) != "")
)

# Remove mensagens com role "system" (não suportado no fine-tuning)
df_no_system = (
    df_norm.withColumn(
        "msgs_req_filtered", F.filter(F.col("msgs_req"), lambda x: x.role != "system")
    )
    .filter(F.size(F.col("msgs_req_filtered")) > 0)
    .select(F.col("msgs_req_filtered").alias("msgs_req"), F.col("model_answer"))
)

# COMMAND ----------

# Monta estrutura final para fine-tuning (formato {"role": "X", "parts": [{"text": "..."}]})
df_final = df_norm.select(
    F.to_json(
        F.struct(
            F.array_union(
                F.transform(
                    F.col("msgs_req"),
                    lambda x: F.struct(
                        x.role.alias("role"),
                        F.array(F.struct(x.content.alias("text"))).alias("parts"),
                    ),
                ),
                F.array(
                    F.struct(
                        F.lit("model").alias("role"),
                        F.array(F.struct(F.col("model_answer").alias("text"))).alias(
                            "parts"
                        ),
                    )
                ),
            ).alias("contents")
        )
    ).alias("line_json")
)

# COMMAND ----------


# Verifica roles consecutivos na conversa
df_complete_conversation = df_no_system.withColumn(
    "complete_messages",
    F.array_union(
        F.col("msgs_req"),
        F.array(
            F.struct(
                F.lit("model").alias("role"), F.col("model_answer").alias("content")
            )
        ),
    ),
)

df_with_all_roles = df_complete_conversation.withColumn(
    "all_roles", F.transform(F.col("complete_messages"), lambda x: x.role)
)

df_role_check = df_with_all_roles.withColumn(
    "has_consecutive_roles",
    F.when(
        F.size(F.col("all_roles")) >= 2,
        F.exists(
            F.sequence(F.lit(1), F.size(F.col("all_roles")) - 1),
            lambda i: (
                F.element_at(F.col("all_roles"), i)
                == F.element_at(F.col("all_roles"), i + 1)
            ),
        ),
    ).otherwise(F.lit(False)),
).filter(~F.col("has_consecutive_roles"))

# COMMAND ----------

# Build user messages
user_msgs = F.transform(
    F.col("msgs_req"),
    lambda x: F.struct(
        x.role.alias("role"),
        F.array(F.struct(x.content.alias("text"))).alias("parts"),
    ),
)

# Build model answer
model_msg = F.array(
    F.struct(
        F.lit("model").alias("role"),
        F.array(F.struct(F.col("model_answer").alias("text"))).alias("parts"),
    )
)

# Union and wrap
contents = F.array_union(user_msgs, model_msg)
final_struct = F.struct(contents.alias("contents"))

# Convert to JSON and filter
df_final = df_role_check.select(F.to_json(final_struct).alias("line_json")).filter(
    F.col("line_json").isNotNull()
    & (F.col("line_json") != "")
    & (F.col("line_json") != "{}")
    & (F.col("line_json") != '{"contents":[]}')
    & F.col("line_json").contains("contents")
)

# COMMAND ----------

# Amostra dos resultados
print("Amostra dos resultados:")
df_final.limit(3).display()

# COMMAND ----------

# Split em training e validation
total_records = df_final.count()
training_count = total_records - VALIDATION_SIZE

print(f"Total: {total_records:,} registros")
print(f"Training: {training_count:,} | Validation: {VALIDATION_SIZE:,}")

# Embaralha e divide os dados
df_shuffled = df_final.orderBy(F.rand(seed=RANDOM_SEED)).withColumn(
    "row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
)

df_validation = df_shuffled.filter(F.col("row_num") <= VALIDATION_SIZE).select(
    "line_json"
)
df_training = df_shuffled.filter(F.col("row_num") > VALIDATION_SIZE).select("line_json")

# COMMAND ----------

# Salva os datasets
df_training.coalesce(1).write.mode("overwrite").text(training_path)
df_validation.coalesce(1).write.mode("overwrite").text(validation_path)

print(f"Training salvo em: {training_path}")
print(f"Validation salvo em: {validation_path}")

# TODO fazer processo de fine tuning de forma programática, hoje estamos usando o Vertex AI via console do Google
# https://cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-use-supervised-tuning
