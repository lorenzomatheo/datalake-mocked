from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

analytics_chat_milvus_s3 = StructType(
    [
        StructField("pergunta_id", StringType(), True),
        StructField("atendente_id", StringType(), True),
        StructField("chat_date", DateType(), True),
        StructField("msg_index", IntegerType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("pergunta", StringType(), True),
        StructField("topico", StringType(), True),
        StructField("role", StringType(), True),
        StructField("rn", IntegerType(), True),
        StructField("resposta", StringType(), True),
        StructField("eh_resposta_padrao", BooleanType(), True),
        StructField("data_extracao", TimestampType(), True),
    ]
)
