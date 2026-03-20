import json

import pyspark.sql.types as T


# TODO: adicionar type hints em get_pbm(spark: SparkSession) -> DataFrame e from_rdd(rdd) -> DataFrame.
# TODO: s3Path hardcoded para prod — parametrizar com stage.
def get_pbm(spark):
    s3Path = 's3://maggu-datalake-prod/1-raw-layer/fontes_maggu/pbm/*.json'
    return from_rdd(spark.sparkContext.wholeTextFiles(s3Path))


entry_schema = T.StructType(
    [
        T.StructField("active_ingredient", T.StringType()),
        T.StructField("manufacturer_image", T.StringType()),
        T.StructField("manufacturer_name", T.StringType()),
        T.StructField("medicine_ean", T.StringType()),
        T.StructField("medicine_full_name", T.StringType()),
        T.StructField("medicine_id", T.StringType()),
        T.StructField("medicine_name", T.StringType()),
        T.StructField("medicine_note", T.StringType()),
        T.StructField("medicine_presentation", T.StringType()),
        T.StructField("patient_discount", T.StringType()),
        T.StructField("pbm_image", T.StringType()),
        T.StructField("pbm_link", T.StringType()),
        T.StructField("pbm_name", T.StringType()),
        T.StructField("platform_name", T.StringType()),
        T.StructField("replace_discount", T.StringType()),
    ]
)


def from_rdd(rdd):
    entries = rdd.flatMap(lambda p: json.loads(p[1]))

    return (
        entries.toDF(entry_schema)
        .dropna(subset='medicine_ean')
        .dropDuplicates(['medicine_ean'])
    )
