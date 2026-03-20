# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

spark = SparkSession.builder.appName("Calendario").getOrCreate()

start_date = "2022-01-01"
end_date = (F.current_date() + F.expr("INTERVAL 1 YEAR")).cast("string")

df_dates = spark.sql(
    f"""
  SELECT sequence(to_date('{start_date}'), date_add(current_date(), 365), interval 1 day) as date_array
"""
).select(F.explode(F.col("date_array")).alias("data"))

dias_semana = {
    "Sunday": "Domingo",
    "Monday": "Segunda-feira",
    "Tuesday": "Terça-feira",
    "Wednesday": "Quarta-feira",
    "Thursday": "Quinta-feira",
    "Friday": "Sexta-feira",
    "Saturday": "Sábado",
}

meses_ano = {
    "January": "Janeiro",
    "February": "Fevereiro",
    "March": "Março",
    "April": "Abril",
    "May": "Maio",
    "June": "Junho",
    "July": "Julho",
    "August": "Agosto",
    "September": "Setembro",
    "October": "Outubro",
    "November": "Novembro",
    "December": "Dezembro",
}

# Criar mapas para tradução no PySpark
translate_dia_semana = F.create_map([F.lit(x) for x in sum(dias_semana.items(), ())])
translate_mes = F.create_map([F.lit(x) for x in sum(meses_ano.items(), ())])

# Adicionar colunas ao DataFrame
df_calendar = (
    df_dates.withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn(
        "dia_da_semana", translate_dia_semana[F.date_format("data", "EEEE")]
    )  # Traduzir dia da semana
    .withColumn(
        "nome_mes", translate_mes[F.date_format("data", "MMMM")]
    )  # Traduzir mês
    .withColumn("numero_dia_semana", F.dayofweek("data"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("semana_do_ano", F.weekofyear("data"))
    .withColumn(
        "semana_do_ano_str",
        F.concat(F.lit("Semana "), F.weekofyear("data").cast("string")),
    )  # String da semana
    .withColumn(
        "eh_final_de_semana",
        F.when(F.dayofweek("data").isin(7, 1), True).otherwise(False),
    )
    .withColumn(
        "eh_dia_util", F.when(F.dayofweek("data").isin(7, 1), False).otherwise(True)
    )
)


# COMMAND ----------

(
    df_calendar.withColumn("data_atualizacao", F.current_timestamp())
    .write.mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("production.analytics.calendario")
)
