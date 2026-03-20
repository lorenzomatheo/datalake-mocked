# Databricks notebook source
from maggulake.environment import DatabricksEnvironmentBuilder
from maggulake.mappings.anvisa import AnvisaParser
from maggulake.mappings.anvisa_medicamentos import AnvisaMedicamentosParser
from maggulake.mappings.ConsultaRemedios import CrParser
from maggulake.mappings.farmarcas import FarmarcasParser
from maggulake.mappings.iqvia import IqviaParser, s3_file_iqvia
from maggulake.mappings.minas_mais import MinasMaisParser
from maggulake.mappings.RD import RdParser
from maggulake.tables import Raw

env = DatabricksEnvironmentBuilder.build(
    "atualiza_fontes_da_verdade",
    dbutils,
)

spark = env.spark

# COMMAND ----------

rd_parser = RdParser(spark, env.settings.name_short)

rd_df = rd_parser.to_raw_df()

# COMMAND ----------

rd_table = env.table(Raw.rd)
rd_table.write(rd_df, mode="overwrite")

# COMMAND ----------

cr_parser = CrParser(spark)

marcas_df, apresentacoes_df = cr_parser.to_raw_dfs(env.settings.name_short)

# COMMAND ----------

consulta_remedios_marcas_table = env.table(Raw.consulta_remedios_marcas)
consulta_remedios_marcas_table.write(marcas_df, mode="overwrite")

# COMMAND ----------

consulta_remedios_apresentacoes_table = env.table(Raw.consulta_remedios_apresentacoes)
consulta_remedios_apresentacoes_table.write(apresentacoes_df, mode="overwrite")

# COMMAND ----------

anvisa_df = AnvisaParser(spark).to_raw_df()

# COMMAND ----------

anvisa_table = env.table(Raw.anvisa)
anvisa_table.write(anvisa_df, mode="overwrite")

# COMMAND ----------

anvisa_medicamentos_parser = AnvisaMedicamentosParser(spark)

# COMMAND ----------

anvisa_medicamentos_precos_df = anvisa_medicamentos_parser.to_raw_precos_df()

# COMMAND ----------

anvisa_medicamentos_precos_table = env.table(Raw.anvisa_medicamentos_precos)
anvisa_medicamentos_precos_table.write(anvisa_medicamentos_precos_df, mode="overwrite")

# COMMAND ----------

anvisa_medicamentos_restricoes_df = anvisa_medicamentos_parser.to_raw_restricoes_df()

# COMMAND ----------

anvisa_medicamentos_restricoes_table = env.table(Raw.anvisa_medicamentos_restricoes)
anvisa_medicamentos_restricoes_table.write(
    anvisa_medicamentos_restricoes_df, mode="overwrite"
)

# COMMAND ----------

farmarcas_df = FarmarcasParser(spark).to_raw_df()

# COMMAND ----------

farmarcas_table = env.table(Raw.farmarcas)
farmarcas_table.write(farmarcas_df, mode="overwrite")

# COMMAND ----------

iqvia_df = IqviaParser(spark).to_raw_df(env.full_s3_path(s3_file_iqvia))

# COMMAND ----------

iqvia_table = env.table(Raw.iqvia)
iqvia_table.write(iqvia_df, mode="overwrite")

# COMMAND ----------

minas_mais_df = MinasMaisParser(spark).to_raw_df()

# COMMAND ----------

minas_mais_table = env.table(Raw.minas_mais)
minas_mais_table.write(minas_mais_df, mode="overwrite")
