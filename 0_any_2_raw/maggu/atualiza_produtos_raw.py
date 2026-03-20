# Databricks notebook source
# MAGIC %pip install bs4 pandas openpyxl

# COMMAND ----------

from functools import reduce

import pyspark.sql.functions as F
from delta import DeltaTable

from maggulake.environment import CopilotTable, DatabricksEnvironmentBuilder, Table
from maggulake.mappings.anvisa import AnvisaParser
from maggulake.mappings.anvisa_medicamentos import AnvisaMedicamentosParser
from maggulake.mappings.buscador_bigdatacorp import BuscadorBigDataCorp
from maggulake.mappings.buscador_bluesoft import BuscadorBluesoft
from maggulake.mappings.buscador_portal_obm import BuscadorPortalObm
from maggulake.mappings.buscador_produtos import BuscadorProdutos
from maggulake.mappings.ConsultaRemedios import CrParser
from maggulake.mappings.farmarcas import FarmarcasParser
from maggulake.mappings.iqvia import IqviaParser
from maggulake.mappings.minas_mais import MinasMaisParser
from maggulake.mappings.produtos_genericos import ProdutosGenericosParser
from maggulake.mappings.RD import RdParser
from maggulake.mappings.sara import SaraParser
from maggulake.tables import Raw
from maggulake.utils.valores_invalidos import (
    COLUNAS_PERMITIR_NUMERICO,
    nullifica_coluna_se_valor_invalido,
)

env = DatabricksEnvironmentBuilder.build("atualiza_produtos_raw", dbutils)

# COMMAND ----------

spark = env.spark
stage = env.settings.name_short

produtos_raw = env.table(Raw.produtos)
dt = DeltaTable.forName(spark, Table.produtos_raw.value)
schema = dt.toDF().drop("gerado_em").schema

# COMMAND ----------

# MAGIC %md ### Pega fontes da verdade

# COMMAND ----------

rd_table = env.table(Raw.rd)
rd_parser = RdParser(spark, stage)

rd_df = rd_parser.to_produtos_raw(rd_table.spark_df).cache()

# COMMAND ----------

consulta_remedios_marcas_table = env.table(Raw.consulta_remedios_marcas)
consulta_remedios_apresentacoes_table = env.table(Raw.consulta_remedios_apresentacoes)
cr_parser = CrParser(spark)

consulta_remedios_df = cr_parser.to_produtos_raw(
    consulta_remedios_marcas_table.spark_df,
    consulta_remedios_apresentacoes_table.spark_df,
).cache()

# COMMAND ----------

sara_table = env.table(Raw.sara)
sara_parser = SaraParser(spark)

sara_df = sara_parser.to_produtos_raw(sara_table.spark_df).cache()

# COMMAND ----------

anvisa_table = env.table(Raw.anvisa)
anvisa_parser = AnvisaParser(spark)

anvisa_df = anvisa_parser.to_produtos_raw(anvisa_table.spark_df).cache()

# COMMAND ----------

anvisa_medicamentos_precos_table = env.table(Raw.anvisa_medicamentos_precos)
anvisa_medicamentos_restricoes_table = env.table(Raw.anvisa_medicamentos_restricoes)
anvisa_medicamentos_parser = AnvisaMedicamentosParser(spark)

anvisa_medicamentos_df = anvisa_medicamentos_parser.to_produtos_raw(
    anvisa_medicamentos_precos_table.spark_df,
    anvisa_medicamentos_restricoes_table.spark_df,
).cache()

# COMMAND ----------

farmarcas_table = env.table(Raw.farmarcas)
farmarcas_parser = FarmarcasParser(spark)

farmarcas_df = farmarcas_parser.to_produtos_raw(farmarcas_table.spark_df).cache()

# COMMAND ----------

iqvia_table = env.table(Raw.iqvia)
iqvia_parser = IqviaParser(spark)

iqvia_df = iqvia_parser.to_produtos_raw(iqvia_table.spark_df).cache()

# COMMAND ----------

# arquivo compartilhado pela rede Minas Mais via email
minas_mais_table = env.table(Raw.minas_mais)
minas_mais_parser = MinasMaisParser(spark)

minas_mais_df = minas_mais_parser.to_produtos_raw(minas_mais_table.spark_df).cache()

# COMMAND ----------

produtos_genericos_parser = ProdutosGenericosParser(spark)

produtos_genericos_df = produtos_genericos_parser.to_produtos_raw(
    spark.read.table(Table.produtos_genericos.value),
).cache()

# COMMAND ----------

# MAGIC %md ### Ordena fontes da verdade e atualiza produtos_raw

# COMMAND ----------


def junta_fontes(prioridades_por_coluna=None, **fontes):
    """
    Unifica múltiplas fontes de dados (DataFrames) em um único DataFrame, consolidando colunas
    com base em uma prioridade definida.

    O processo de unificação segue estas etapas:
    1. Identifica todos os EANs únicos presentes em qualquer uma das fontes.
    2. Realiza um LEFT JOIN de cada fonte com a lista de EANs.
    3. Para cada coluna do schema (ex: nome, fabricante, etc.), aplica uma estratégia de `coalesce`
       para escolher o valor não nulo seguindo a ordem de prioridade.

    A ordem de prioridade padrão é a ordem em que os argumentos são passados para a função (**fontes).
    No entanto, é possível sobrescrever essa ordem para colunas específicas usando `prioridades_por_coluna`.

    Args:
        prioridades_por_coluna (dict, optional): Dicionário onde a chave é o nome da coluna e o valor
            é o nome da fonte (str) ou uma lista de fontes (list[str]) que devem ter prioridade.
            Exemplo:
            {
                "fabricante": "farmarcas",  # 'farmarcas' será a primeira, seguida pelas outras na ordem padrão
                "marca": ["rd", "iqvia"]    # 'rd' em 1º, 'iqvia' em 2º, seguidas pelas outras
            }
        **fontes (DataFrame): DataFrames de origem passados como argumentos nomeados.
            Os nomes dos argumentos (ex: rd, cr, farmarcas) são usados como identificadores das fontes.
            Exemplo: junta_fontes(rd=df_rd, cr=df_cr)

    Returns:
        DataFrame: DataFrame unificado com as colunas consolidadas.
    """
    if prioridades_por_coluna is None:
        prioridades_por_coluna = {}

    # 1. Pega todos os EANs distintos de todas as fontes
    todos_eans = (
        reduce(
            lambda df1, df2: df1.union(df2.select("ean")),
            [df.select("ean") for df in fontes.values()],
        )
        .distinct()
        .cache()
    )

    # 2. Faz join com todas as fontes (renomeando colunas)
    df_unido = todos_eans

    for nome_fonte, df in fontes.items():
        # Garante schema e renomeia colunas para {source}_{col}
        # Exceto EAN que usamos para join
        colunas_renomeadas = [
            F.col(c).alias(f"{nome_fonte}_{c}") for c in schema.names if c != 'ean'
        ]

        # .to(schema) garante que colunas faltantes sejam null e tipos batam
        df_preparado = df.to(schema).select('ean', *colunas_renomeadas)

        df_unido = df_unido.join(df_preparado, "ean", "left")

    # 3. Coalesce final baseado na prioridade
    colunas_finais = [F.col("ean")]

    for col in schema.names:
        if col == 'ean':
            continue

        # Ordem padrão = ordem dos argumentos
        prioridade_fontes = list(fontes.keys())

        # Aplica override se configurado
        if col in prioridades_por_coluna:
            config_prioridade = prioridades_por_coluna[col]

            # Normaliza para lista se for string única
            fontes_prioritarias = (
                [config_prioridade]
                if isinstance(config_prioridade, str)
                else config_prioridade
            )

            # Valida se todas as fontes existem
            fontes_invalidas = [
                f for f in fontes_prioritarias if f not in prioridade_fontes
            ]
            if fontes_invalidas:
                opcoes_validas = list(fontes.keys())
                raise ValueError(
                    f"Fontes {fontes_invalidas} inválidas para a coluna '{col}'. "
                    f"Opções válidas: {opcoes_validas}"
                )

            # Reorganiza: Fontes Prioritárias + (Resto - Prioritárias)
            fontes_restantes = [
                f for f in prioridade_fontes if f not in fontes_prioritarias
            ]
            prioridade_fontes = fontes_prioritarias + fontes_restantes

        # Monta coalesce (nullifica valores inválidos para que o coalesce pule
        # corretamente fontes com "N/i", "", etc.)
        tipo_coluna = schema[col].dataType
        numerico = col in COLUNAS_PERMITIR_NUMERICO
        colunas_coalesce = [
            nullifica_coluna_se_valor_invalido(
                F.col(f"{src}_{col}"), numerico, tipo=tipo_coluna
            )
            for src in prioridade_fontes
        ]
        colunas_finais.append(F.coalesce(*colunas_coalesce).alias(col))

    return df_unido.select(*colunas_finais)


# COMMAND ----------

nao_medicamentos = (
    junta_fontes(
        prioridades_por_coluna={"fabricante": "farmarcas"},
        rd=rd_df.filter("eh_medicamento = false"),
        cr=consulta_remedios_df.filter("eh_medicamento = false"),
        farmarcas=farmarcas_df.filter("eh_medicamento = false"),
        iqvia=iqvia_df.filter("eh_medicamento = false"),
        minas_mais=minas_mais_df.drop('eans_alternativos'),
    )
    .withColumn("gerado_em", F.current_timestamp())
    .filter("fonte != 'MINAS_MAIS'")
    .dropDuplicates(["ean"])
)

# COMMAND ----------

medicamentos = (
    junta_fontes(
        prioridades_por_coluna={
            # Ordem definida por quão direto e curado é o dado.
            "eans_alternativos": ["anvisa_medicamentos"],
            "nome": [
                "anvisa_medicamentos",
                "anvisa",
                "sara",
                "rd",
                "cr",
            ],
            "fabricante": [
                "farmarcas",
                "sara",
                "iqvia",
                "anvisa_medicamentos",
                "anvisa",
                "produtos_genericos",
                "minas_mais",
            ],
            "marca": ["rd", "cr", "anvisa"],
            "descricao": ["cr", "rd", "sara"],
            "tipo_medicamento": ["anvisa_medicamentos", "anvisa", "sara"],
            "eh_otc": ["farmarcas", "rd", "anvisa_medicamentos"],
            "eh_tarjado": [
                "anvisa",
                "anvisa_medicamentos",
                "sara",
                "cr",
                "rd",
            ],
            "tarja": ["anvisa_medicamentos", "sara", "cr", "rd"],
            "eh_controlado": [
                "anvisa",
                "anvisa_medicamentos",
                "sara",
                "cr",
                "rd",
            ],
            "principio_ativo": [
                "anvisa_medicamentos",
                "anvisa",
                "iqvia",
                "produtos_genericos",
                "farmarcas",
                "rd",
            ],
            "dosagem": ["sara", "produtos_genericos", "rd"],
            "via_administracao": ["sara", "cr"],
            "temperatura_armazenamento": ["sara", "cr"],
            "categorias": ["rd", "cr", "iqvia"],
            "idade_recomendada": ["anvisa_medicamentos", "sara", "cr"],
        },
        anvisa_medicamentos=anvisa_medicamentos_df,
        anvisa=anvisa_df,
        farmarcas=farmarcas_df.filter("eh_medicamento = true"),
        cr=consulta_remedios_df.filter("eh_medicamento = true"),
        sara=sara_df,
        produtos_genericos=produtos_genericos_df,
        rd=rd_df.filter("eh_medicamento = true"),
        iqvia=iqvia_df.filter("eh_medicamento = true"),
        minas_mais=minas_mais_df.drop('eans_alternativos'),
    )
    .withColumn("gerado_em", F.current_timestamp())
    .filter("fonte != 'MINAS_MAIS'")
    .dropDuplicates(["ean"])
)

# COMMAND ----------

produtos_raw_df = medicamentos.unionByName(
    nao_medicamentos.join(medicamentos.select("ean"), "ean", "left_anti"),
    allowMissingColumns=True,
)

produtos_raw.merge(produtos_raw_df, "ean")

# COMMAND ----------

# MAGIC %md ### Completa com APIs externas

# COMMAND ----------

produtos_raw = dt.toDF()
produtos_loja = env.table(CopilotTable.produtos_loja)

buscador = BuscadorProdutos(
    [BuscadorPortalObm(env), BuscadorBigDataCorp(env), BuscadorBluesoft(env)]
)

# COMMAND ----------

eans_faltantes = (
    produtos_loja.select("ean")
    .distinct()
    .join(F.broadcast(produtos_raw.select("ean")), "ean", "leftanti")
)

# COMMAND ----------

produtos_externos = buscador.pesquisa_eans(eans_faltantes)

# COMMAND ----------

produtos_externos.write.mode("append").saveAsTable(Table.produtos_raw.value)
