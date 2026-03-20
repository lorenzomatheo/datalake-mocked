# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook responsável por gerar ranking dos atendentes por rede, por missão e rodada

# COMMAND ----------

# Às 9h de segunda, o ranking dos atendentes da rodada anterior é exibido na aba destaques na Mini Maggu.
# Todos os lendas vivas são exibidos (por rede e por missão).
# Caso o número de lendas vivas seja inferior a 10, os posições restantes são preenchidas por ordem decrescente de pontos.

# COMMAND ----------

import json
from datetime import date, timedelta
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from maggulake.io.postgres import PostgresAdapter

USER = dbutils.secrets.get(scope='postgres', key='POSTGRES_USER')
PASSWORD = dbutils.secrets.get(scope='postgres', key='POSTGRES_PASSWORD')
postgres = PostgresAdapter("prod", USER, PASSWORD, utilizar_read_replica=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Dataframe inicial com `id do atendente`, `pontos`, `nível atingido`, `conta` e `loja`

# COMMAND ----------

# Conversão R$ 1,00 = 100 magguletes
REAL_EM_MAGGULETES = 100

# Alguns atendentes podem solicitar que seus nomes não sejam exibidos na aba de destaques
USERNAMES_EXCLUIDOS = ["HELTONBALLEJODOSSANTOS-14100"]

# Determina rodada passada
# NOTE: Esse código assume que sempre há pelo menos 2 rodadas criadas
w_rodada = Window.orderBy(F.col("criado_em").desc(), F.col("id").desc())
rodada_passada = (
    postgres.read_table(spark, "gameficacao_rodada")
    .select("id", "data_inicio", "data_termino", "criado_em")
    .withColumn("rn", F.row_number().over(w_rodada))
    .filter(F.col("rn") == 2)
    .select("id", "data_inicio", "data_termino")
    .first()
)

id_rodada_passada = rodada_passada["id"]
data_inicio_rodada = rodada_passada["data_inicio"]
data_termino_rodada = rodada_passada["data_termino"]

# Determina pontos da rodada passada
df_pontos_rodada_passada = (
    postgres.read_table(spark, "gameficacao_saldodepontos")
    .filter(F.col("rodada_id") == id_rodada_passada)
    .select(
        "jogador_id",
        "missao_id",
        "pontos_totais",
        "nivel_atual",
        "criado_em",
        "atualizado_em",
    )
    .filter(F.col("jogador_id").isNotNull() & F.col("missao_id").isNotNull())
)

# Determina nome da missão e moedas a partir do id
df_missao = postgres.read_table(spark, "gameficacao_missao").select(
    F.col("id").alias("missao_id"),
    F.col("nome").alias("missao_nome"),
    F.coalesce(F.col("moedas_desbloqueadas_nivel_1"), F.lit(0)).alias("m1"),
    F.coalesce(F.col("moedas_desbloqueadas_nivel_2"), F.lit(0)).alias("m2"),
    F.coalesce(F.col("moedas_desbloqueadas_nivel_3"), F.lit(0)).alias("m3"),
    F.coalesce(F.col("moedas_desbloqueadas_nivel_4"), F.lit(0)).alias("m4"),
    F.coalesce(F.col("moedas_desbloqueadas_nivel_5"), F.lit(0)).alias("m5"),
)

df_pontos_rodada_passada = df_pontos_rodada_passada.join(
    F.broadcast(df_missao), on="missao_id", how="left"
)

# Filtra por atendentes que pontuaram na rodada passada
jogadores_rodada = df_pontos_rodada_passada.select("jogador_id").distinct()
df_atendentes = (
    postgres.read_table(spark, "atendentes_atendente")
    .select(
        F.col("usuario_django_id").alias("jogador_id"),
        "conta_id",
        "first_name",
        "last_name",
        "username",
        "id",
    )
    .join(jogadores_rodada, on="jogador_id", how="left_semi")
    .filter(~F.col("username").isin(USERNAMES_EXCLUIDOS))
)

# Descobre de qual loja cada atendente faz parte
# NOTE: A coluna loja estava prevista mas foi retirada. Continua aqui caso for necessário no futuro.
df_atendente_loja_mapa = (
    postgres.read_table(spark, "atendentes_atendenteloja")
    .select(F.col("atendente_id").alias("id"), "loja_id")
    .join(
        postgres.read_table(spark, "contas_loja").select(
            F.col("id").alias("loja_id"), "codigo_loja"
        ),
        on="loja_id",
        how="left",
    )
    .select("id", "loja_id", "codigo_loja")
)

# Enriquece atendentes com loja_id e codigo_loja
# NOTE: A coluna loja estava prevista mas foi retirada. Continua aqui caso for necessário no futuro.
df_atendentes_enriquecido = (
    df_atendentes.alias("a")
    .join(F.broadcast(df_atendente_loja_mapa).alias("m"), on="id", how="left")
    .select(
        "a.id",
        "a.jogador_id",
        "a.conta_id",
        "a.first_name",
        "a.last_name",
        "a.username",
        "m.loja_id",
        "m.codigo_loja",
    )
)

# Join com pontos por atendente, conta, loja e missão
df_atendentes_conta_loja = (
    df_pontos_rodada_passada.alias("p")
    .join(df_atendentes_enriquecido.alias("a"), on="jogador_id", how="left")
    .select(
        "p.jogador_id",
        "p.missao_id",
        "p.missao_nome",
        "p.pontos_totais",
        "p.nivel_atual",
        "p.criado_em",
        "p.atualizado_em",
        "a.conta_id",
        "a.first_name",
        "a.last_name",
        "a.username",
        "a.loja_id",
        "a.codigo_loja",
        "p.m1",
        "p.m2",
        "p.m3",
        "p.m4",
        "p.m5",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Formata nome do atendente, número da loja e nível atingido

# COMMAND ----------


def add_nome_completo(df):
    """Monta nome priorizando first/last e username de fallback"""
    first = F.trim(F.coalesce(F.col("first_name"), F.lit("")))
    last = F.trim(F.coalesce(F.col("last_name"), F.lit("")))

    nome = F.coalesce(
        F.when((first == "") & (last == ""), F.lit(None))
        .when(F.lower(first) == F.lower(last), first)
        .when(last == "", first)
        .when(first == "", last)
        .otherwise(F.concat_ws(" ", first, last)),
        F.trim(F.col("username")),
    )
    nome_norm = F.initcap(F.regexp_replace(nome, r"\s+", " "))
    return (
        df.withColumn("nome_completo", nome_norm)
        .withColumn("nome_ordenavel", F.lower(F.col("nome_completo")))
        .drop("first_name", "last_name")
    )


def add_codigo_loja_fmt(df):
    """Transforma codigo_loja em formato 'Loja X' ou usa codigo_loja como fallback"""
    # NOTE: A coluna loja estava prevista mas foi retirada. Continua aqui caso for necessário no futuro.
    digits = F.regexp_extract(F.col("codigo_loja").cast("string"), r"(\d+)", 1)
    cod_fmt = F.when(digits != "", F.concat(F.lit("Loja "), digits)).otherwise(
        F.col("codigo_loja").cast("string")
    )
    return df.withColumn("codigo_loja_fmt", cod_fmt).drop("codigo_loja", "loja_id")


def add_nivel_cols(df):
    """Normaliza nivel_atua, cria label e nível numérico"""
    norm = F.regexp_replace(
        F.lower(F.coalesce(F.col("nivel_atual"), F.lit("nenhum"))), "_", "-"
    )
    mapa = F.create_map(
        F.lit("nenhum"),
        F.lit("-"),
        F.lit("nivel-1"),
        F.lit("Esquenta"),
        F.lit("nivel-2"),
        F.lit("Iniciante"),
        F.lit("nivel-3"),
        F.lit("Intermediário"),
        F.lit("nivel-4"),
        F.lit("Avançado"),
        F.lit("nivel-5"),
        F.lit("Lenda Viva"),
    )
    nivel_num = F.coalesce(
        F.regexp_extract(norm, r"nivel-(\d+)", 1).cast("int"), F.lit(0)
    )
    nivel_label = mapa[norm]
    return df.withColumn("nivel_label", nivel_label).withColumn("nivel_num", nivel_num)


def moedas_acumuladas(coluna_nivel, *colunas_moedas):
    moedas_array = F.array(*[F.coalesce(c, F.lit(0)) for c in colunas_moedas])

    total_niveis = len(colunas_moedas)

    # Garante que o nível fique entre 0 e total_niveis
    nivel_limitado = F.least(F.greatest(coluna_nivel, F.lit(0)), F.lit(total_niveis))
    return F.aggregate(
        F.slice(moedas_array, 1, nivel_limitado),
        F.lit(0),
        lambda acumulado, valor: acumulado + valor,
    )


# COMMAND ----------

df_atendentes_conta_loja_fmt = (
    df_atendentes_conta_loja.transform(add_nome_completo)
    .transform(add_codigo_loja_fmt)
    .transform(add_nivel_cols)
    .withColumn(
        "total_moedas",
        moedas_acumuladas(
            F.col("nivel_num"),
            F.col("m1"),
            F.col("m2"),
            F.col("m3"),
            F.col("m4"),
            F.col("m5"),
        ),
    )
    .drop("id")
    .select(
        "nome_completo",
        "nome_ordenavel",
        "nivel_label",
        "nivel_num",
        "total_moedas",
        "codigo_loja_fmt",
        "pontos_totais",
        "jogador_id",
        "nivel_atual",
        "conta_id",
        "missao_id",
        "missao_nome",
        "criado_em",
        "atualizado_em",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Extrai os melhores colocados por conta

# COMMAND ----------

# Base: conta_id, missao_id, nome, pontos_totais, nivel_num, total_moedas
df_base_pontos = df_atendentes_conta_loja_fmt.select(
    "conta_id",
    "missao_id",
    "missao_nome",
    "jogador_id",
    "pontos_totais",
    "total_moedas",
    "nome_completo",
    "nome_ordenavel",
    "nivel_atual",
    "nivel_label",
    "nivel_num",
    "codigo_loja_fmt",
).dropDuplicates(["conta_id", "missao_id", "jogador_id"])

# Todos os lendas vivas por conta e missão
df_nivel5 = df_base_pontos.filter(F.col("nivel_num") == 5).select(
    "nome_completo",
    "nome_ordenavel",
    "nivel_label",
    "total_moedas",
    "codigo_loja_fmt",
    "pontos_totais",
    "conta_id",
    "missao_id",
    "missao_nome",
    "jogador_id",
)

# Determina a quantidade de lendas vivas por conta e missão
df_contagem_nivel5 = df_nivel5.groupBy("conta_id", "missao_id").agg(
    F.countDistinct("jogador_id").alias("qtd_nivel5")
)

# Restante ordenado por pontos
df_candidatos_base = (
    df_base_pontos.filter(F.col("nivel_num") != 5)
    .filter(F.col("total_moedas") > 0)
    .join(df_contagem_nivel5, on=["conta_id", "missao_id"], how="left")
    .withColumn("qtd_nivel5", F.coalesce(F.col("qtd_nivel5"), F.lit(0)))
    .withColumn("faltantes", F.greatest(F.lit(10) - F.col("qtd_nivel5"), F.lit(0)))
    .select(
        "nome_completo",
        "nome_ordenavel",
        "nivel_label",
        "total_moedas",
        "codigo_loja_fmt",
        "pontos_totais",
        "conta_id",
        "missao_id",
        "missao_nome",
        "jogador_id",
        "faltantes",
    )
)

# Desempate: pontos conquistados e ordem alfabética
janela_candidatos = Window.partitionBy("conta_id", "missao_id").orderBy(
    F.col("pontos_totais").desc(),
    F.col("nome_ordenavel").asc(),
    F.col("jogador_id").asc(),
)

df_candidatos_rankeados = df_candidatos_base.withColumn(
    "posicao", F.row_number().over(janela_candidatos)
)

df_restantes_selecionados = df_candidatos_rankeados.filter(
    F.col("posicao") <= F.col("faltantes")
).select(
    "nome_completo",
    "nome_ordenavel",
    "nivel_label",
    "total_moedas",
    "codigo_loja_fmt",
    "pontos_totais",
    "conta_id",
    "missao_id",
    "missao_nome",
    "jogador_id",
)

# Todos lendas vivas + complemento até 10 (se necessário)
df_destaques = df_nivel5.unionByName(df_restantes_selecionados).filter(
    F.col("conta_id").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Monta mensagem usando HTML

# COMMAND ----------

# Ranking local por conta/missão
window_rank = Window.partitionBy("conta_id", "missao_id").orderBy(
    F.col("pontos_totais").desc(), F.col("nome_ordenavel").asc()
)

df_ranked_local = df_destaques.withColumn(
    "pos", F.row_number().over(window_rank)
).select(
    "conta_id",
    "missao_id",
    "missao_nome",
    F.col("pos"),
    F.col("nome_completo").alias("Nome"),
    F.col("nivel_label").alias("Nível"),
    # NOTE: reativar se quiser exibir coluna Loja
    # F.col("codigo_loja_fmt").alias("Loja"),
    F.when(F.col("total_moedas") == 0, F.lit("-"))
    .otherwise(
        F.concat(
            F.lit("R$ "),
            ((F.col("total_moedas") / REAL_EM_MAGGULETES).cast("int")).cast("string"),
        )
    )
    .alias("Prêmio recebido"),
)

# Cria linhas individualmente
df_linhas = df_ranked_local.select(
    "conta_id",
    "missao_id",
    "missao_nome",
    F.struct(
        F.col("pos").alias("pos"),
        F.col("Nome").alias("Nome"),
        F.col("Nível").alias("Nível"),
        # NOTE: reativar se quiser exibir coluna Loja
        # F.col("Loja").alias("Loja"),
        F.col("Prêmio recebido").alias("Prêmio recebido"),
    ).alias("linha"),
)

# Agrega por grupo e ordena as linhas
df_agregado = df_linhas.groupBy("conta_id", "missao_id", "missao_nome").agg(
    F.array_sort(F.collect_list(F.col("linha"))).alias("linhas")
)

# Transforma cada struct em <tr>...</tr>
rows_html = F.expr("""
transform(linhas, x ->
  concat(
    '<tr>',
      '<td style="border:1px solid #ccc; padding:6px; text-align:center;">', cast(x.pos as string), '</td>',
      '<td style="border:1px solid #ccc; padding:6px; text-align:center;">', x.Nome, '</td>',
      '<td style="border:1px solid #ccc; padding:6px; text-align:center;">', x.`Nível`, '</td>',
      -- NOTE: reativar se quiser exibir coluna Loja
      -- '<td style="border:1px solid #ccc; padding:6px; text-align:center;">', x.Loja, '</td>',
      '<td style="border:1px solid #ccc; padding:6px; text-align:center;">', cast(x.`Prêmio recebido` as string), '</td>',
    '</tr>'
  )
)
""")

# Cabeçalho/rodapé + junta as linhas
df_html = (
    df_agregado.withColumn("rows_html", rows_html)
    .withColumn("tbody_html", F.concat_ws("\n", F.col("rows_html")))
    .withColumn(
        "tabela_html",
        F.concat(
            F.lit(
                '<table style="border-collapse:collapse; width:100%; text-align:center;">'
                '<thead><tr>'
                '<th style="border:1px solid #ccc; padding:6px;">Posição</th>'
                '<th style="border:1px solid #ccc; padding:6px;">Nome</th>'
                '<th style="border:1px solid #ccc; padding:6px;">Nível</th>'
                # NOTE: reativar se quiser exibir coluna Loja
                #'<th style="border:1px solid #ccc; padding:6px;">Loja</th>'
                '<th style="border:1px solid #ccc; padding:6px;">Prêmio recebido</th>'
                '</tr></thead><tbody>'
            ),
            F.col("tbody_html"),
            F.lit("</tbody></table>"),
        ),
    )
    .select("conta_id", "missao_id", "missao_nome", "tabela_html")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Adicionando destaques na tabela `mini_maggu_v1_destaque`

# COMMAND ----------

sql_destaques = """
WITH dados AS (
  SELECT *
  FROM jsonb_to_recordset(%(rows)s::jsonb) AS r(
    id uuid,
    titulo text,
    mensagem text,
    imagem text,
    video text,
    url_iframe text,
    data_validade date,
    filtrar_por_contas_e_lojas boolean,
    apenas_no_erp text
  )
)
INSERT INTO mini_maggu_v1_destaque (
  id, titulo, mensagem, imagem, video, url_iframe,
  data_validade, criado_em, atualizado_em,
  filtrar_por_contas_e_lojas, apenas_no_erp
)
SELECT
  id, titulo, mensagem, imagem, video, url_iframe,
  data_validade, NOW(), NOW(),
  filtrar_por_contas_e_lojas, apenas_no_erp
FROM dados
"""

sql_m2m_contas = """
WITH dados AS (
  SELECT *
  FROM jsonb_to_recordset(%(rows)s::jsonb) AS r(
    destaque_id uuid,
    conta_id uuid
  )
)
INSERT INTO mini_maggu_v1_destaque_todas_as_lojas_das_contas (
  destaque_id, conta_id
)
SELECT
  destaque_id, conta_id
FROM dados;
"""

hoje = date.today()
validade = hoje + timedelta(days=(6 - hoje.weekday() or 7))

data_inicio_rodada_fmt = data_inicio_rodada.strftime("%d/%m/%y")
data_termino_rodada_fmt = data_termino_rodada.strftime("%d/%m/%y")
datas_rodada_fmt = f"{data_inicio_rodada_fmt} à {data_termino_rodada_fmt}"

rows_payload = []
m2m_contas_payload = []

for row in df_html.toLocalIterator():
    destaque_id = str(uuid4())
    conta_id = row["conta_id"]
    missao_nome_original = row['missao_nome'].strip()

    # Padroniza nome da missão para sempre começar com "Missão"
    if not missao_nome_original.lower().startswith("missão"):
        missao_nome_fmt = f"Missão {missao_nome_original}"
    else:
        missao_nome_fmt = missao_nome_original

    rows_payload.append(
        {
            "id": destaque_id,
            "titulo": f"Destaques da semana {missao_nome_fmt} - {datas_rodada_fmt}",
            "mensagem": row["tabela_html"],
            "imagem": None,
            "video": None,
            "url_iframe": None,
            "data_validade": validade.isoformat(),
            "filtrar_por_contas_e_lojas": conta_id is not None,
            "apenas_no_erp": None,
        }
    )

    if conta_id:
        m2m_contas_payload.append(
            {
                "destaque_id": destaque_id,
                "conta_id": conta_id,
            }
        )

if rows_payload:
    conn = postgres.get_connection()
    try:
        postgres.execute_query_with_params(
            sql_destaques, {"rows": json.dumps(rows_payload)}, connection=conn
        )

        if m2m_contas_payload:
            postgres.execute_query_with_params(
                sql_m2m_contas,
                {"rows": json.dumps(m2m_contas_payload)},
                connection=conn,
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Falha ao inserir destaques: {e}") from e
    finally:
        conn.close()

    print(f"Inseridos {len(rows_payload)} destaques.")
else:
    print("Nenhum destaque para inserir.")
