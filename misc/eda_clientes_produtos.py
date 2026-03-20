# Databricks notebook source
dbutils.widgets.text("produto", "fixa derme")
dbutils.widgets.dropdown("tipo_analise", "automatica", ["automatica", "manual"])

# COMMAND ----------

# MAGIC %pip install sweetviz
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import sweetviz as sv
from pyspark.sql import functions as F

produto = dbutils.widgets.get("produto")
tipo_analise = dbutils.widgets.get("tipo_analise")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Encontra clientes que compraram o produto

# COMMAND ----------

result = spark.sql(
    f"""
    WITH vendas_fixa_derme AS (
        SELECT venda_id
        FROM `production`.`analytics`.`view_vendas`
        WHERE lower(nome_produto) like '%{produto}%'
    ),

    clientes_sem_duplicatas AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cpf_cnpj ORDER BY atualizado_em DESC) as rn,
            size(array_distinct(compras)) as qtd_compras
        FROM `production`.`refined`.`clientes_conta_refined` c
        WHERE EXISTS (
            SELECT 1
            FROM vendas_fixa_derme v
            WHERE array_contains(c.compras, v.venda_id)
        )
    )

    SELECT
        * EXCEPT(rn)
    FROM clientes_sem_duplicatas
    WHERE rn = 1
"""
)

result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepara os dados para análise

# COMMAND ----------


def preparar_dados(df_spark):
    df = df_spark.toPandas()

    # removendo idades negativas
    df = df[(df["idade"] > 0) | (df["idade"].isna())]

    # Ajusta colunas de datas
    colunas_data = [
        "data_nascimento",
        "data_primeira_compra",
        "data_ultima_compra",
        "criado_em",
        "atualizado_em",
    ]
    for col in colunas_data:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Remove colunas do tipo array que dão problema nessa lib
    colunas_arrays = [
        "enderecos",
        "doencas_cronicas",
        "doencas_agudas",
        "medicamentos_controle",
        "compras",
    ]
    df = df.drop(columns=colunas_arrays)

    # Removendo colunas de ids
    colunas_ids = [
        "id",
        "conta_id",
        "cod_externo",
    ]
    df = df.drop(columns=colunas_ids)

    # Remove colunas que contêm apenas valores nulos
    colunas_com_valores = df.columns[df.notna().any()].tolist()
    df = df[colunas_com_valores]

    return df


df = preparar_dados(result)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gerando html com análise gráfica com SweetViz

# COMMAND ----------

df_analise = preparar_dados(result)
report = sv.analyze(df_analise)
report.show_html("/tmp/relatorio_clientes.html", open_browser=False)
dbutils.fs.mv(
    "file:/tmp/relatorio_clientes.html", "dbfs:/FileStore/relatorio_clientes.html"
)

# Gera botão para download to html
download_path = "/files/relatorio_clientes.html"
html = f"""
<a href="{download_path}" download="relatorio_clientes.html" target="_blank">
    <button style="
        background-color: #4CAF50;
        border: none;
        color: white;
        padding: 15px 32px;
        text-align: center;
        text-decoration: none;
        display: inline-block;
        font-size: 16px;
        margin: 4px 2px;
        cursor: pointer;
        border-radius: 4px;">
        Download Relatório
    </button>
</a>
"""
displayHTML(html)

# COMMAND ----------

if tipo_analise == "automatica":
    dbutils.notebook.exit("Análise concluída com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insights iniciais sobre os clientes

# COMMAND ----------


# pylint: disable=too-many-branches
def gerar_insights(df):
    insights = []

    # Demografia
    insights.append("\n=== DEMOGRAFIA ===")
    insights.append(f"Total de Clientes: {len(df):,}")

    if "idade" in df.columns:
        idade_media = df["idade"].mean()
        if pd.notna(idade_media):
            insights.append(f"Idade Média: {idade_media:.1f} anos")

    if "sexo" in df.columns:
        dist_sexo = df["sexo"].value_counts(normalize=True).round(3) * 100
        insights.append("Distribuição por Sexo:")
        for sexo, pct in dist_sexo.items():
            if pd.notna(sexo) and pd.notna(pct):
                insights.append(f"- {sexo}: {pct:.1f}%")

    if "eh_pessoa_fisica" in df.columns:
        pct_pf = df["eh_pessoa_fisica"].mean() * 100
        if pd.notna(pct_pf):
            insights.append(f"Pessoas Físicas: {pct_pf:.1f}%")

    # Comportamento de Compra
    insights.append("\n=== COMPORTAMENTO DE COMPRA ===")

    if "ticket_medio" in df.columns:
        ticket_medio = df["ticket_medio"].mean()
        if pd.notna(ticket_medio):
            insights.append(f"Ticket Médio: R$ {ticket_medio:.2f}")

    if "media_itens_por_cesta" in df.columns:
        media_itens = df["media_itens_por_cesta"].mean()
        if pd.notna(media_itens):
            insights.append(f"Média de Itens por Cesta: {media_itens:.1f}")

    if "total_de_compras" in df.columns:
        media_compras = df["total_de_compras"].mean()
        if pd.notna(media_compras):
            insights.append(f"Média de Compras por Cliente: {media_compras:.1f}")

    # Medicamentos
    insights.append("\n=== PERFIL DE CONSUMO ===")

    if "pct_itens_medicamentos" in df.columns:
        media_med = df["pct_itens_medicamentos"].mean()
        if pd.notna(media_med):
            insights.append(f"Média de Itens Medicamentos: {media_med:.1f}%")

    if all(
        col in df.columns
        for col in [
            "pct_itens_medicamentos_genericos",
            "pct_itens_medicamentos_marca",
        ]
    ):
        media_gen = df["pct_itens_medicamentos_genericos"].mean()
        media_marca = df["pct_itens_medicamentos_marca"].mean()
        if pd.notna(media_gen) and pd.notna(media_marca):
            insights.append(
                f"Proporção Genéricos vs Marca: {media_gen:.1f}% vs {media_marca:.1f}%"
            )

    print("\nInsights gerados:")
    for insight in insights:
        print(insight)


gerar_insights(df)

# COMMAND ----------


# COMMAND ----------


def analisar_dados_demograficos(df):
    plt.figure(figsize=(15, 10))

    # Distribuição de idade
    plt.subplot(2, 2, 1)
    sns.histplot(data=df, x="idade", bins=30)
    plt.title("Distribuição de Idade")

    # Distribuição por sexo
    plt.subplot(2, 2, 2)
    sns.countplot(data=df, x="sexo")
    plt.title("Distribuição por Sexo")

    # Distribuição por faixa etária
    plt.subplot(2, 2, 3)
    sns.countplot(data=df, x="faixa_etaria")
    plt.xticks(rotation=45)
    plt.title("Distribuição por Faixa Etária")

    # Tipo de pessoa (física/jurídica)
    plt.subplot(2, 2, 4)
    dados_tipo = pd.DataFrame(
        {
            "Tipo": ["Pessoa Física", "Pessoa Jurídica"],
            "Quantidade": [
                df["eh_pessoa_fisica"].sum(),
                df["eh_pessoa_juridica"].sum(),
            ],
        }
    )
    sns.barplot(data=dados_tipo, x="Tipo", y="Quantidade")
    plt.title("Distribuição por Tipo de Pessoa")

    plt.tight_layout()
    display(plt.gcf())
    plt.close()


def analisar_comportamento_compra(df):
    plt.figure(figsize=(15, 10))

    # Distribuição do ticket médio
    plt.subplot(2, 2, 1)
    sns.histplot(data=df, x="ticket_medio", bins=30)
    plt.title("Distribuição do Ticket Médio")

    # Média de itens por cesta
    plt.subplot(2, 2, 2)
    sns.histplot(data=df, x="media_itens_por_cesta", bins=30)
    plt.title("Média de Itens por Cesta")

    # Total de compras
    plt.subplot(2, 2, 3)
    sns.histplot(data=df, x="total_de_compras", bins=30)
    plt.title("Distribuição do Total de Compras")

    # Intervalo médio entre compras
    plt.subplot(2, 2, 4)
    sns.histplot(data=df, x="intervalo_medio_entre_compras", bins=30)
    plt.title("Intervalo Médio entre Compras (dias)")

    plt.tight_layout()
    display(plt.gcf())
    plt.close()


# COMMAND ----------


def analyze_array_column(df, column_name):
    print(f"Analisando {column_name}...")

    value_counts = (
        df.select(F.explode(F.col(column_name)).alias("value"))
        .groupBy("value")
        .count()
        .orderBy("count", ascending=False)
        .limit(10)
        .toPandas()
    )

    if len(value_counts) == 0:
        plt.text(0.5, 0.5, f"Sem dados para {column_name}", ha="center", va="center")
        return

    barras = plt.bar(range(len(value_counts)), value_counts["count"])
    plt.title(f"Top 10 valores mais comuns em {column_name}")
    plt.xlabel("Valores")
    plt.ylabel("Quantidade")

    plt.xticks(range(len(value_counts)), value_counts["value"], rotation=45, ha="right")

    for barra in barras:
        height = barra.get_height()
        plt.text(
            barra.get_x() + barra.get_width() / 2.0,
            height,
            f"{int(height)}",
            ha="center",
            va="bottom",
        )


columns_to_analyze = [
    "doencas_cronicas",
    "doencas_agudas",
    "medicamentos_controle",
]

n_cols = 2
n_rows = (len(columns_to_analyze) + 1) // 2
plt.figure(figsize=(20, 6 * n_rows))

for idx, column in enumerate(columns_to_analyze, 1):
    plt.subplot(n_rows, n_cols, idx)
    analyze_array_column(result, column)

plt.tight_layout()
plt.show()

# COMMAND ----------

print("Analisando Dados Demográficos...")
analisar_dados_demograficos(df)

# COMMAND ----------

print("Analisando Comportamento de Compra...")
analisar_comportamento_compra(df)
