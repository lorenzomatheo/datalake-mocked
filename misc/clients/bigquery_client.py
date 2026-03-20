# Databricks notebook source
# MAGIC %md
# MAGIC # BigQuery Client
# MAGIC
# MAGIC Notebook para consultas ad-hoc ao BigQuery usando o ambiente Databricks.
# MAGIC BigQuery é usado para queries analíticas pesadas, aliviando a carga do PostgreSQL.

# COMMAND ----------

from datetime import datetime, timedelta

from maggulake.environment import DatabricksEnvironmentBuilder
from maggulake.environment.tables import CopilotTable

# COMMAND ----------

# Configuração de ambiente
env = DatabricksEnvironmentBuilder.build("bigquery_client", dbutils)

# Atalhos para facilitar o uso
spark = env.spark
bigquery = env.bigquery_adapter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exemplos de Uso dos Métodos Built-in

# COMMAND ----------

# MAGIC %md
# MAGIC ### Acessando Tabelas Diretamente

# COMMAND ----------

# Usando o método table() com enum CopilotTable (type-safe)
tabela_produtos = bigquery.table(CopilotTable.produtos)
print(f"Tabela completa: {tabela_produtos}")

# Lendo a tabela
spark.read.table(tabela_produtos).limit(10).display()

# COMMAND ----------

# Outras tabelas disponíveis
spark.read.table(bigquery.table(CopilotTable.vendas)).limit(5).display()

# COMMAND ----------

spark.read.table(bigquery.table(CopilotTable.produtos_loja)).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Queries Prontas de Vendas

# COMMAND ----------

# Vendas dos últimos 7 dias
data_inicio = datetime.now() - timedelta(days=7)

vendas_recentes = bigquery.get_vendas_por_periodo(
    data_inicio=data_inicio, status="venda-concluida", incluir_tenant=True
)

print(f"Total de vendas: {vendas_recentes.count()}")
vendas_recentes.limit(20).display()

# COMMAND ----------

# Vendas filtradas por lojas específicas
vendas_por_loja = bigquery.get_vendas_por_periodo(
    data_inicio=datetime.now() - timedelta(days=30),
    loja_ids=["loja-id-1", "loja-id-2"],  # Substituir por IDs reais
    status="venda-concluida",
    incluir_tenant=True,
)

vendas_por_loja.display()

# COMMAND ----------

# Vendas de EANs específicos
eans_interesse = ["7896658012345", "7891234567890"]  # Substituir por EANs reais

vendas_por_ean = bigquery.get_vendas_por_periodo(
    data_inicio=datetime.now() - timedelta(days=15),
    eans=eans_interesse,
    status="venda-concluida",
)

vendas_por_ean.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Produtos

# COMMAND ----------

# Produtos mais vendidos (classificação ABC)
# Classe A: 80% do faturamento
produtos_classe_a = bigquery.get_produtos_mais_vendidos(
    dias=30, classificacao_abc="A", apenas_lojas_ativas=True
)

print(f"Produtos classe A: {produtos_classe_a.count()}")
produtos_classe_a.display()

# COMMAND ----------

# Produtos classe B (80-95% do faturamento)
produtos_classe_b = bigquery.get_produtos_mais_vendidos(
    dias=30, classificacao_abc="B", apenas_lojas_ativas=True
)

produtos_classe_b.display()

# COMMAND ----------

# Produtos classe C (últimos 5% do faturamento)
produtos_classe_c = bigquery.get_produtos_mais_vendidos(
    dias=30, classificacao_abc="C", apenas_lojas_ativas=True
)

produtos_classe_c.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consultas de Produtos e Estoque

# COMMAND ----------

# Buscar produtos por EANs
eans = ["7896658012345", "7891234567890"]  # Substituir por EANs reais

produtos = bigquery.get_produtos_por_eans(
    eans=eans, colunas=["id as produto_id", "ean", "nome", "marca", "descricao"]
)

produtos.display()

# COMMAND ----------

# Buscar estoque dos mesmos produtos
estoque = bigquery.get_estoque_por_eans(eans=eans)

print(f"Registros de estoque: {estoque.count()}")
estoque.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Vendedores

# COMMAND ----------

# Vendedores ativos nos últimos 30 dias
vendedores_ativos = bigquery.get_vendedores_com_vendas(
    data_inicio=datetime.now() - timedelta(days=30),
    status_venda="venda-concluida",
    status_item="vendido",
)

print(f"Vendedores ativos: {vendedores_ativos.count()}")
vendedores_ativos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Queries SQL Customizadas

# COMMAND ----------

# Exemplo 1: Análise de vendas por tenant
query_vendas_tenant = f"""
    SELECT
        c.databricks_tenant_name as tenant,
        COUNT(DISTINCT v.id) as total_vendas,
        SUM(vi.valor_final) as faturamento_total,
        AVG(vi.valor_final) as ticket_medio
    FROM {bigquery.table(CopilotTable.vendas)} v
    JOIN {bigquery.table(CopilotTable.vendas_item)} vi ON v.id = vi.venda_id
    JOIN {bigquery.table(CopilotTable.contas_loja)} cl ON v.loja_id = cl.id
    JOIN {bigquery.table(CopilotTable.contas_conta)} c ON cl.conta_id = c.id
    WHERE DATE(v.realizada_em) >= DATE_SUB(CURRENT_DATE(), 30)
        AND v.status = 'venda-concluida'
    GROUP BY tenant
    ORDER BY faturamento_total DESC
"""

bigquery.sql(query_vendas_tenant).display()

# COMMAND ----------

# Exemplo 2: Top produtos por loja
query_top_produtos = f"""
    SELECT
        cl.codigo_loja,
        p.ean,
        p.nome as produto,
        COUNT(DISTINCT vi.venda_id) as qtd_vendas,
        SUM(vi.quantidade) as unidades_vendidas,
        SUM(vi.valor_final) as faturamento
    FROM {bigquery.table(CopilotTable.vendas_item)} vi
    JOIN {bigquery.table(CopilotTable.vendas)} v ON vi.venda_id = v.id
    JOIN {bigquery.table(CopilotTable.produtos)} p ON vi.produto_id = p.id
    JOIN {bigquery.table(CopilotTable.contas_loja)} cl ON v.loja_id = cl.id
    WHERE DATE(v.realizada_em) >= DATE_SUB(CURRENT_DATE(), 30)
        AND v.status = 'venda-concluida'
    GROUP BY cl.codigo_loja, p.ean, p.nome
    ORDER BY cl.codigo_loja, faturamento DESC
"""
