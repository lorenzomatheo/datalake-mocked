# Databricks notebook source
# MAGIC %md
# MAGIC # Postgres Client
# MAGIC
# MAGIC Notebook para consultas ad-hoc ao banco de dados Postgres usando o ambiente Databricks

# COMMAND ----------

from maggulake.environment import DatabricksEnvironmentBuilder

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build("postgres_client", dbutils)
spark = env.spark
postgres = env.postgres_replica_adapter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Métodos Built-in do PostgresAdapter
# MAGIC
# MAGIC O `postgres_adapter` possui diversos métodos prontos para facilitar o trabalho com o banco:
# MAGIC

# COMMAND ----------

# Lista todas as tabelas disponíveis
postgres.list_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lendo Tabelas

# COMMAND ----------

# Ler tabela completa como Spark DataFrame
postgres.read_table(spark, "produtos_produtoloja").display()

# COMMAND ----------


df_pandas = postgres.read_table_pandas("contas_conta")
df_pandas.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Queries Prontas de Negócio

# COMMAND ----------

# Produtos em campanhas ativas da indústria
produtos_campanha = postgres.get_produtos_em_campanha(
    spark, tipo="INDUSTRIA", status="ATIVA"
)
print(f"Total de produtos em campanhas: {produtos_campanha.count()}")
# Mostra os primeiros 10
produtos_campanha.show(10)

# COMMAND ----------

# Lojas disponíveis (exclui testes, ERPs, obsoletas)
lojas_disponiveis = postgres.get_lojas(
    spark, extra_fields=["codigo_loja", "cnpj", "created_at"]
)
lojas_disponiveis.display()

# COMMAND ----------

# Contas ativas (exclui contas de teste)
contas = postgres.get_contas(spark)
contas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Queries SQL Customizadas

# COMMAND ----------

# Exemplo 1: Query simples retornando pandas DataFrame
query_simples = """
    SELECT count(*) as total
    FROM vendas_venda
"""
postgres.execute_query(query_simples)

# COMMAND ----------

# Exemplo 2: Query com JOIN retornando Spark DataFrame
query_com_join = """
    SELECT
        cl.codigo_loja as loja,
        sum(vi.preco_venda_desconto * vi.quantidade) AS valor_vendido,
        count(distinct vi.venda_id) as coupons
    FROM
        vendas_item vi
    JOIN
        vendas_venda vv ON vi.venda_id = vv.id
    LEFT JOIN
        contas_loja cl ON vv.loja_id = cl.id
    WHERE
        cl.databricks_tenant_name like '%farmagui%'
    GROUP BY
        loja
    ORDER BY
        loja
"""
postgres.read_query(spark, query_com_join).display()

# COMMAND ----------

# Exemplo 3: Query com parâmetros (previne SQL injection)
query_parametrizada = """
    SELECT
        id,
        nome,
        ean
    FROM
        produtos_produtov2
    WHERE
        nome ILIKE %s
    LIMIT %s
"""
postgres.execute_query_with_params(query_parametrizada, params=('%dipirona%', 10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exemplos de Queries Ad-hoc (seu código original)
# MAGIC
# MAGIC Abaixo mantemos os exemplos de queries customizadas para referência

# COMMAND ----------


query = """
    SELECT count(*) FROM vendas_venda vv
    LEFT JOIN
        (
            SELECT
                ue.codigo_externo,
                ue.user_id,
                clu.loja_id
            FROM
                users_extrainfo ue
            JOIN
                contas_loja_users clu ON ue.user_id = clu.user_id
        ) ue ON vv.vendedor_id = ue.user_id AND vv.loja_id = ue.loja_id


  """

postgres.execute_query(query).display()
