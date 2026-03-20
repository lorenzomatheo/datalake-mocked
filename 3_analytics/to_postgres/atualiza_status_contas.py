# Databricks notebook source
# MAGIC %md
# MAGIC # Atualiza o status das contas baseado no status das lojas
# MAGIC

# COMMAND ----------

from enum import Enum

from pyspark.sql import functions as F

from maggulake.environment import DatabricksEnvironmentBuilder

# COMMAND ----------

# Regra de negocio (usar isso caso o time de Growth pergunte)
# --------------------------------------------------------------------------------------
# 1. Se a conta possui pelo menos 1 loja com status ATIVA      → status da conta = 'ATIVA'
# 2. Se a conta possui TODAS as suas lojas com status de churn → status da conta = 'CHURN'
# 3. Se a conta não possui lojas                               → status da conta = 'SEM_LOJAS'
# 4. Caso contrário,                                           → status da conta = status que mais se repete entre as lojas (excluindo CHURN)

# --------------------------------------------------------------------------------------

# COMMAND ----------


class StatusLoja(str, Enum):
    """
    Status possíveis para as lojas.
    NOTE: Espelho do modelo que fica la no copilot, arquivo lojas.py
    """

    ATIVA = "ATIVA"
    CHURN_VOLUNTARIO = "CHURN_VOLUNTARIO"
    CHURN_INVOLUNTARIO = "CHURN_INVOLUNTARIO"
    EM_ATIVACAO = "EM_ATIVACAO"
    INTEGRACAO = "INTEGRACAO"
    ONBOARDING = "ONBOARDING"


class StatusConta(str, Enum):
    """
    Status possíveis para contas. Fiz parecido com lojas, so simplifiquei CHURN

    NOTE: So pode ate 128 caracteres no Postgres, mas se quiser pode aumentar pelo copilot.
    """

    ATIVA = "ATIVA"
    CHURN = "CHURN"
    EM_ATIVACAO = "EM_ATIVACAO"
    INTEGRACAO = "INTEGRACAO"
    ONBOARDING = "ONBOARDING"

    SEM_LOJAS = "SEM_LOJAS"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "atualiza_status_contas",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
)

spark = env.spark
STAGE = env.settings.name_short
DEBUG = dbutils.widgets.get("debug") == "true"
postgres = env.postgres_adapter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura e Agregação dos Status das Lojas

# COMMAND ----------

# Query que calcula o novo status para cada conta baseado nas suas lojas
query_calcula_status = f"""
WITH total_lojas_por_conta AS (
    SELECT
        cc.id as conta_id,
        COUNT(cl.id) as total_lojas
    FROM contas_conta cc
    LEFT JOIN contas_loja cl ON cc.id = cl.conta_id
    GROUP BY cc.id
),
-- Agrupa lojas por status para análise posterior
lojas_por_status AS (
    SELECT
        cl.conta_id,
        cl.status,
        COUNT(*) as quantidade
    FROM contas_loja cl
    GROUP BY cl.conta_id, cl.status
),
-- Calcula flags booleanas para regras de negócio
flags_conta AS (
    SELECT
        conta_id,
        BOOL_OR(status = '{StatusLoja.ATIVA.value}') as tem_loja_ativa,
        BOOL_AND(status IN ('{StatusLoja.CHURN_VOLUNTARIO.value}', '{StatusLoja.CHURN_INVOLUNTARIO.value}')) as todas_lojas_churn
    FROM contas_loja
    GROUP BY conta_id
),
-- Define o status predominante (excluindo CHURN da contagem)
status_predominante AS (
    SELECT DISTINCT ON (conta_id)
        conta_id,
        status as status_base
    FROM lojas_por_status
    WHERE status NOT IN ('{StatusLoja.CHURN_VOLUNTARIO.value}', '{StatusLoja.CHURN_INVOLUNTARIO.value}')
    ORDER BY conta_id, quantidade DESC
),
-- Aplica regras de negócio na ordem de prioridade
novo_status_conta AS (
    SELECT
        tl.conta_id,
        tl.total_lojas,
        CASE
            -- Regra 3: Se não possui lojas → SEM_LOJAS
            WHEN tl.total_lojas = 0 THEN '{StatusConta.SEM_LOJAS.value}'
            -- Regra 1: Se tem pelo menos 1 loja ATIVA → ATIVA
            WHEN f.tem_loja_ativa THEN '{StatusConta.ATIVA.value}'
            -- Regra 2: Se TODAS as lojas são churn → CHURN
            WHEN f.todas_lojas_churn THEN '{StatusConta.CHURN.value}'
            -- Regra 4: Status predominante (excluindo CHURN)
            ELSE sp.status_base
        END as status_novo
    FROM total_lojas_por_conta tl
    LEFT JOIN flags_conta f ON tl.conta_id = f.conta_id
    LEFT JOIN status_predominante sp ON tl.conta_id = sp.conta_id
)
SELECT
    cc.id as conta_id,
    cc.name as nome_conta,
    cc.status as status_atual,
    nsc.status_novo,
    nsc.total_lojas
FROM contas_conta cc
INNER JOIN novo_status_conta nsc ON cc.id = nsc.conta_id
"""

contas_para_atualizar = postgres.read_query(spark, query_calcula_status)

if DEBUG:
    print(
        f"Total de contas que precisam ser atualizadas: {contas_para_atualizar.count()}"
    )

# COMMAND ----------

if DEBUG:
    print("\nResumo das mudanças de status:")
    contas_para_atualizar.groupBy("status_atual", "status_novo").count().orderBy(
        "status_atual", "status_novo"
    ).display()

# COMMAND ----------

print(f"\nContas que terão status alterado: {contas_para_atualizar.count()}")
contas_para_atualizar.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparação dos Dados para Atualização

# COMMAND ----------

dados_atualizacao = contas_para_atualizar.select(
    F.col("conta_id"), F.col("status_novo")
)

if dados_atualizacao.count() == 0:
    dbutils.notebook.exit(
        "✓ Processo finalizado com sucesso - nenhuma atualização necessária"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Atualização no PostgreSQL

# COMMAND ----------

dados_pandas = dados_atualizacao.toPandas()

conta_ids = dados_pandas['conta_id'].tolist()
status_novos = dados_pandas['status_novo'].tolist()

# Constrói a query usando padrão CASE-WHEN para update em lote
placeholders = ','.join(['%s'] * len(conta_ids))
case_when_parts = []
params = []

for conta_id, status_novo in zip(conta_ids, status_novos):
    case_when_parts.append("WHEN %s THEN %s")
    params.extend([conta_id, status_novo])

# Adiciona os IDs para o WHERE IN clause
params.extend(conta_ids)

update_query = f"""
    UPDATE contas_conta
    SET
        status = CASE id
            {' '.join(case_when_parts)}
            ELSE status
        END,
        updated_at = NOW()
    WHERE id IN ({placeholders})
"""

# COMMAND ----------

if DEBUG:
    print("Query de atualização:")
    print(f"\nParâmetros (primeiros 10): {params[:10]}")

# COMMAND ----------

postgres.execute_query_with_params(update_query, params)


# COMMAND ----------

if DEBUG:
    print("✓ Atualização concluída! Registros atualizados com sucesso!")
    print(f"- Total de contas atualizadas: {len(conta_ids)}")

    # Mostra breakdown por novo status
    resumo_status = dados_pandas.groupby('status_novo').size().to_dict()
    for status, quantidade in resumo_status.items():
        print(f"  - {quantidade} contas → {status}")
