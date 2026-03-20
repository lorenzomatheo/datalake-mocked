# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make install        # Instala dependências Python (pip install -r requirements.in -r requirements-dev.in -e .)
make format         # ruff (imports + estilo) + terraform fmt + mdformat
make lint           # ruff + pylint + pymarkdownlnt
make test           # pytest
make coverage       # pytest --cov=maggulake
make coverage-html  # pytest --cov=maggulake --cov-report=html
./liquibase.sh      # Executa migrações de banco de dados via Docker
```

Para rodar um único teste: `pytest tests/unit/test_foo.py::test_bar`

## Arquitetura

**Data lakehouse Databricks** com arquitetura medallion para o ecossistema farmacêutico. Workflows orquestrados via Terraform, código executado em notebooks Python/PySpark.

### Camadas de dados

| Diretório               | Camada    | Destino no catálogo    |
| ----------------------- | --------- | ---------------------- |
| `0_any_2_raw/`          | raw       | `{catalog}.raw.*`      |
| `1_raw_2_standard/`     | standard  | `{catalog}.standard.*` |
| `2_standard_2_refined/` | refined   | `{catalog}.refined.*`  |
| `3_analytics/`          | analytics | PowerBI / dashboards   |

**Mapeamento de catálogos:** `stage=dev` → `catalog=staging` | `stage=prod` → `catalog=production`

### Pacote `maggulake/`

Utilitários reutilizáveis instalados em modo editável. Módulos principais:

- `environment/` — `DatabricksEnvironmentBuilder` (resolução de stage/catalog, usado em todo notebook)
- `utils/postgres.py` — `PostgresAdapter` (leitura/escrita no PostgreSQL externo)
- `utils/df_utils.py` — utilitários de DataFrame (ex: `normaliza_nomes_colunas`)
- `utils/schemas/` — schemas PySpark reutilizáveis
- `produtos_repository.py` — gerenciamento de IDs de produtos (crítico para data lineage)
- `prompts/` — templates de prompts LLM
- `vector_search_retriever/` — busca de embeddings (Milvus / Databricks Vector Search)

### Infraestrutura (Terraform)

Todos os workflows Databricks estão em `terraform/pipelines/*.tf`. O módulo reutilizável está em `terraform/pipelines/pipeline/`. Imagem Docker customizada: `ghcr.io/maggu-ai/datalake:standard`. Agendamento usa cron Quartz com timezone `America/Sao_Paulo`.

- Workflows críticos (`atualiza_produtos_producao`, `enriquece_produtos`): usar `ON_DEMAND`
- Demais workflows: `SPOT_WITH_FALLBACK`

### Ativação / HyperFlow

`ativacao/hyperflow/` contém definições de WhatsApp Flows em JSON (spec Meta Flow v3.0). Testar via Meta Flow Builder ou WhatsApp mobile (web não suporta Flows). Ver `.github/instructions/hyperflow.instructions.md` para padrões de codificação.

## Padrões críticos

### Padrão de notebook Databricks

```python
# Databricks notebook source
from maggulake.environment import DatabricksEnvironmentBuilder

env = DatabricksEnvironmentBuilder.build("notebook_name", dbutils, widget={...})
# env.settings.catalog → catálogo correto (staging/production)
# env.settings.name_short → stage (dev/prod)
```

Sempre usar `"spark.sql.caseSensitive": "true"` na configuração do Spark.

### Pipeline de enriquecimento de produtos

Para evitar atualizações parciais, o workflow usa uma tabela de staging:

- **Primeiro notebook** lê de `refined.produtos_refined`
- **Notebooks intermediários** leem/escrevem em `refined._produtos_em_processamento`
- **Último notebook** escreve de volta em `refined.produtos_refined`

Nunca escrever diretamente em `produtos_refined` no meio do pipeline.

### Operações Delta Table

```python
# Upsert padrão
delta_table.alias("target").merge(
    updates.alias("source"), "target.id = source.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

Usar `mergeSchema=true` com cautela — pode quebrar schemas de tabelas de produção.

### Secrets

```python
POSTGRES_USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
```

### PostgreSQL

```python
from maggulake.utils.postgres import PostgresAdapter
postgres = PostgresAdapter(stage, POSTGRES_USER, POSTGRES_PASSWORD)
df = postgres.read_table(spark, "schema.table")
postgres.write_table(df, "schema.table", mode="append")
```

### Chamadas a LLMs — LiteLLM

Todas as chamadas a modelos de linguagem passam pelo **LiteLLM** via `litellm_client.completion()`. Nunca importar `ChatOpenAI`, `ChatVertexAI`, `ChatGoogleGenerativeAI` ou outro SDK de LLM diretamente fora de `maggulake/utils/model_wrappers/`.

- **TaggingProcessor** já usa LiteLLM internamente — notebooks não precisam mudar nada
- **MedGemma** (`medgemma.py`) usa endpoint HTTP customizado — não passa pelo LiteLLM
- **Agno agents** (`TracedAgent`, `BuscaWeb`) usam o framework Agno — não passam pelo LiteLLM
- Modelo padrão pode ser sobrescrito via `DEFAULT_LLM_MODEL` (variável de ambiente)
- Nomes de modelo no formato LiteLLM: `"gpt-4o"`, `"gemini/gemini-2.5-flash"`, `"vertex_ai/gemini-2.5-flash"`

## Convenções de código

- Para obter data/hora atual, usar `agora_em_sao_paulo()` de `maggulake.utils.time` — nunca `datetime.now()` com timezone manual
- Type hints obrigatórios em todo código Python
- Schemas PySpark explícitos — nunca confiar em inferência para tabelas de produção
- Extrair lógica repetida para `maggulake/` (princípio DRY)
- `dbutils`, `spark`, `display`, `displayHTML`, `get_ipython` são builtins conhecidos pelo ruff
- Linha máxima: 88 caracteres; target: Python 3.12

## Aprendizado contínuo (self-healing)

Após **qualquer correção** do usuário (ex: "não faça X, use Y"), siga este fluxo:

1. Aplique a correção imediatamente
1. Avalie se o padrão é reutilizável (afeta mais de um arquivo ou sessão)
1. Se sim, adicione a regra na seção apropriada deste CLAUDE.md com a correção já redigida, pronta para aceitar
1. Adicione e commite junto com a mudança

Objetivo: nenhum erro deve precisar ser corrigido duas vezes. Cada correção
vira uma regra compartilhada com todo o time via repositório.

## Pull Requests

- Rodar `make format` e `make lint` antes de abrir o PR
- Atualizar Terraform se o workflow mudou
- Adicionar evidências de execução (screenshots)
- Preferir `squash and merge` para manter o histórico linear
- Requer review de 2 pessoas do time de dados
