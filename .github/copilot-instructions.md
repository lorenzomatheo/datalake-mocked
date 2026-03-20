# Maggu Datalake — README para Máquinas

> Data lakehouse Databricks com arquitetura medallion (raw → standard → refined → analytics) para o ecossistema farmacêutico.

## Comandos Críticos

```bash
make install    # Instala dependências Python
make format     # Ruff + Terraform fmt + MDFormat
make lint       # Ruff + Pylint + PyMarkdownLnt
make test       # Executa suite pytest
make coverage   # Testes com relatório de cobertura
```

**Critério de sucesso absoluto**: A tarefa só está concluída quando `make format && make lint && make test` executam sem erros.

______________________________________________________________________

\<critical_boundaries>

## Limites Críticos

### NEVER — Jamais Faça

- Modifique arquivos em `terraform/` sem aprovação explícita do time
- Escreva diretamente em `refined.produtos_refined` no meio de um pipeline (use `refined._produtos_em_processamento`)
- Use dados reais (PII) em mocks, fixtures ou testes
- Use `.option("mergeSchema", "true")` sem documentar a mudança de schema no PR
- Confie em inferência automática de schema para tabelas de produção
- Execute `git push --force` ou `git reset --hard` sem aprovação do time
- Hardcode credenciais, tokens ou secrets em qualquer arquivo

### MUST — Sempre Faça

- Type hints em **todo** código Python
- Schemas PySpark declarados explicitamente com `T.StructType`
- Notebooks com widget `stage` (dev/prod): `dev` → `catalog=staging`, `prod` → `catalog=production`
- Extraia lógica reutilizável para `maggulake/` (princípio DRY)
- Acesse secrets via `dbutils.secrets.get(scope="...", key="...")`
- Use `"spark.sql.caseSensitive": "true"` na configuração do Spark
- Todo pipeline deve ser **idempotente** (re-executável sem duplicar dados)

\</critical_boundaries>

______________________________________________________________________

## Ciclo de Trabalho: EPIV

Antes de qualquer implementação, siga obrigatoriamente este ciclo:

1. **Entender** — Leia o código existente relevante. Identifique dependências e impactos antes de agir.
1. **Planejar** — Descreva os passos e impactos **antes** de escrever código. Proponha o plano ao usuário.
1. **Implementar** — Siga as convenções: type hints, schemas explícitos, DRY, stage-aware, sem PII em testes.
1. **Verificar** — Execute `make format && make lint && make test`. Corrija **todos** os erros antes de declarar conclusão.

______________________________________________________________________

## Arquitetura Medallion

| Pasta                   | Destino                | Descrição                                  |
| ----------------------- | ---------------------- | ------------------------------------------ |
| `0_any_2_raw/`          | `{catalog}.raw.*`      | Ingestão bruta (CSV, JSON, XML)            |
| `1_raw_2_standard/`     | `{catalog}.standard.*` | Padronização e schemas Delta               |
| `2_standard_2_refined/` | `{catalog}.refined.*`  | Enriquecimento LLM, fuzzy matching, ANVISA |
| `3_analytics/`          | PowerBI / PostgreSQL   | BI e dashboards                            |

**Catálogos**: `stage=dev` → `catalog=staging` | `stage=prod` → `catalog=production`

**S3**: `maggu-datalake-dev` (staging) | `maggu-datalake-prod` (production)

### Padrão Crítico: Pipeline de Enriquecimento

Para evitar atualizações parciais em `refined.produtos_refined`:

- Etapas intermediárias leem/escrevem **exclusivamente** em `refined._produtos_em_processamento`
- Apenas o **primeiro notebook** do workflow lê de `produtos_refined`
- Apenas o **último notebook** salva de volta em `produtos_refined`

______________________________________________________________________

## Padrões de Desenvolvimento

### Estrutura Mínima de Notebook

```python
from maggulake.environment import DatabricksEnvironmentBuilder

env = DatabricksEnvironmentBuilder.build("notebook_name", dbutils, widget={"stage": "dev"})
```

### Imports Frequentes

```python
from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.utils.postgres import PostgresAdapter
from maggulake.utils.df_utils import normaliza_nomes_colunas
from maggulake.utils.schemas import schema_ids_produtos
```

### Acesso a Secrets e PostgreSQL

```python
# Secrets — nunca hardcode credenciais
POSTGRES_USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
POSTGRES_PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")

# PostgreSQL
from maggulake.utils.postgres import PostgresAdapter
postgres = PostgresAdapter(stage, POSTGRES_USER, POSTGRES_PASSWORD)
df = postgres.read_table(spark, "schema.table")
```

### Armadilhas Comuns

- **Widgets persistem**: defina defaults explicitamente em cada notebook
- **Cargas incrementais**: prefira MERGE/upsert; todo pipeline deve ser idempotente
- **Schemas**: nunca confie em inferência automática em produção; use `T.StructType` explícito
- **mergeSchema**: documente toda mudança de schema antes de usar no PR
- **UDFs**: prefira funções built-in do PySpark (menor overhead de serialização)
- **Memória**: use `.cache()` estrategicamente; verifique dimensionamento de cluster no Terraform

______________________________________________________________________

## Infraestrutura (Terraform)

Definições em `terraform/pipelines/*.tf` — módulo reutilizável: `terraform/pipelines/pipeline/`

- **ON_DEMAND**: `atualiza_produtos_producao`, `enriquece_produtos` (confiabilidade crítica)
- **SPOT_WITH_FALLBACK**: demais workflows (custo otimizado)
- Agendamento: cron Quartz, timezone `America/Sao_Paulo`
- Imagens Docker: `ghcr.io/maggu-ai/datalake:standard` (veja `docker/databricks_images/`)

Workspace: `https://dbc-25297b38-ec1c.cloud.databricks.com`

______________________________________________________________________

## Pull Requests

### Antes de Abrir

- `make format && make lint && make test` passando localmente
- Screenshots de evidências de execução no Databricks
- Atualizar `terraform/pipelines/` se o workflow foi criado ou modificado
- Para alterações de tabelas: validar migrações Liquibase (`liquibase/`)

### Antes do Merge

- `update with rebase` antes de dar merge
- Preferir **squash and merge** para histórico linear
- Mínimo de **2 revisões** do time de dados

______________________________________________________________________

## Referências (Progressive Disclosure)

Para detalhes que não estão aqui, consulte:

- [`README.md`](README.md) — visão geral e contexto do projeto
- [`tests/README.md`](tests/README.md) — convenções de testes com pytest
- [`maggulake/`](maggulake/) — pacote de utilitários (leia antes de criar helpers novos)
- [`.github/instructions/hyperflow.instructions.md`](.github/instructions/hyperflow.instructions.md) — WhatsApp Flows (Meta Flow spec v3.0)

______________________________________________________________________

> **A tarefa só está concluída quando `make format && make lint && make test` executam sem erros.**
