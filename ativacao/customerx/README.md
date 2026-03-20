# Integração CustomerX

Este diretório contém notebooks para integração com a API do CustomerX, uma plataforma de gestão de relacionamento com clientes (CRM).

## Visão Geral

A integração oferece dois tipos de operações:

### 1. Setup Inicial (Create-Only)

- **Grupos** → Cria grupos (um por rede) para organização e filtros
- **Matrizes e Filiais** → Cria contas (redes) e lojas no CustomerX com vínculo aos grupos
- **Associação** → Vincula filiais às suas matrizes
- **Contatos** → Cria donos de contas e gerentes de lojas

### 2. Sincronização Incremental (Update-Only)

- **Atualização de Grupos** → Atualiza dados de grupos modificados no Postgres
- **Atualização de Matrizes/Filiais** → Atualiza dados de contas/lojas modificadas
- **Atualização de Contatos** → Atualiza dados de donos/gerentes modificados
- **Atualização de Emails** → Substitui emails fake por reais quando disponíveis

## Utilitários Centralizados

Os notebooks utilizam módulos centralizados em `maggulake/customerx/`:

### `notebook_setup.py`

Centraliza configuração comum de todos os notebooks:

```python
env, customerx_client, dry_run = setup_customerx_notebook(dbutils, "nome_do_notebook")
```

Retorna:

- `env`: Ambiente Databricks configurado com Postgres adapter (`env.postgres_replica_adapter`)
- `customerx_client`: Cliente CustomerX API
- `dry_run`: Boolean para modo de teste

### `constants.py`

Define constantes da integração:

- `FAKE_EMAIL_DOMAIN`: Domínio para emails fallback (`maggu.ai`)
- `FAKE_EMAIL_PATTERN`: Padrão de email fake para gerentes (`loja_{uuid}@maggu.ai`)

### `checkpoint.py`

Gerencia checkpoints de sincronização incremental:

- `get_last_sync()`: Retorna timestamp da última sincronização bem-sucedida
- `update_checkpoint()`: Atualiza checkpoint após execução

Usa tabela `{catalog}.raw.customerx_sync_checkpoint` para rastrear:

- Timestamp de última sincronização
- Status (success, failed, running)
- Quantidade de registros processados
- Mensagens de erro

### `reporting.py`

Utilitários para relatórios e logging de erros:

- `ErrorAccumulator`: Acumula erros durante processamento e salva em batch
- Schemas para sumários e rastreamento de erros
- Salva erros em `{catalog}.raw.customerx_integration_errors`

## Auditoria e Rastreabilidade

O sistema implementa rastreabilidade completa através de:

1. **Checkpoints**: Tabela `control.customerx_sync_checkpoint` registra todas as execuções
1. **Erros**: Tabela `control.customerx_integration_errors` armazena falhas com contexto
1. **External IDs**: Usa UUIDs do Postgres para garantir idempotência

## Notebooks Disponíveis

### Notebooks de Setup Inicial (Create-Only)

#### 1. `cria_grupos.py`

Cria grupos no CustomerX a partir das contas (redes) do Postgres.

**Fonte de Dados**: `env.postgres_replica_adapter.get_contas(spark)`

**Estratégia**:

- **Diff-based sync**: Compara com CustomerX antes de criar
- **Create-only**: Não atualiza grupos existentes, apenas cria novos
- **Idempotente**: Usa UUID da conta do Postgres como `external_id` do grupo
- **Um grupo por rede**: Facilita filtros e organização no CustomerX

**Filtros aplicados automaticamente pelo adapter**:

- Exclui contas de teste/ERPs (toolspharma, automatiza, hos, asasys, etc.)
- Exclui contas obsoletas (nome iniciado com "obsoleto")

#### 2. `cria_matrizes_filiais.py`

Cria clientes no CustomerX a partir das tabelas `contas_conta` e `contas_loja` do Postgres.

**Fonte de Dados**:

- `env.postgres_replica_adapter.get_contas(spark)` → Matrizes (redes)
- `env.postgres_replica_adapter.get_lojas(spark)` → Filiais (lojas)

**Estratégia**:

- **Diff-based sync**: Compara com CustomerX antes de criar
- **Two-phase**: Cria matrizes primeiro, depois filiais
- **Idempotente**: Usa UUID do Postgres como `external_id_client`
- Vincula clientes aos grupos via `external_id_group`
- Calcula campo `estado` (UF) para redes baseado na UF mais frequente das lojas
- **Previne matrizes órfãs**: Apenas cria contas que têm pelo menos 1 loja válida

**Campos enviados para CustomerX**:

- UUID completo preservado em `custom_attributes.postgres_uuid`
- `external_id_client` truncado para 6 caracteres (limitação da API)

**Filtros aplicados automaticamente pelo adapter**:

- Apenas lojas ativas (`ativo = true`)
- Exclui lojas de contas de ERPs e obsoletas
- **Exclui contas sem lojas válidas** (previne matrizes sem filiais)

#### 3. `associa_matrizes_filiais.py`

Vincula filiais (lojas) às suas matrizes (redes) usando o endpoint `/branch_clients`.

**Fonte de Dados**:

- `env.postgres_replica_adapter.get_lojas(spark)`
- `env.postgres_replica_adapter.get_contas(spark)`

**Estratégia**:

- **Association-only**: Apenas vincula, não cria nem atualiza clientes
- Busca todos os clientes existentes no CustomerX
- Agrupa filiais por matriz
- Envia associações em batch

**Pré-requisito**: Executar `cria_grupos.py` e `cria_matrizes_filiais.py` antes.

#### 4. `cria_contatos.py`

Cria contatos (donos e gerentes) no CustomerX.

**Fonte de Dados**:

- `env.postgres_replica_adapter.read_table(spark, "contas_donoconta")` → Donos
- `env.postgres_replica_adapter.get_gerentes_lojas(spark)` → Gerentes

**Mudança Importante na Fonte de Dados (Gerentes)**:

Anteriormente: `contas_gerenteloja` (cobertura ~8% das lojas)
Atualmente: `atendentes_atendenteloja` (cobertura ~100% com emails fallback)

A query `get_gerentes_lojas()` faz JOIN entre:

- `atendentes_atendenteloja`
- `atendentes_atendente`
- `atendentes_ativacao`
- `atendentes_informacoespessoais`
- `contas_loja`

E seleciona **um gerente por loja** (o mais recentemente cadastrado).

**Estratégia**:

- **Two-phase**: Cria donos primeiro, depois gerentes
- **Create-only**: Não atualiza registros existentes
- **Idempotente**: Compara com contatos existentes para evitar duplicação
- **Validação**: Apenas cria contatos com nome válido
- **Fallback de E-mail**: Gerentes sem email recebem `loja_{uuid}@maggu.ai`
- **Unicidade**: API valida duplicação por (telefone, cliente) e (email, cliente)

**Benefícios da Mudança**:

- 📈 Cobertura de ~8% → ~100% das lojas com contatos
- 🔓 Destrava automações do CustomerX que requerem contatos
- 📧 Sistema de fallback permite atualizações posteriores via `atualiza_emails_contatos.py`

**Pré-requisito**: Executar `cria_matrizes_filiais.py` antes (contatos precisam estar associados a contas/lojas existentes).

### Notebooks de Sincronização Incremental (Update-Only)

#### 5. `atualiza_grupos.py`

Atualiza grupos no CustomerX que foram modificados no Postgres.

**Fonte de Dados**: `env.postgres_replica_adapter.get_contas(spark)` + filtro `updated_at > last_sync`

**Estratégia**:

- **Incremental**: Atualiza apenas registros modificados desde última execução
- **Checkpoint-based**: Usa `control.customerx_sync_checkpoint` para rastrear última sync
- **Update-only**: Apenas atualiza registros existentes (não cria novos)
- **Campos atualizados**: description (nome do grupo), status

**Checkpoint**:

- Primeira execução: Processa TODOS os registros
- Execuções seguintes: Apenas registros com `updated_at > last_sync`

**Pré-requisito**: Executar `cria_grupos.py` antes da primeira execução.

#### 6. `atualiza_matrizes_filiais.py`

Atualiza clientes (matrizes/filiais) no CustomerX que foram modificados no Postgres.

**Fonte de Dados**:

- `env.postgres_replica_adapter.get_contas(spark)` + filtro `updated_at > last_sync`
- `env.postgres_replica_adapter.get_lojas(spark)` + filtro `updated_at > last_sync`

**Estratégia**:

- **Incremental**: Atualiza apenas registros modificados
- **Checkpoint-based**: Usa `control.customerx_sync_checkpoint`
- **Update-only**: Apenas atualiza registros existentes (não cria novos)
- **Campos atualizados**: company_name, trading_name, custom_attributes

**Checkpoint**:

- Primeira execução: Processa TODOS os registros
- Execuções seguintes: Apenas registros com `updated_at > last_sync`

**Pré-requisito**: Executar `cria_matrizes_filiais.py` antes da primeira execução.

#### 7. `atualiza_contatos.py`

Atualiza contatos (donos/gerentes) no CustomerX que foram modificados no Postgres.

**Fonte de Dados**:

- Query SQL direta para `contas_donoconta` com filtro `updated_at > last_sync`
- `env.postgres_replica_adapter.get_gerentes_lojas(spark)` + filtro para múltiplos campos `updated_at`

**Campos monitorados para gerentes**:

- `info_updated_at` (atendentes_informacoespessoais)
- `ativacao_updated_at` (atendentes_ativacao)
- `atendente_updated_at` (atendentes_atendente)

**Estratégia**:

- **Incremental**: Atualiza apenas registros modificados
- **Checkpoint-based**: Usa `control.customerx_sync_checkpoint`
- **Update-only**: Apenas atualiza registros existentes (não cria novos)
- **Campos atualizados**: name, email, document (CPF), phones

**Checkpoint**:

- Primeira execução: Processa TODOS os registros
- Execuções seguintes: Apenas registros modificados em qualquer campo monitorado

**Pré-requisito**: Executar `cria_contatos.py` antes da primeira execução.

#### 8. `atualiza_emails_contatos.py`

Atualiza emails de contatos no CustomerX, substituindo emails fake por emails reais.

**Objetivo**:
Quando gerentes cadastram email real após criação inicial do contato com email fake,
este notebook identifica e atualiza automaticamente os contatos no CustomerX.

**Fonte de Dados**:

- `customerx_client.fetch_all_contacts()` → Contatos com email fake no CX
- `env.postgres_replica_adapter.get_gerentes_lojas(spark)` → Gerentes com email real

**Estratégia**:

- **Busca seletiva**: Identifica apenas contatos de gerentes com email `loja_{uuid}@maggu.ai`
- **Cruzamento**: Verifica no Postgres se gerente agora tem email real cadastrado
- **Update parcial**: Atualiza apenas o campo de email (via PATCH)
- **Idempotente**: Pode ser executado regularmente sem duplicação
- **Update-only**: Não cria novos contatos, apenas atualiza existentes

**Quando executar**:

- Regularmente após `cria_contatos.py` para sincronizar novos emails
- Sob demanda quando houver campanha de coleta de emails
- Para monitoramento e melhoria da qualidade dos dados

**Benefícios**:

- 📈 Melhora progressiva da qualidade dos dados
- 📧 Substitui emails fake por reais conforme disponibilidade
- 🔄 Sincronização automática sem intervenção manual
- 📊 Relatório de taxa de atualização e emails fake restantes

**Pré-requisito**: Executar `cria_contatos.py` antes para garantir que contatos existam.

### Utilitários de Manutenção

#### 9. `reset_sandbox.py`

Remove **todos** os dados do ambiente sandbox do CustomerX (útil para testes).

**Estratégia**:

- **Destrutivo e irreversível**
- **Bloqueado para produção** (apenas sandbox)
- **Ordem de deleção** (respeitando constraints de FK):
  1. Contatos → Dependem de clientes
  1. Clientes → Dependem de grupos
  1. Grupos → Podem ser removidos por último
- Rate limiting para evitar sobrecarga da API
- Retorna estatísticas completas de cada tipo de entidade

**Quando executar**:

- Limpeza de ambiente de teste
- Reset completo antes de validação de novos fluxos
- **Manual apenas** (não incluído em pipelines automatizados)

**Nota de Segurança**: O ambiente é hardcoded para "sandbox" no código para evitar execução acidental em produção.

## Configuração

### Widgets Databricks

Todos os notebooks aceitam os seguintes widgets (definidos via `setup_customerx_notebook()`):

- `stage`: Ambiente do banco de dados (`dev`, `prod`) - herdado do padrão Databricks
- `catalog`: Catálogo Databricks (`dev`, `staging`, `production`) - herdado do padrão Databricks
- `cx_environment`: Ambiente CustomerX (`sandbox`, `production`)
- `dry_run`: Modo de teste sem criar/atualizar dados (`true`, `false`)

### Secrets

Os notebooks usam secrets do Databricks (scope `customerx`):

- `API_TOKEN_SANDBOX`: Token para ambiente sandbox
- `API_TOKEN_PRODUCTION`: Token para ambiente de produção

### Postgres Adapter

O `env.postgres_replica_adapter` é automaticamente inicializado pelo `setup_customerx_notebook()` e fornece métodos para:

- `get_contas(spark)`: Retorna contas válidas (exclui teste/ERP/obsoletas automaticamente)
- `get_lojas(spark)`: Retorna lojas válidas
- `get_gerentes_lojas(spark)`: Retorna gerentes ativos (um por loja)
- `read_table(spark, table)`: Leitura genérica de tabelas
- `read_query(spark, query)`: Execução de queries SQL customizadas

## Ordem de Execução

### Setup Inicial (criação completa do zero):

```bash
# Notebooks de criação (create-only)
1. cria_grupos.py (cx_environment=sandbox, dry_run=false)
2. cria_matrizes_filiais.py (cx_environment=sandbox, dry_run=false)
3. associa_matrizes_filiais.py (cx_environment=sandbox, dry_run=false)
4. cria_contatos.py (cx_environment=sandbox, dry_run=false)
```

### Sincronização Regular (atualização incremental):

```bash
# Notebooks de atualização (update-only) - executam via checkpoint
1. atualiza_grupos.py
2. atualiza_matrizes_filiais.py
3. atualiza_contatos.py
4. atualiza_emails_contatos.py
```

### Orquestração

**Nota**: Atualmente não existe um workflow Terraform configurado para CustomerX.
A execução dos notebooks pode ser feita:

- Manualmente no Databricks UI
- Via Databricks Workflows (a ser configurado)
- Ou através de chamadas API do Databricks

Para implementar orquestração automatizada, será necessário criar um arquivo
`terraform/pipelines/sync_customerx.tf` seguindo o padrão dos outros pipelines.

## Bibliotecas Utilizadas

### Módulos CustomerX (`maggulake/customerx/`)

- `notebook_setup.py`: Setup centralizado de notebooks
  - `setup_customerx_notebook()`: Inicializa ambiente, API client e Postgres adapter
- `constants.py`: Constantes da integração (FAKE_EMAIL_DOMAIN, etc.)
- `checkpoint.py`: Gerenciamento de checkpoints incrementais
  - `get_last_sync()`: Obtém timestamp da última sincronização
  - `update_checkpoint()`: Atualiza checkpoint após execução
- `reporting.py`: Utilitários de logging e relatórios
  - `ErrorAccumulator`: Acumula e salva erros em batch
  - Schemas para sumários e rastreamento de erros

### DTOs e Modelos (`maggulake/utils/integracoes/customerx/models/`)

- `conta.ContaDTO`: Modelo para contas/redes (matrizes)
  - `from_postgres_row()`: Converte row do Postgres para DTO
  - `to_customerx_payload()`: Gera payload para API CustomerX
- `loja.LojaDTO`: Modelo para lojas (filiais)
  - `from_postgres_row()`: Converte row do Postgres para DTO
  - `to_customerx_payload()`: Gera payload para API CustomerX
- `contact.ContactDTO`: Modelo para contatos (donos/gerentes)
  - `from_dono_row()`: Converte dono do Postgres para DTO
  - `from_atendente_loja_row()`: Converte gerente do Postgres para DTO
  - `to_customerx_payload()`: Gera payload para API CustomerX
- `group.GroupDTO`: Modelo para grupos
  - Validações e conversões para payload CustomerX
- `customer.CustomerXCustomerDTO`: Modelo para clientes retornados da API CustomerX

### Cliente API (`maggulake/utils/integracoes/customerx/client.py`)

- `CustomerXClient`: Cliente HTTP para API CustomerX
  - `fetch_all_customers()`: Busca todos os clientes com paginação
  - `fetch_all_contacts()`: Busca todos os contatos com paginação
  - `fetch_all_groups()`: Busca todos os grupos com paginação
  - `criar_cliente_com_payload()`: Cria cliente (conta/loja)
  - `criar_contato()`: Cria contato (dono/gerente)
  - `criar_grupo()`: Cria grupo
  - `associar_filiais()`: Associa filiais a matriz
  - `atualizar_cliente()`: Atualiza cliente existente (PUT)
  - `atualizar_contato()`: Atualiza contato existente (PUT)
  - Rate limiting automático
  - Retry com exponential backoff
  - Não faz retry em erros 4xx (client errors)

### Reset/Limpeza (`maggulake/utils/integracoes/customerx/reset.py`)

- `CustomerXSandboxReset`: Gerenciador de reset de ambiente sandbox
  - `reset_environment()`: Remove todos os contatos, clientes e grupos
  - Deleta em ordem (contatos → clientes → grupos) respeitando FKs
  - Rate limiting automático para evitar sobrecarga da API

### Utilitários (`maggulake/utils/strings.py`)

- `limpa_cnpj()`: Limpa formatação de CPF/CNPJ
- `validate_email_return_none_if_fail()`: Valida email
- `standardize_phone_number()`: Padroniza telefone

## Testes

Testes unitários estão em `tests/customerx/`:

- `test_client.py`: Testes do CustomerXClient
- `test_contact.py`: Testes do ContactDTO (validações, conversões, payloads)
- `test_reset.py`: Testes do script de reset

Execute os testes:

```bash
pytest tests/customerx/
```

## Tabelas de Controle

### `{catalog}.raw.customerx_sync_checkpoint`

Armazena checkpoints de sincronização incremental:

**Schema**:

- `notebook_name` (String): Nome do notebook que executou a sync
- `last_sync_at` (Timestamp): Timestamp da última sincronização
- `status` (String): Status da execução (`success`, `failed`, `running`)
- `records_processed` (Integer): Quantidade de registros processados
- `error_message` (String): Mensagem de erro se status = failed
- `updated_at` (Timestamp): Timestamp da atualização do checkpoint

**Uso**: Permite sincronização incremental (apenas registros modificados desde last_sync)

### `{catalog}.raw.customerx_integration_errors`

Armazena erros ocorridos durante sincronização:

**Schema**:

- `notebook_name` (String): Nome do notebook que gerou o erro
- `operation_type` (String): Tipo de operação (ex: "criar_cliente", "atualizar_contato")
- `entity_id` (String): ID da entidade que causou erro
- `entity_name` (String): Nome da entidade (opcional)
- `error_message` (String): Mensagem de erro completa
- `error_timestamp` (Timestamp): Timestamp do erro
- `catalog` (String): Catálogo onde ocorreu o erro

**Uso**: Auditoria e troubleshooting de erros de integração

**TODO**: Implementar limpeza periódica de erros com mais de 1 ano

## Documentação da API

Base URL:

- Sandbox: `https://sandbox.api.customerx.com.br`
- Produção: `https://api.customerx.com.br`

Documentação completa: [CustomerX API Docs](https://doc.api.customerx.com.br/)

### Limites da API

- **Rate Limiting**: Cliente monitora headers `X-RateLimit-Remaining` e `X-RateLimit-Reset`
- **Paginação**: Máximo 20 registros por página
- **Retries**: Máximo 3 tentativas com exponential backoff
- **Timeout**: 60 segundos por requisição
- **Client Errors (4xx)**: Não faz retry (erros de payload/validação)

## Fluxo de Dados Completo

```
Postgres (Origem)
    ↓
PostgresReplicaAdapter (Filtros automáticos)
    ↓
DTOs (Validação e transformação)
    ↓
CustomerXClient (API HTTP)
    ↓
CustomerX (Destino)
    ↓
Checkpoint/Errors (Auditoria)
```

### Filtros Aplicados Automaticamente

O `PostgresReplicaAdapter` aplica os seguintes filtros em todas as queries:

- **Contas**: Exclui contas em `EXCLUDED_ACCOUNT_PATTERNS` (toolspharma, automatiza, hos, asasys, etc.)
- **Contas**: Exclui contas com nome iniciado com "obsoleto"
- **Lojas**: Apenas lojas com `ativo = true`
- **Lojas**: Exclui lojas de contas em `EXCLUDED_ACCOUNT_PATTERNS`
- **Lojas**: Exclui lojas com nome iniciado com "obsoleto"
- **Gerentes**: Apenas gerentes com `is_active = true`
- **Gerentes**: Apenas lojas com `ativo = true`
- **Gerentes**: Um gerente por loja (mais recente)
