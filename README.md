# Datalake

Data lakehouse Databricks com arquitetura medallion para o ecossistema farmacêutico.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/WSL
# ou .venv\Scripts\activate  # Windows

make install
```

## Qualidade de código

### Hook post-commit (local)

Após cada `git commit`, o hook `.git/hooks/post-commit` pergunta interativamente se você quer rodar `make format` e `make lint` antes de dar push:

```text
Quer rodar 'make format' antes de dar push? [s/N]
Quer rodar 'make lint' antes de dar push? [s/N]
```

O hook é **não-bloqueante** — se você recusar, o commit já foi feito normalmente.

#### O que cada comando faz

**`make format`** corrige a formatação automaticamente:

- `ruff check --select I --fix` — organiza imports
- `ruff format` — formata código Python
- `terraform fmt -recursive terraform` — formata arquivos Terraform
- `mdformat .` — formata arquivos Markdown

**`make lint`** verifica erros sem corrigir:

- `ruff check` — linting rápido (estilo, imports, erros comuns)
- `pylint --rcfile=.pylintrc .` — análise estática mais completa
- `pymarkdownlnt --config .pymarkdown.json scan .` — lint de Markdown

#### Instalação do hook

Copie o arquivo para `.git/hooks/`:

```bash
cp hooks/post-commit .git/hooks/post-commit
chmod +x .git/hooks/post-commit
```

### CI — GitHub Actions (remoto)

Ao abrir ou atualizar um Pull Request, o workflow `.github/workflows/linters.yml` roda automaticamente os mesmos checks em paralelo:

| Job                   | Runner              | O que faz                            |
| --------------------- | ------------------- | ------------------------------------ |
| `ruff`                | `ubuntu-latest`     | `ruff check` + `ruff format --check` |
| `pylint`              | `ubuntu-latest`     | `pylint --rcfile=.pylintrc .`        |
| `markdown-format`     | `ubuntu-latest`     | `mdformat --check .`                 |
| `markdown-lint`       | `ubuntu-latest`     | `pymarkdownlnt scan .`               |
| `terraform-validate`  | `ubuntu-latest`     | `terraform validate`                 |
| `terraform-fmt-check` | `ubuntu-latest`     | `terraform fmt -check`               |
| `notify-on-failure`   | `self-hosted, omni` | Envia alerta no Discord via Omni     |

O workflow é acionado quando há mudanças em arquivos `.py`, `.ipynb`, `.md`, `.tf`, `pyproject.toml`, `requirements*`, `.pylintrc` ou `.github/**`.

### Notificação de falha no Discord

Se **qualquer job de lint falhar**, o job `notify-on-failure` é executado em um **self-hosted runner** (label `omni`) que tem acesso à rede privada onde o servidor Omni está rodando.

O fluxo completo:

```text
PR aberto/atualizado
  └─► GitHub Actions roda linters (ubuntu-latest)
        └─► Algum linter falha
              └─► Job notify-on-failure (self-hosted runner, rede privada)
                    └─► curl POST para Omni API (/api/v2/messages/send)
                          └─► Omni envia mensagem para o canal Discord
```

A mensagem no Discord contém:

- Nome do PR
- Autor do push
- Quais jobs falharam
- Link para os detalhes do run

#### Configuração necessária

**GitHub Secrets** (Settings > Secrets and variables > Actions):

| Secret                     | Descrição                 | Como obter                                                        |
| -------------------------- | ------------------------- | ----------------------------------------------------------------- |
| `OMNI_BASE_URL`            | URL do servidor Omni      | `hostname -I` na máquina do Omni (ex: `http://192.168.1.50:8882`) |
| `OMNI_API_KEY`             | API key do Omni           | `omni auth status` ou `cat ~/.omni/config.json`                   |
| `OMNI_DISCORD_INSTANCE_ID` | UUID da instância Discord | `omni instances list`                                             |
| `OMNI_DISCORD_CHANNEL_ID`  | ID do canal Discord       | Botão direito no canal > Copiar ID                                |

```bash
gh secret set OMNI_BASE_URL --body "http://<host>:8882"
gh secret set OMNI_API_KEY --body "<api-key>"
gh secret set OMNI_DISCORD_INSTANCE_ID --body "<uuid>"
gh secret set OMNI_DISCORD_CHANNEL_ID --body "<id-do-canal>"
```

**Self-hosted runner** (máquina na rede privada com acesso ao Omni):

```bash
# Gerar token de registro
gh api repos/<owner>/<repo>/actions/runners/registration-token -X POST

# Instalar e configurar o runner
mkdir ~/actions-runner && cd ~/actions-runner
curl -o actions-runner-linux-x64-2.322.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.322.0/actions-runner-linux-x64-2.322.0.tar.gz
tar xzf actions-runner-linux-x64-2.322.0.tar.gz
./config.sh --url https://github.com/<owner>/<repo> --token <TOKEN> --labels omni
./run.sh
```

**Bot Discord** (precisa estar no servidor e ter acesso ao canal):

1. [Discord Developer Portal](https://discord.com/developers/applications) > selecionar o bot
1. OAuth2 > marcar scope `bot` > marcar permissões `Send Messages` e `View Channels`
1. Abrir a URL gerada e autorizar no servidor
1. Se o canal for privado: Editar canal > Permissões > adicionar o bot

**Instância Omni**:

```bash
omni instances create --name "alerts-discord" --channel discord
omni instances connect <instance-id> --token "<DISCORD_BOT_TOKEN>"
```

## Genie — Orquestrador de tarefas

O diretório `.genie/` é o orquestrador de tarefas do projeto. Ele define especificações de trabalho ("wishes"), rastreia o estado de execução e coordena workers via mailbox.

### Estrutura

```text
.genie/
├── wishes/          # Especificações de tarefas (WISH.md)
│   ├── post-commit-format-lint-hook/
│   │   └── WISH.md
│   └── pr-lint-discord-notify/
│       └── WISH.md
├── state/           # Estado de execução (JSON)
│   └── pr-lint-discord-notify.json
└── mailbox/         # Mensagens entre workers (JSON)
    └── new-project-team-lead.json
```

### Wishes

Cada wish é uma especificação completa de uma tarefa, contendo:

- **Summary** — o que deve ser feito
- **Scope** (IN/OUT) — o que está dentro e fora do escopo
- **Decisions** — decisões técnicas com justificativa
- **Success Criteria** — checklist de critérios de aceite
- **Execution Groups** — divisão em waves e grupos com agentes responsáveis
- **QA Criteria** — como validar que a tarefa foi concluída

### State

Arquivos JSON em `state/` rastreiam o progresso de cada wish:

```json
{
  "wish": "pr-lint-discord-notify",
  "groups": {
    "1": {
      "status": "done",
      "assignee": "engineer"
    }
  }
}
```

Status possíveis: `pending`, `in_progress`, `done`.

### Mailbox

Workers se comunicam via `mailbox/`. Cada arquivo JSON contém mensagens com remetente, destinatário e corpo — usado para notificar sobre fixes aplicados, resultados de reviews, etc.
