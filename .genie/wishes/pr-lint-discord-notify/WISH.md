# Wish: Notificação Discord em Falhas de Format/Lint no PR

| Field      | Value                    |
| ---------- | ------------------------ |
| **Status** | DRAFT                    |
| **Slug**   | `pr-lint-discord-notify` |
| **Date**   | 2026-03-19               |

## Summary

Adiciona um step no GitHub Actions que, quando qualquer job de format ou lint falhar no CI do PR, envia automaticamente uma notificação para um canal do Discord via API do Omni. O objetivo é reduzir o tempo de detecção de falhas de qualidade de código, sem que o desenvolvedor precise ficar olhando a aba de Actions.

## Scope

### IN

- Step `notify-discord-on-failure` adicionado ao workflow `.github/workflows/linters.yml`
- Notificação disparada quando qualquer dos jobs falha: `ruff`, `pylint`, `markdown-format`, `markdown-lint`, `terraform-fmt-check`, `terraform-validate`
- Mensagem inclui: nome do PR, autor, URL direto para o workflow run, e qual(is) job(s) falhou
- Secret `OMNI_DISCORD_WEBHOOK_URL` adicionado à documentação de setup (não hardcoded)
- Notificação enviada via `curl` para a API do Omni (HTTP POST com JSON)

### OUT

- Notificação de sucesso (apenas falhas disparam o aviso)
- Modificação dos jobs de lint em si (só adiciona o step de notificação)
- Criação de um bot Discord separado ou código Python novo
- Integração com Slack ou outros canais
- Notificações para o workflow de `tests.yml` (escopo limitado ao `linters.yml`)

## Decisions

| Decision                                             | Rationale                                                                                                     |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Step de notificação no próprio `linters.yml`         | Evita criar um novo workflow; o contexto do job falho já está disponível (`needs` + `if: failure()`)          |
| `curl` em vez de action de marketplace               | Zero dependências externas; a API do Omni é um HTTP POST simples                                              |
| Job separado `notify-on-failure`                     | Um único job de notificação com `needs` em todos os jobs de lint garante que dispara ao primeiro erro         |
| Secret `OMNI_DISCORD_WEBHOOK_URL` via GitHub Secrets | Segue o padrão do projeto: nunca hardcode credenciais; acessado com `${{ secrets.OMNI_DISCORD_WEBHOOK_URL }}` |
| Mensagem em português                                | Time é brasileiro; consistente com os outros avisos do projeto                                                |

## Success Criteria

- [ ] Ao falhar qualquer job de lint/format no PR, o GitHub Actions dispara o job `notify-on-failure`
- [ ] O job `notify-on-failure` executa apenas se pelo menos um job anterior falhou (`if: failure()`)
- [ ] A mensagem enviada ao Discord contém: título do PR, nome do autor, URL do run, e lista de jobs que falharam
- [ ] Quando todos os jobs passam, o job `notify-on-failure` é pulado (não envia mensagem)
- [ ] A URL da API do Omni é lida de `secrets.OMNI_DISCORD_WEBHOOK_URL` (nunca hardcoded)
- [ ] `make format && make lint && make test` continuam passando após a mudança

## Execution Strategy

### Wave 1 (sequencial — grupo único)

| Group | Agent    | Description                                                        |
| ----- | -------- | ------------------------------------------------------------------ |
| 1     | engineer | Adicionar job `notify-on-failure` ao `linters.yml` com step `curl` |

### Wave 2 (após Wave 1)

| Group  | Agent    | Description                                                         |
| ------ | -------- | ------------------------------------------------------------------- |
| review | reviewer | Revisar YAML, validar `if: failure()`, checar ausência de hardcodes |

## Execution Groups

### Group 1: Adicionar Job de Notificação ao linters.yml

**Goal:** Inserir um job `notify-on-failure` que depende de todos os jobs de lint e, em caso de falha, envia HTTP POST para a API do Omni com detalhes do PR.

**Deliverables:**

1. Job `notify-on-failure` no `.github/workflows/linters.yml` com:
   - `needs: [ruff, pylint, markdown-format, markdown-lint, terraform-validate, terraform-fmt-check]`
   - `if: failure()`
   - Step `curl` que monta o payload JSON e chama `${{ secrets.OMNI_DISCORD_WEBHOOK_URL }}`
1. Payload da mensagem inclui:
   - `${{ github.event.pull_request.title }}`
   - `${{ github.actor }}`
   - `${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}`
   - Lista de quais jobs falharam (via `needs.<job>.result`)

**Acceptance Criteria:**

- [ ] Job `notify-on-failure` presente no YAML com `if: ${{ failure() }}`
- [ ] `needs` lista todos os 6 jobs de lint
- [ ] Nenhuma URL ou token hardcoded no YAML
- [ ] `make lint` (pymarkdownlnt no YAML) não levanta erro sobre o arquivo modificado

**Validation:**

```bash
# Validar que o secret não está hardcoded
grep -v 'secrets.OMNI_DISCORD_WEBHOOK_URL' .github/workflows/linters.yml | grep -qiE '(discord|omni|webhook|http)' && echo "FAIL: possível hardcode" || echo "OK: sem hardcode"

# Validar que o job notify-on-failure existe e tem if: failure()
grep -q 'notify-on-failure' .github/workflows/linters.yml && echo "OK: job existe"
grep -q "failure()" .github/workflows/linters.yml && echo "OK: if failure() presente"

# Validar que make lint e make test ainda passam
make lint && make test
```

**depends-on:** none

______________________________________________________________________

## QA Criteria

- [ ] Simular falha de lint num PR de teste: o job `notify-on-failure` aparece como executado na aba Actions
- [ ] A mensagem recebida no Discord contém o título do PR e a URL do run
- [ ] Simular PR com todos os checks passando: `notify-on-failure` aparece como `skipped`
- [ ] Workflows existentes (`tests.yml`, `check-is-rebased.yml`) não foram alterados

______________________________________________________________________

## Assumptions / Risks

| Risk                                                                        | Severity | Mitigation                                                                                |
| --------------------------------------------------------------------------- | -------- | ----------------------------------------------------------------------------------------- |
| API do Omni pode ter formato de payload diferente do Discord webhook padrão | Medium   | Ajustar o JSON do `curl` conforme a documentação do Omni (campo `content` vs outro campo) |
| Secret `OMNI_DISCORD_WEBHOOK_URL` precisa ser criado no repositório GitHub  | Low      | Documentar no PR como adicionar o secret em Settings → Secrets → Actions                  |
| Em forks externos, secrets não estão disponíveis (Actions de PRs de forks)  | Low      | Fora do escopo; o repo é privado e PRs são internos                                       |
| `if: failure()` no job-level avalia falha de qualquer job em `needs`        | Low      | Comportamento esperado — já é o que queremos                                              |

______________________________________________________________________

## Review Results

_Populado por `/review` após execução._

______________________________________________________________________

## Files to Create/Modify

```
.github/workflows/linters.yml    # Adicionar job notify-on-failure ao final
```
