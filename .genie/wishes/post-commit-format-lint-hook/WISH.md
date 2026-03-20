# Wish: Post-Commit Hook com Prompt Interativo para Format/Lint

| Field      | Value                          |
| ---------- | ------------------------------ |
| **Status** | DRAFT                          |
| **Slug**   | `post-commit-format-lint-hook` |
| **Date**   | 2026-03-19                     |

## Summary

Cria um git hook `post-commit` que, após cada commit, pergunta interativamente no terminal WSL se o usuário deseja rodar `make format` e/ou `make lint` antes de dar push. O objetivo é facilitar a execução do pipeline de qualidade de código sem torná-la obrigatória.

## Scope

### IN

- Script `post-commit` em bash, colocado em `.git/hooks/post-commit`
- Prompt interativo que pergunta separadamente se quer rodar `make format` e `make lint`
- Compatibilidade com terminal WSL (leitura de stdin via `/dev/tty`)
- Exibição do output dos comandos em tempo real
- Script executável (chmod +x)

### OUT

- Bloqueio do commit ou push em caso de falha no format/lint (hook não é bloqueante)
- Hook `pre-push` (o hook é pós-commit, não pré-push)
- Modificações em CI/CD ou GitHub Actions
- Instalação via ferramenta de gerenciamento de hooks (husky, lefthook, etc.)
- Suporte a sistemas operacionais além de WSL/Linux

## Decisions

| Decision                               | Rationale                                                                                               |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `post-commit` (não `pre-push`)         | Executa imediatamente após o commit, antes de qualquer push — o usuário pode corrigir no mesmo contexto |
| Leitura via `/dev/tty`                 | Em WSL, stdin do hook pode ser redirecionado; `/dev/tty` garante leitura do terminal real               |
| Perguntas separadas para format e lint | Permite rodar só um deles quando necessário (ex: só format se já sabe que lint passa)                   |
| Bash puro (sem dependências externas)  | Hook deve funcionar sem instalar nada além do que já existe no repo                                     |
| Hook não-bloqueante                    | O usuário decide; não queremos forçar nem interromper o fluxo de git                                    |

## Success Criteria

- [ ] Após `git commit`, o terminal exibe prompt perguntando se quer rodar `make format`
- [ ] Após resposta `s`/`S`/`y`/`Y`, `make format` é executado e o output aparece no terminal
- [ ] Após `make format`, o terminal exibe prompt perguntando se quer rodar `make lint`
- [ ] Após resposta `s`/`S`/`y`/`Y`, `make lint` é executado e o output aparece no terminal
- [ ] Respostas negativas (`n`/`N` ou Enter) pulam o comando sem erro
- [ ] O hook não quebra fluxo normal do git (exit code 0 sempre)
- [ ] Funciona corretamente em terminal WSL

## Execution Strategy

### Wave 1 (sequencial — grupo único)

| Group | Agent    | Description                             |
| ----- | -------- | --------------------------------------- |
| 1     | engineer | Criar e instalar o script `post-commit` |

### Wave 2 (após Wave 1)

| Group  | Agent    | Description                                                  |
| ------ | -------- | ------------------------------------------------------------ |
| review | reviewer | Revisar script, testar interatividade WSL, validar critérios |

## Execution Groups

### Group 1: Criar Hook Post-Commit

**Goal:** Escrever e instalar o script bash `post-commit` com prompt interativo compatível com WSL.

**Deliverables:**

1. Arquivo `.git/hooks/post-commit` com permissão de execução
1. (Opcional) Cópia do script em `.githooks/post-commit` para versionamento — com instrução no README ou CLAUDE.md de como instalar

**Acceptance Criteria:**

- [ ] Arquivo existe em `.git/hooks/post-commit` e é executável
- [ ] Script usa `/dev/tty` para leitura de input (compatibilidade WSL)
- [ ] Aceita `s`, `S`, `y`, `Y` como confirmação; qualquer outra coisa (incluindo Enter) como negação
- [ ] Cada comando (format e lint) é perguntado separadamente
- [ ] Exit code do hook é sempre 0 (não bloqueante)

**Validation:**

```bash
# Verificar existência e permissão
test -x .git/hooks/post-commit && echo "OK: hook existe e é executável"

# Verificar que usa /dev/tty
grep -q '/dev/tty' .git/hooks/post-commit && echo "OK: usa /dev/tty"

# Smoke test manual (não automatizável): git commit --allow-empty -m "test hook"
```

**depends-on:** none

______________________________________________________________________

## QA Criteria

- [ ] Após `git commit --allow-empty -m "test"`, o terminal pergunta sobre `make format`
- [ ] Digitando `s` + Enter roda `make format` e exibe output
- [ ] Digitando `n` + Enter pula para a pergunta de `make lint`
- [ ] Digitando apenas Enter (sem texto) pula o comando
- [ ] O fluxo completo não deixa o terminal em estado broken/travado

______________________________________________________________________

## Assumptions / Risks

| Risk                                                                            | Severity | Mitigation                                                                 |
| ------------------------------------------------------------------------------- | -------- | -------------------------------------------------------------------------- |
| `.git/hooks/` não é versionado — outros devs não recebem o hook                 | Low      | Adicionar cópia em `.githooks/` e instrução de instalação                  |
| Em GUIs git (GitKraken, VSCode Source Control) o hook pode não ter acesso a TTY | Low      | Fora do escopo; hook é para uso CLI no WSL                                 |
| `make format` pode alterar arquivos após o commit                               | Medium   | Documentar que o usuário precisará fazer um novo commit se houver mudanças |

______________________________________________________________________

## Files to Create/Modify

```
.git/hooks/post-commit          # Hook principal (não versionado)
.githooks/post-commit           # Cópia versionada do hook (recomendado)
```
