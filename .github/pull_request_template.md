## Descricao

<!-- Escreva resumidamente do que se trata esse PR, caso o titulo ja nao seja autoexplicativo. -->

## Checklist (marcar todos antes de mergear)

- [ ] Atualizar pipeline no terraform (caso necessário)
- [ ] Solicitar review de 2 pessoas do time de dados
- [ ] Adicionar print com evidência de execução
- [ ] Adicionar print do impacto nos pipelines de eval

## Evidências

### Execução

<!-- Adicione aqui o print/screenshot da execução -->

### Impacto nos Pipelines de Eval

<!-- Adicione aqui o print/screenshot do impacto nos pipelines de eval -->

## Guidelines Gerais

### Antes do Merge

- Confirmar que o código foi formatado (`make format`)
- Executar `update with rebase` antes de dar merge
- Verificar se todos os checks de CI/CD passaram
- Preferir `squash and merge` para manter a árvore do git linear
- Para alterações de tabelas, validar migrações do Liquibase
