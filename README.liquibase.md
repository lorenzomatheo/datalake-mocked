# Liquibase

Nosso gestor de migrações no datalake.

## Instalação

Primeiro vamos precisar de 2 informações: o httpPath do seu cluster e um token do Databricks.

Para achar o httpPath, entre no Databricks, clique no Compute no menu, e clique no seu cluster.
Na primeira aba mesmo, lá embaixo nas configurações avançadas, clique em "JDBC/ODBC".
Anote o "HTTP Path".

Para criar um token, clique no ícone do seu usuário em cima à direita, e vá para Settings.
Lá, encontre o menu Developer.
Depois clique em Access token e siga as instruções.

Depois, crie um arquivo chamado `.env.liquibase` na raíz do projeto, no seguinte formato:

```bash
DATABRICKS_HTTP_PATH="{seu httpPath}"
DATABRICKS_TOKEN={seu token}
```

### Utilização

Tem um script chamado `liquibase.sh` na raiz do projeto que deve ser usado para rodar os comandos.
Ele vai chamar o programa liquibase com as configuracoes do projeto mais o catálogo que for passado como primeiro argumento.
Se, por exemplo, você quiser ver o histórico de migrações em staging, use:

```sh
./liquibase.sh staging history
```

Comandos mais utilizados são `history`, `status`, `validate` e `update`.
Existem muitos outros comandos, e voce pode rodar `./liquibase.sh staging --help` para descobri-los.
Cuidado, alguns comandos escrevem o changelog-file ao invés de lê-lo. Mas sempre dá pra reverter com `git checkout HEAD liquibase/root_changelog.yml`

### Gerando migrações

As migrações estão organizadas por schema na pasta liquibase.
Para alterar a tabela `production.refined.produtos_refined`, deverá ser alterado/criado o arquivo `liquibase/refined/produtos_refined.sql`.
Se nem a pasta do schema existir, além de criá-la, descomente as linhas pertinentes a esse schema no arquivo `root_changelog.yml`.

As migrações tem o formato:

```sql
--changeset usuário:id
CREATE TABLE ...
```

Lembre-se de por seu nome no usuário, e de sempre manter o id sequencial
(dentro de um mesmo arquivo, cada nova migração deverá ter um id um número
maior que a anterior). Garanta que seu usuário seja único, dê preferência
pelo seu username do GitHub. O arquivo `liquibase/utils/exemplo.sql` serve
de exemplo.

Para testar se a atualização está OK, use:

```sh
./liquibase.sh staging validate
```

Para subir as alterações em um ambiente, rode:

```sh
./liquibase.sh staging update
```

Obs.:

- Lembre-se sempre de rodar em staging primeiro, depois em prod.
- Se você criar uma migração nas pastinhas mas não executar, a próxima pessoa
  que rodar o liquibase vai acabar re-executando a migração indevidamente, o
  que _provavelmente_ vai dar erro e cancelar a operação.
