#! /bin/bash

set -eou pipefail

if [[ -z "$1" || "$1" == "-h" || "$1" == "--help" ]]; then
    echo "Como usar: ./liquibase catalogo {comando liquibase}"
    echo "Ex: ./liquibase staging status"
    echo "Comandos comuns sao history, status, upgrade"
    echo "Para outros comandos, checar o --help pra ver ser o --changelog-file e escrita ou leitura. Evite sobrescrever o changelog-file"
    echo "  -h, --help    Mostra essa mensagem"
    exit 0
fi

CATALOG=$1
URL="jdbc:databricks://$DATABRICKS_SERVER_HOSTNAME/default;transportMode=http;ssl=1;httpPath=$DATABRICKS_HTTP_PATH;AuthMech=3;UID=token;PWD=$DATABRICKS_TOKEN;ConnCatalog=$CATALOG;"

shift

/liquibase/liquibase --url=$URL --changelog-file=${CHANGELOG_FILE:-root_changelog.yml} "$@"
