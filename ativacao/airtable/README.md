# Integracao do Airtable

No primeiro semestre de 2025 fizemos uma integracao entre o Airtable e o nosso banco de dados.

As seguintes

- Usuarios (ou atendentes)
- Campanhas
- Missoes

Nossas integracoes funcionavam, resumidamente, da seguinte forma:

- Criar usuarios no airtable quando eles surgirem no copilot
- Atualizar usuarios do copilot com base nos dados do airtable
- Fazer o mesmo para missoes e campanhas.

Por n motivos achamos melhor derrubar a integracao.
O airtable continua sendo usado pelo time de negocios, porem essa informacao nao esta sincronizada com o postgres.

Os codigos foram portanto removidos do datalake para nao gerar confusao, porem podem ser resgatados pela arvore do git.

UPDATE: o v2 serve para quando usamos a planilha onde a Mariana já deixa os record_ids das lojas, assim fica mais facil a operação.
https://docs.google.com/spreadsheets/d/1bztDhIr_KDm4xPbY6Zh6_YHzMXVYBzN_6boj9SiDJYk/edit?gid=0#gid=0

UPDATE:

- em setembro de 2025 decidimos desligar o airtable de vez.
- Os dados foram salvos numa planilha Backup_Airtable (https://docs.google.com/spreadsheets/d/1a3Ueq29IefC9R1spGIV_iFNMnE2Rp7RPhjM37Bs9Zxs/edit?gid=2037191039#gid=2037191039)
- Os dados foram passados pro banco e, a partir daí, todas as alterações foram feitas no admin.
