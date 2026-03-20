# Terraform

Infraestrutura como codigo.

Importante:

- Alterações realizadas na UI do databricks nao serao automaticamente alteradas no terraform.
- Simplesmente alterar o .tf nao reflete mudanças no databricks, precisa executar os comandos abaixo.
- Siga corretamente a sintaxe do terraform pois ele eh bastante sensível a pequenas falhas.

## Requisitos

- Instalar terraform (https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
- Instalar e configurar AWS CLI
- Gerar token de acesso no databricks

## Passo a passo

### Configurando

Adicionar o token do databricks ao .env.default
Alternativamente, voce pode instalar a extensão do databricks no vscode.

Garanta que o terraform e o aws cli estão funcionando (`terraform -help` e `aws --version`).

Também é necessário criar um arquivo `.databricks.cfg` na pasta root do repositório.
Adicionar o seguinte conteúdo:

```text
[DEFAULT]
host = https://*****************.cloud.databricks.com/
token = ************************************
```

### Aplicando todos os tf

```bash
cd terraform/
terraform init         # instala dependências
terraform validate     # validacao básica de sintaxe
terraform plan         # planeja as atualizações a serem feitas
terraform apply        # aplica as alterações. Rodar somente se o comando anterior resultar sem erros.
```

### Aplicando parcialmente os tf

Caso queira atualizar, por exemplo, somente os pipelines do databricks:

```bash
terraform plan -target=modules.pipelines
```

Voce deve ver uma mensagem deste tipo:

```text
Plan: 0 to add, 4 to change, 0 to destroy.
```

Caso através da mensagem acima voce perceber que o terraform vai adicionar um novo
workflow em vez de alterar o atual, pode ser que alguma coisa tenha dado errado.

Rodar assim para atualizar apenas um pipeline:

```bash
terraform apply -target=module.pipelines.module.nome_do_pipeline
```

Edit Set/2025:

- Obs.: Para autenticar no databricks `/usr/local/bin/databricks auth login --host https://dbc-25297b38-ec1c.cloud.databricks.com`
- Clicar enter para fazer login no databricks pelo navegador
- Rodar o terraform apply novamente

### Criando clusters para novos usuários

Chegou alguém no time e precisa criar um cluster para ele? Alguém apagou seu cluster enquanto você não estava olhando? Não se preocupe, siga os passos abaixo:

1. Adicione o usuário ao grupo developers no databricks (somente admins podem fazer isso).

O terraform está configurado para criar clusters automaticamente para todos os usuários do grupo `developers` do databricks.

2. Execute os comandos abaixo:

```bash
cd terraform/
terraform init         # instala dependências
terraform validate     # validacao básica de sintaxe
terraform apply -target=module.clusters  # digite 'yes' se estiver tudo ok
```
