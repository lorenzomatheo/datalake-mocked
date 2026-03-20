# Maggu Datalake

## Estrutura do Datalake

Nosso armazenamento segue a seguinte estrutura:

| Número | Camada    | Descrição                                                                                                                                | Exemplo                                               |
| ------ | --------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| 1      | raw       | *Dados brutos*. Não possui especificação de formato, portanto, pode ser arquivos `.csv`, `.json`, `.xml`, etc.                           | tabela `produtos_raw` (antiga `produtos_bronze`)      |
| 2      | standard  | *Dados oficiais, estruturados e otimizados*. O dado deve ser guardado em `.parquet` ou em `delta_tables` seguindo os schemas do pyspark. | tabela `produtos_standard` (antiga `produtos_silver`) |
| 3      | refined   | Operações nas tabelas otimizadas são feitas aqui.                                                                                        | tabela `produtos_refined` (antiga `produtos_gold`)    |
| 4      | analytics | Em teoria são dados que consumimos a partir de dashboards ou outras formas de "visualização" dos dados.                                  | ...                                                   |

## Revisões

Na sprint 17 (2024) fizemos uma refatoração da estrutura do nosso datalake (vide [notion](https://www.notion.so/Revis-o-da-estrutura-da-tabela-de-produtos-f377fd1868ce4068970cc6ec4d6c895c?pvs=4)).

Optamos por alterar os nomes das nossas camadas da seguinte forma:

- Bronze -> Raw
- Silver -> Standard
- Gold -> Refined
- Analytics

## Setup

Voce provavelmente vai querer rodar os notebooks pelo
[databricks](https://accounts.cloud.databricks.com/),
porém algumas etapas de desenvolvimento são mais fáceis através da sua IDE preferida.
Vamos aos passos.

### 0. Instalar software necessário

Instale uma versão suportada do Python:

- [Python](https://www.python.org/downloads/)
- Consulte as versões ativas no Guia de Versões do Python.
- Importante: Evite versões de pré-lançamento (pre-releases como alpha, beta),
  pois elas frequentemente não possuem compatibilidade com todas as bibliotecas
  do projeto (falta de wheels pré-compilados).
- Clonar esse repositório:
  Abra o terminal na pasta desejada e execute
  `git clone https://link-do-seu-repo-aqui.git`

### 1. Criar um ambiente virtual

Essa etapa eh opcional porem altamente recomendada.

```bash
python3 -m venv .venv
```

Agora ativar o ambiente que acabou de ser criado:

```bash
# Linux
source ./.venv/bin/activate
```

```powershell
# Windows (dê preferência para o linux, mas caso seja teimoso)
.\.venv\Scripts\Activate
```

### 2. Instalando pacotes necessários

> Obs.: Veja o comando "install" no arquivo Makefile

Instale os pacotes para ativar o _Intellisense_, _type checking_ e também para executar os arquivos.

```bash
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.in
python3 -m pip install -r requirements-dev.in
```

### 3. Makefile

Muitos comandos e alguns hacks estão documentados no nosso Makefile.
[Make](https://www.gnu.org/software/make/manual/html_node/Introduction.html) eh
um programa do GNU que nos permite salvar alguns comandos e instrucoes.

Alguns exemplos de comandos que podem te ajudar sao:

- `make format`: executa formatação do codigo
- `make pylint`: executa o pylint ja com algumas regras pre-definidas.

A ideia do Make eh facilitar nossa vida, mas voce pode rodar os comandos normalmente sem utilizar o make.

### 4. Linting e formatação de codigo

Buscamos um codigo legível e longevo.
Queremos que qualquer pessoa do time possa ler diferentes partes do parte e entender rapidamente o que esta acontecendo.

### Para formatar nossos códigos, utilizamos

- `ruff format`

Além de formatar, utilizamos _linters_ para garantir a qualidade e consistência
do código, isso facilita bastante as revisões e previne alguns erros mais
simples de serem introduzidos.

Para executar or formatadores localmente, basta:

```sh
make format
```

### Para _linterizar_ nossos códigos, utilizamos

- `ruff`
- `pylint`

Para executar or linter localmente, basta:

```sh
make lint
```

Sempre ao abrir um PR voce recebera um report quanto aos linters. É importante
que você atue para corrigir os problemas aponteados pelos checks.

### 5. Testes

Atualmente temos alguns testes unitários para cobrir funções utilitárias da nossa lib,
isso previne que erros sejam introduzidos ao alterarmos esses códigos.

Para executar os testes, voce pode rodar:

```bash
make test
```

### 6. Navegue pelo repositório e seja feliz

A estrutura deste repositório eh +/- assim:

```bash
datalake/
    ├── 0_any_2_raw/               - Notebooks de ingestão de dados brutos (raw).
    ├── 1_raw_2_standard/          - Notebooks de transformação raw → standard.
    ├── 2_standard_2_refined/      - Notebooks de enriquecimento standard → refined.
    ├── 3_analytics/               - Notebooks para criação de tabelas pro analytics.
    ├── apuracao/                   - Cálculos financeiros (bônus, prêmios, descontos).
    ├── ativacao/                   - Integrações com plataformas externas (WhatsApp, CustomerX, etc.).
    ├── docker/                    - Imagens Docker customizadas para Databricks.
    ├── docs/                      - Documentação adicional do projeto.
    ├── evals/                     - Notebooks de avaliação de qualidade das recomendações.
    ├── liquibase/                 - Migrações de banco de dados (changelogs).
    ├── maggulake/                 - Biblioteca Python reutilizável do projeto.
    ├── misc/                      - Notebooks esporádicos que não pertencem a um fluxo específico.
    ├── terraform/                 - Infraestrutura como código (Databricks workflows).
    └── tests/                     - Testes unitários e de integração.
```
