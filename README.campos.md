# Descrição das colunas do datalake

Última atualização: 17/sept/2024.

A lista abaixo descreve todos os nomes de campos (colunas) utilizados no nosso datalake.

Obs.: Essa tabela pode desatualizar bem facilmente, mas ainda assim pode ajudar quem estiver chegando no projeto.

Obs.: Para uma descrição mais alto nível, ler o canal "Tech" no [notion](https://www.notion.so/Tech-2679655f6297438994c9c8e070ea8459?pvs=4).

```SQL

    -- Identificação
    id STRING NOT NULL,
    id_produto STRING NOT NULL,

    -- EANs
    ean STRING NOT NULL, -- Código de barras do produto. Em geral, seguem o padrão EAN13, porém existem diversas exceções.
    eans_alternativos ARRAY<STRING>, -- EANs equivalentes ao um determinado EAN principal. Ex.: ["0123456", "00123456"]

    -- Identificação de lojas
    conta STRING NOT NULL, -- Algo semelhante ao termo "rede de farmácias". Exemplo: xisfarmacia, farmagui, etc.
    tenant STRING NOT NULL, -- O mesmo que "conta". TODO: tenant -> conta. Deletar tenant. Mantido por retrocompatibilidade por enquanto.
    loja STRING NOT NULL, -- Cada conta possui diferentes lojas. Exemplos: farmagui loja 1, farmagui loja 2, etc.
    codigo_loja STRING NOT NULL, -- O mesmo que loja. TODO: remover coluna duplicada.

    -- Timestamps
    gerado_em TIMESTAMP NOT NULL,
    atualizado_em TIMESTAMP NOT NULL,
    apagado_em TIMESTAMP,

    -- Booleans
    eh_produto_foco BOOLEAN, -- Se o produto é um produto marcado como "foco" (seja pela maggu, industria ou ) naquela loja.
    eh_medicamento BOOLEAN, -- Se o produto é um medicamento ou não.
    eh_tarjado BOOLEAN, -- Se o medicamento possui tarja Preta ou Vermelha.
    eh_controlado BOOLEAN, -- Se o medicamento eh de venda controlada, ou seja, se a venda somente acontece sob prescricao medica.
    eh_otc BOOLEAN, -- Se o medicamento é isento de prescrição médica ("Over the counter", em inglês)
    eh_generico, -- Se o medicamento é um medicamento genérico
    permite_desconto BOOLEAN, -- Se aquele produto pode ser vendido com desconto naquela loja ou não.
    pode_partir BOOLEAN, -- Se o medicamento pode ser dividido.
    uso_continuo BOOLEAN, -- Se o medicamento é de uso contínuo
    descontinuado BOOLEAN, -- Se o medicamento não está mais em atividade

    -- Preços e descontos
    preco_venda DOUBLE, -- Preço em que cada unidade é vendida naquela loja
    preco_venda_desconto DOUBLE, -- Preço do produto quando vendido com desconto (pode ser diferente )
    custo_compra DOUBLE, -- Custo que a loja tem por comprar o produto
    estoque_unid INTEGER, -- Quantidade de unidades do produto em estoque naquela loja.
    desconto_percentual_maximo DOUBLE, -- Desconto máximo (em %) que pode ser aplicado ao vender este produto. TODO: renomear para desconto_maximo_percentual
    preco_pmc DOUBLE,  -- Preço máximo ao consumidor. Corresponde ao preço teto praticado pelo comércio varejista.

    -- Grupos
    grupo STRING, -- Determinado pela loja. Exemplo: Cosméticos, Perfumes, Esmaltes, etc.
    subgrupo STRING, -- Determinado pela loja. Exemplo: Esmaltes coloridos, Esmaltes sem cor, etc.
    categoria STRING, -- Categoria das campanhas da industrias (TODO: nome ambíguo)

    -- Maggu coins
    maggu_coins_venda_organica INTEGER, -- Quantidade de maggu coins paga por cada venda ORGÂNICA deste produto
    maggu_coins_venda_agregada INTEGER, -- Quantidade de maggu coins paga por cada venda AGREGADA deste produto
    maggu_coins_ads_substituicao INTEGER, -- Quantidade de maggu coins paga por cada venda SUBSTITUIÇÃO POR ADS deste produto
    maggu_coins_ads_agregada INTEGER, -- Quantidade de maggu coins paga por cada venda AGREGADA deste produto se ele for produto ADS.

    -- Nomes de produtos
    nome STRING NOT NULL, -- Nome do produto
    marca STRING, -- Nome da linha de produtos/marca (Dove, Listerine, Valda)
    nome_comercial STRING, -- Nome otimizado para busca e apresentação no copilot (é utilizado como primeira opção, caso esteja nulo o fallback vai para o campo nome)
    nome_complemento STRING, -- Nome sem a parte do "nome_simples", removido da camada refined por nao ser utilizado.
    nomes_alternativos ARRAY<STRING>, -- Nomes que o mesmo produto pode receber. Em geral, é a concatenação dos nomes que cada loja diferente pode ter dado para o mesmo produto.

    -- Descrição de produtos
    fabricante STRING, -- Fabricante do produto. Exemplo: Bayer, Euro Farma, Colgate, Listerine, Dove, etc...
    descricao STRING, -- Descrição do  produto
    status STRING, -- Se um determinado produto ainda está sendo utilizado em nossas bases. Pode ser "ativo" ou "inativo".

    -- Fonte e arquivos
    imagem_url STRING, -- URL da foto do produto.
    json_data STRING, -- Dados do produto em versão json, em geral salvam o payload recebido pela ingestão de dados de clientes.
    arquivo_s3 STRING, -- Local onde o arquivo .json do produto está salvo no s3
    fonte STRING, -- Marca a origem do produto, exemplos: RD, Consulta Remédios, Bluesoft, Toolspharma, etc.

    -- Categorias e indicações
    indicacao STRING, -- Texto dizendo para qual "quadro" esse produto é indicado: "O produto ... é indicado para ..."
    categorias ARRAY<STRING>,  -- Categorias em que o produto se encaixa. Exemplo: Analgésicos, Descongestionante Nasal, etc.
    categorias_de_complementares_por_indicacao ARRAY<STRUCT< -- Funciona como um "de x para" entre indicações e categorias do produto
        indicacao STRING,
        categorias ARRAY<STRING>
    >>,
    informacoes_para_embeddings STRING, -- Prompt utilizado para enriquecimento via LLM. Essa coluna eh importante para a criação do vector search index.
    idade_recomendada STRING, -- O produto é geralmente para Criança, Adulto, Idoso?
    sexo_recomendado STRING, -- O produto geralmente é para Homem ou mulher?

    -- Informações relevantes para medicamentos
    numero_registro STRING, -- O registro do medicamento na Anvisa
    tipo_medicamento STRING, -- Tipo de medicamento: Medicamento Referência, Medicamento Similar Intercambiável, etc.
    dosagem STRING, -- Dosagem em que o medicamento é apresentado.
                    -- Para alguns casos pode ser entendido como a menor divisão
                    -- do medicamento (1 comprimido). Exemplo: 10mg + 40mg, 25mg/mL
    volume_quantidade STRING, -- Volume ou quantidade do produto: 35g, 1L, 50ml
    variantes ARRAY<STRING>, -- Variantes disponíveis do mesmo produtos
                             -- (cada um deve corresponder a outro EAN):
                             -- 0: "100mg, caixa com 30 comprimidos revestidos"
    principio_ativo STRING, -- Princípio ativo do medicamento. Não se aplica para não medicamentos.
    plataforma_pbm STRING, -- Esse campo nao e utilizado em momento algum
                           -- (por enquanto). Plataforma PBM refere-se a um
                           -- sistema que gerencia Programas de Benefício em
                           -- Medicamentos. Exemplo: e-pharma.
    contraindicacoes STRING,
    efeitos_colaterais STRING,
    interacoes_medicamentosas STRING,
    instrucoes_de_uso STRING,
    advertencias_e_precaucoes STRING,
    condicoes_de_armazenamento STRING,
    validade_apos_abertura STRING, -- Validade do produto apos abertura da embalagem
    tipo_receita STRING, -- confira o enum na pasta utils
    tipo_de_receita_completo STRING,
    informacoes_nutricionais STRING,
    forma_farmaceutica STRING, -- confira o enum na pasta utils
    via_administracao STRING, -- Deve ser consumido por via oral, intra-venal,
                              -- etc.? Verifique o enum na pasta utils.
    classes_terapeuticas ARRAY<STRING>, -- conjuntos de classes para cada
                                        -- princípio ativo de um medicamento,
                                        -- define se é de controle especial ou
                                        -- antimicrobiano. Ex: Estomatológicos
    especialidades ARRAY<STRING>, -- Exemplo: "Psiquiatria", "Neurologia", "Odontologia"
    doencas_relacionadas STRING, -- Quais doenças o medicamento trata
    temperatura_armazenamento STRING, -- Se deve ser armazenado em temperatura ambiente ou refrigerado
    bula ARRAY<STRUCT<
        id STRING,
        title STRING,
        content STRING
    >>,
```
