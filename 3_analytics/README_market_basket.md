# Market Basket Analysis - Análise de Cesta de Mercado

Esta documentação explica como usar a implementação de análise de cesta de mercado para descobrir produtos frequentemente comprados juntos e gerar regras de associação de produtos.

## Visão Geral

A análise de cesta de mercado usa o algoritmo **FP-Growth** para identificar padrões de compra e criar regras do tipo "se produto A está na cesta, então produto B provavelmente também estará".

## Como Usar

### 1. Executar o Notebook

Execute o notebook `3_analytics/market_basket_analysis.py` no Databricks com os seguintes parâmetros:

#### Parâmetros Disponíveis:

- **debug**: `ligado/desligado` - Ativa modo debug com visualizações detalhadas
- **min_support**: `0.01` - Suporte mínimo (ex: 0.01 = 1% das transações)
- **min_confidence**: `0.05` - Confiança mínima (ex: 0.05 = 5%)
- **dias_analise**: `90` - Período de análise em dias
- **auto_ajuste**: `ligado/desligado` - Ativa ajuste automático para dados esparsos
- **modo_sparse**: `conservador/moderado/agressivo` - Nível de agressividade do ajuste

### 2. Estrutura dos Dados de Entrada

O notebook utiliza a tabela `view_vendas` que contém:

- `venda_id`: ID único da transação
- `produto_id`: ID único do produto
- `produto_ean`: Código EAN do produto
- `nome_produto`: Nome do produto
- `quantidade`: Quantidade vendida
- `realizada_em`: Data/hora da venda

### 3. Dados de Saída

#### Tabela de Regras de Associação: `analytics.product_association_rules`

| Campo                       | Tipo      | Descrição                       |
| --------------------------- | --------- | ------------------------------- |
| `produto_a_id`              | String    | ID do produto A (antecedente)   |
| `produto_b_id`              | String    | ID do produto B (consequente)   |
| `produto_a_ean`             | String    | EAN do produto A                |
| `produto_b_ean`             | String    | EAN do produto B                |
| `produto_a_nome`            | String    | Nome do produto A               |
| `produto_b_nome`            | String    | Nome do produto B               |
| `support`                   | Double    | Suporte da regra A → B          |
| `confidence`                | Double    | Confiança da regra A → B        |
| `lift`                      | Double    | Lift da regra A → B             |
| `conviction`                | Double    | Conviction da regra             |
| `total_transacoes`          | Long      | Total de transações analisadas  |
| `transacoes_produto_a`      | Long      | Transações contendo produto A   |
| `transacoes_produto_b`      | Long      | Transações contendo produto B   |
| `transacoes_ambos_produtos` | Long      | Transações contendo A e B       |
| `algoritmo_usado`           | String    | Algoritmo utilizado (fp_growth) |
| `periodo_analise_dias`      | Integer   | Período de análise              |
| `data_calculo`              | Timestamp | Data do cálculo                 |

## Métricas Explicadas

### Support (Suporte)

Percentual de transações que contêm ambos os produtos A e B.

```
Support(A → B) = Transações(A ∩ B) / Total_Transações
```

### Confidence (Confiança)

Probabilidade de encontrar B dado que A está presente.

```
Confidence(A → B) = Transações(A ∩ B) / Transações(A)
```

### Lift

Indica o quanto a presença de A aumenta a probabilidade de B estar presente.

```
Lift(A → B) = Confidence(A → B) / Support(B)
```

- Lift > 1: Associação positiva
- Lift = 1: Independência
- Lift < 1: Associação negativa

### Conviction

Mede o grau de implicação de uma regra.

```
Conviction(A → B) = (1 - Support(B)) / (1 - Confidence(A → B))
```

## Algoritmo FP-Growth

O FP-Growth (Frequent Pattern Growth) é um algoritmo eficiente para mineração de padrões frequentes que:

- **Vantagens**: Altamente eficiente para grandes datasets, especialmente com suporte baixo
- **Performance**: Usa estrutura de dados compacta (FP-tree) que reduz significativamente o tempo de processamento
- **Escalabilidade**: Funciona bem tanto para datasets pequenos quanto grandes
- **Implementação**: Usa a implementação otimizada do PySpark MLlib

## Recursos para Dados Esparsos

### Auto-Ajuste de Parâmetros

O notebook inclui funcionalidade de auto-ajuste que:

- Analisa automaticamente a densidade dos dados
- Classifica o nível de sparsity (Baixa/Moderada/Alta/Extrema)
- Ajusta parâmetros baseado no modo escolhido
- Testa progressivamente parâmetros mais baixos se nenhuma regra for encontrada

### Modos de Ajuste:

- **Conservador**: Ajustes mais cautelosos, mantendo qualidade alta
- **Moderado**: Balanceia entre qualidade e quantidade de regras
- **Agressivo**: Parâmetros mais baixos para capturar padrões raros

## Exemplos de Uso

### Consulta Básica - Top Regras de Associação

```sql
SELECT 
    produto_a_nome,
    produto_b_nome,
    confidence,
    lift,
    support
FROM analytics.product_association_rules
WHERE periodo_analise_dias = 90
ORDER BY confidence DESC
LIMIT 20;
```

### Encontrar Produtos Complementares

```sql
SELECT 
    produto_a_nome AS produto_principal,
    produto_b_nome AS produto_complementar,
    confidence AS probabilidade,
    lift,
    transacoes_ambos_produtos AS ocorrencias
FROM analytics.product_association_rules  
WHERE produto_a_ean = '7891234567890'  -- EAN do produto de interesse
    AND confidence >= 0.1
    AND lift > 1.5
ORDER BY confidence DESC;
```

### Análise por Categoria

```sql
-- Requer join com tabela de produtos para categorias
SELECT 
    p1.categorias[0] AS categoria_a,
    p2.categorias[0] AS categoria_b,
    AVG(pc.confidence) AS confidence_media,
    COUNT(*) AS num_regras
FROM analytics.product_association_rules pc
JOIN refined.produtos_refined p1 ON pc.produto_a_id = p1.id  
JOIN refined.produtos_refined p2 ON pc.produto_b_id = p2.id
GROUP BY p1.categorias[0], p2.categorias[0]
HAVING num_regras >= 5
ORDER BY confidence_media DESC;
```

## Configurações Recomendadas

### Para Análise Exploratória

```
min_support = 0.005   # 0.5%
min_confidence = 0.03 # 3%
dias_analise = 180    # 6 meses
auto_ajuste = "ligado"
modo_sparse = "moderado"
```

### Para Regras de Negócio

```
min_support = 0.01    # 1%
min_confidence = 0.1  # 10%
dias_analise = 90     # 3 meses  
auto_ajuste = "ligado"
modo_sparse = "conservador"
```

### Para Descoberta de Padrões Raros

```
min_support = 0.001   # 0.1%
min_confidence = 0.05 # 5%
dias_analise = 365    # 1 ano
auto_ajuste = "ligado"
modo_sparse = "agressivo"
```

## Interpretação dos Resultados

### Exemplo de Regra Forte

```
Produto A: Shampoo Pantene
Produto B: Condicionador Pantene  
Support: 0.02 (2% das vendas)
Confidence: 0.65 (65% dos que compram shampoo também compram condicionador)
Lift: 4.2 (4.2x mais provável comprar condicionador se comprou shampoo)
```

**Interpretação**: Esta é uma regra forte - produtos complementares da mesma marca.

### Filtros de Qualidade Aplicados

O notebook automaticamente aplica os seguintes filtros:

- `confidence >= min_confidence`
- `support >= min_support`
- `lift > 1.0` (apenas associações positivas)
- `transacoes_ambos_produtos >= 2` (mínimo 2 co-ocorrências)
- `produto_a_id != produto_b_id` (produtos diferentes)

## Monitoramento e Manutenção

### Frequência Recomendada

- **Análises exploratórias**: Semanal
- **Atualização de regras de negócio**: Mensal
- **Análises sazonais**: Conforme eventos/sazonalidade

## Solução de Problemas

### Poucas Regras Geradas

- Ative o auto_ajuste com modo "agressivo"
- Aumente `dias_analise` para incluir mais dados
- Verifique se há transações suficientes com múltiplos produtos

### Muitas Regras de Baixa Qualidade

- Use auto_ajuste no modo "conservador"
- Aumente `min_confidence`
- Filtre por categorias específicas

### Performance Lenta

- Reduza `dias_analise` se necessário
- Aumente `min_support` para reduzir o número de itemsets
- Execute em cluster com mais recursos
- Use cache nas consultas intermediárias

### Dados Muito Esparsos

O notebook tem diagnósticos automáticos que sugerem:

- Agrupamento de produtos similares
- Análise por categoria
- Foco em clientes frequentes
- Técnicas alternativas de recomendação
