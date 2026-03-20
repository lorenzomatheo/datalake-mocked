"""
Prompts para o notebook enriquece_sintomas_e_categorias_complementares.

Fase 1 — PROMPT_SINTOMAS_PRODUTO:
    Dado um produto medicamento (nome + descrição), extrai os sintomas
    ou indicações que o produto trata.

Fase 2 — PROMPT_CATEGORIAS_POR_SINTOMA:
    Dado um sintoma, retorna categorias de produtos farmacêuticos
    complementares (não medicamentos) que o cliente pode comprar.
"""

PROMPT_SINTOMAS_SYSTEM_INSTRUCTION = """\
Para produtos farmacêuticos, identifique os sintomas ou problemas de saúde que cada produto é projetado para tratar ou aliviar.

Use as informações fornecidas (EAN, NOME e DESCRIÇÃO) para extrair os termos relevantes.

Regras para a resposta:
- Liste apenas os termos no singular.
- Cada termo deve ser um sintoma ou problema de saúde específico.
- Formate a resposta como uma lista de termos separados por '|'. Não inclua descrições adicionais.
- Limite sua resposta a 10 termos.
- Se o produto não tem função médica, responda "N/A".
- Retorne o EAN exatamente como fornecido, sem modificação.

Exemplo de entrada:
EAN: 7891058011022
NOME: Tylenol Paracetamol 750mg
DESCRIÇÃO: Analgésico e antitérmico indicado para redução de febre e alívio temporário da dor.

Exemplo de resposta esperada:
ean: 7891058011022
resposta: dor de cabeça|dor muscular|dor nas costas|febre|inflamação\
"""

PROMPT_SINTOMAS_USER_TEMPLATE = """\
Produto a analisar:
{input}\
"""

PROMPT_CATEGORIAS_SYSTEM_INSTRUCTION = """\
Dado um problema de saúde, identifique TIPOS DE PRODUTOS FARMACÊUTICOS, \
excluindo remédios, que o cliente pode considerar comprar em uma farmácia para auxiliar \
no alívio ou tratamento desse problema específico.

Critérios para a resposta:
- Concentre-se em TIPOS DE PRODUTOS FARMACÊUTICOS relevantes para o PROBLEMA informado.
- Exclua quaisquer medicamentos da sua lista, focando em alternativas não medicamentosas.
- Apresente os TIPOS DE PRODUTOS no singular.
- Limite sua resposta a 5 termos.
- Estruture a resposta como uma lista de termos separados por "|". Não inclua explicações.
- Se não houver produtos complementares aplicáveis, responda "N/A".

Exemplo de entrada: queda de cabelo
Exemplo de resposta esperada: shampoo|tônico capilar|vitamina\
"""

PROMPT_CATEGORIAS_USER_TEMPLATE = """\
O problema a analisar é: {input}\
"""

PROMPT_SINTOMAS_PRODUTO = """\
Para o produto farmacêutico descrito abaixo, identifique os sintomas ou problemas de saúde \
que ele é projetado para tratar ou aliviar.

Use as informações fornecidas (EAN, NOME e DESCRIÇÃO) para extrair os termos relevantes.

Regras para a resposta:
- Liste apenas os termos no singular.
- Cada termo deve ser um sintoma ou problema de saúde específico.
- Formate a resposta como uma lista de termos separados por '|'. Não inclua descrições adicionais.
- Limite sua resposta a 10 termos.
- Se o produto não tem função médica, responda "N/A".
- Retorne o EAN exatamente como fornecido, sem modificação.

Exemplo de entrada:
EAN: 7891058011022
NOME: Tylenol Paracetamol 750mg
DESCRIÇÃO: Analgésico e antitérmico indicado para redução de febre e alívio temporário da dor.

Exemplo de resposta esperada:
ean: 7891058011022
resposta: dor de cabeça|dor muscular|dor nas costas|febre|inflamação

Produto a analisar:
{input}
"""

PROMPT_CATEGORIAS_POR_SINTOMA = """\
Dado o PROBLEMA "{input}" mencionado, identifique TIPOS DE PRODUTOS FARMACÊUTICOS, \
excluindo remédios, que o cliente pode considerar comprar em uma farmácia para auxiliar \
no alívio ou tratamento desse problema específico.

Critérios para a resposta:
- Concentre-se em TIPOS DE PRODUTOS FARMACÊUTICOS relevantes para o PROBLEMA informado.
- Exclua quaisquer medicamentos da sua lista, focando em alternativas não medicamentosas.
- Apresente os TIPOS DE PRODUTOS no singular.
- Limite sua resposta a 5 termos.
- Estruture a resposta como uma lista de termos separados por "|". Não inclua explicações.
- Se não houver produtos complementares aplicáveis, responda "N/A".

Exemplo de entrada: queda de cabelo
Exemplo de resposta esperada: shampoo|tônico capilar|vitamina
"""
