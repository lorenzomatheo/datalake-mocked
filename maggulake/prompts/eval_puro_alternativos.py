"""
Prompts para avaliacao de recomendacoes de produtos alternativos (substituicao)
usando LLM as a Judge.
Separados por tipo de produto: medicamento (com distincao OTC) e nao medicamento.

Estrutura de cada prompt:
  *_SYSTEM : instrucao de sistema - papel, rigor e regras fixas de conduta
  *_PROMPT : template do turno do usuario - contexto → dados → rubrica → saida → tarefa
"""

from textwrap import dedent

# ---------------------------------------------------------------------------
# MEDICAMENTO
# ---------------------------------------------------------------------------

EVAL_ALTERNATIVOS_SYSTEM = dedent("""\
    Voce e um avaliador especializado em recomendacoes de produtos alternativos
    (substituicao) em farmacias.

    Regras de conduta:
    - Avalie somente com base nos dados e criterios fornecidos; nao invente informacoes.
    - Seja objetivo e conciso; limite o motivo a no maximo duas frases.
    - Nao extrapole alem do que esta descrito nos produtos informados.
    - Responda estritamente no formato estruturado solicitado.
    - Seja consistente: produtos com as mesmas caracteristicas devem receber
      a mesma avaliacao independentemente da ordem em que aparecem.
    - Coerencia logica: antes de responder, verifique se sua avaliacao e coerente
      com o que voce avaliaria para um par identico ou muito semelhante; se a
      mesma logica levaria a conclusao oposta, revise a avaliacao.\
""")

EVAL_ALTERNATIVOS_MEDICAMENTO_PROMPT = dedent("""\
    CONTEXTO DO SISTEMA DE RECOMENDACAO:
    1. O cliente bipa um medicamento na farmacia.
    2. O sistema busca medicamentos que podem substituir o medicamento bipado.
    3. O sistema retorna produtos alternativos ao medicamento bipado.

    ID DA AVALIACAO: {id}

    MEDICAMENTO BIPADO:
    {ref_info}

    MEDICAMENTO RECOMENDADO (ALTERNATIVO):
    {rec_info}

    RUBRICA DE AVALIACAO:

    Medicamentos de prescricao — CORRETA quando:
    - Mesmo principio ativo (ou equivalente terapeutico direto)
    - Mesma concentracao por unidade (quantidade na embalagem nao importa)
      EXCECAO DE DOSAGEM: concentracao diferente e aceitavel quando for fator simples
      da dose bipada (ex: ref 500mg → rec 250mg [fator 0.5x] ou rec 1000mg [fator 2x]),
      pois o ajuste pode ser feito na quantidade de unidades administradas.
      Nao se aplica a fatores grandes (ex: 500mg → 50mg, fator 10x).
    - Forma farmaceutica identica ou equivalente:
        EQUIVALENTES : comprimido <-> comprimido revestido
        NAO EQUIVALENTES: comprimido <-> capsula, solucao oral <-> suspensao
    - Via de administracao identica
    - Troca permitida pela ANVISA conforme tipo do medicamento:
        CORRETA : generico <-> referencia
        CORRETA : similar intercambivel <-> referencia
        INCORRETA: similar <-> generico
        CORRETA (excecao empirica): similar <-> similar — aceitavel por regra empirica
          adotada; mencionar explicitamente no motivo que esta excecao foi aplicada.

    Medicamentos de prescricao — INCORRETA quando:
    - Principio ativo diferente sem equivalencia terapeutica direta
    - Concentracoes diferentes que nao sejam fator simples da dose bipada
      (ex: 500mg vs 50mg [fator 10x]; 10mg/ml vs 50mg/ml [fator 5x])
    - Formas farmaceuticas incompativeis (ex: comprimido vs capsula)
    - Vias de administracao incompativeis
    - Tipo de troca nao permitido pela ANVISA (ex: similar por generico)

    OTC/MIP (isentos de prescricao) — criterios mais flexiveis:
    - Principios ativos da mesma classe terapeutica sao aceitaveis
      (ex: ibuprofeno e naproxeno sao ambos AINEs)
    - Pequenas variacoes de dosagem sao aceitaveis se a indicacao for a mesma
    - Forma farmaceutica pode variar se a via de administracao for a mesma
      (ex: comprimido e capsula via oral)

    Nota - concentracao vs quantidade de embalagem:
    - Concentracao: principio ativo por unidade (ex: 500mg, 10mg/ml) - afeta a avaliacao
    - Quantidade: unidades na embalagem (ex: caixa com 30 ou 60 comprimidos) - nao afeta

    Exemplos:
    - Ref "Amoxicilina 500mg capsula"  -> Rec "Amoxil 500mg capsula"             [CORRETA]
    - Ref "Losartana 50mg"             -> Rec "Losartana Potassica 50mg generico" [CORRETA]
    - Ref "Amoxicilina 500mg"          -> Rec "Ibuprofeno 400mg"                  [INCORRETA]
    - Ref "Dipirona 500mg comprimido"  -> Rec "Dipirona 50mg/ml gotas"            [INCORRETA]
    - Ref "Buscopan Composto"          -> Rec "Dorflex"                           [CORRETA]
    - Ref "Engov"                      -> Rec "Sonrisal"                          [CORRETA]
    - Ref "Neosaldina"                 -> Rec "Shampoo anticaspa"                 [INCORRETA]

    FORMATO DA RESPOSTA:
    - id        : repetir exatamente o ID da avaliacao recebido ({id})
    - avaliacao : "CORRETA" ou "INCORRETA"
    - motivo    : explicacao tecnica objetiva (principio ativo, concentracao, forma farmaceutica, tipo;
                  se excecao de dosagem ou similar<->similar foi aplicada, mencionar explicitamente)

    Avalie a recomendacao acima e retorne o resultado estruturado.\
""")

# ---------------------------------------------------------------------------
# NAO MEDICAMENTO
# ---------------------------------------------------------------------------

EVAL_ALTERNATIVOS_NAO_MEDICAMENTO_PROMPT = dedent("""\
    CONTEXTO DO SISTEMA DE RECOMENDACAO:
    1. O cliente bipa um produto na farmacia (nao medicamento).
    2. O sistema busca produtos que podem substituir o produto bipado.
    3. O sistema retorna produtos alternativos ao produto bipado.

    ID DA AVALIACAO: {id}

    PRODUTO BIPADO:
    {ref_info}

    PRODUTO RECOMENDADO (ALTERNATIVO):
    {rec_info}

    RUBRICA DE AVALIACAO:

    CORRETA quando:
    - O produto recomendado atende a mesma necessidade/funcao do produto bipado
    - Sao da mesma categoria ou subcategoria de produto
    - O consumidor consideraria trocar um pelo outro

    INCORRETA quando:
    - Os produtos atendem necessidades diferentes
    - Sao de categorias diferentes sem relacao de substituicao
    - O consumidor nao consideraria trocar um pelo outro

    Exemplos:
    - Ref "Shampoo Dove 400ml"         -> Rec "Shampoo Pantene 400ml"          [CORRETA]
    - Ref "Protetor Solar Nivea FPS50" -> Rec "Protetor Solar La Roche FPS50"  [CORRETA]
    - Ref "Fralda Pampers M"           -> Rec "Fralda Huggies M"               [CORRETA]
    - Ref "Vitamina C 1000mg"          -> Rec "Vitamina C 500mg efervescente"  [INCORRETA]
    - Ref "Shampoo Dove"               -> Rec "Fralda Pampers"                 [INCORRETA]
    - Ref "Protetor Solar"             -> Rec "Esmalte de unha"                [INCORRETA]
    - Ref "Vitamina C 1000mg"          -> Rec "Proteina whey"                  [INCORRETA]

    FORMATO DA RESPOSTA:
    - id        : repetir exatamente o ID da avaliacao recebido ({id})
    - avaliacao : "CORRETA" ou "INCORRETA"
    - motivo    : explicacao objetiva (categoria, funcao e necessidade atendida)

    Avalie a recomendacao acima e retorne o resultado estruturado.\
""")
