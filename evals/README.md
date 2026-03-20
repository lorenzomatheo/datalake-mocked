# Evals

Notebooks de avaliação de qualidade das recomendações de produtos.

## Notebooks

| Notebook                                  | Descrição                                                                                                                                                                                                                                | Workflow            |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- |
| `eval_recomendacoes.py`                   | Avalia recomendações de **complementares** usando cestas convertidas como fonte da verdade. Usa o pareto dos 100 produtos mais vendidos e verifica se os produtos recomendados aparecem nos primeiros 10 resultados do vector search.    | `atualiza_produtos` |
| `eval_puro_alternativos_recomendacoes.py` | Avalia recomendações de **alternativos** (substituição) usando LLM Judge (Gemini). Dois avaliadores separados: medicamentos (intercambiabilidade farmacêutica) e não-medicamentos (equivalência de uso). Métricas: Precision e Hit Rate. | `atualiza_produtos` |
| `valida_recomendacoes.py`                 | Validação de recomendações via LLM (ChatDatabricks) com tracking no LangSmith. Uso ad-hoc, não integrado a nenhum workflow.                                                                                                              | —                   |

## Execução

Os notebooks `eval_recomendacoes` e `eval_puro_alternativos_recomendacoes` rodam automaticamente como parte do workflow `atualiza_produtos_producao` (definido em `terraform/pipelines/atualiza_produtos.tf`), após a etapa de geração de embeddings.
