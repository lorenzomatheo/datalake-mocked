# Observabilidade LLM — Guia de Referência

Tracing de todas as chamadas LLM do datalake via **LangSmith**.

## Setup em notebooks

Todo notebook que faz chamada LLM deve configurar o tracing logo após o build do ambiente:

```python
from maggulake.utils.model_wrappers import setup_langsmith

env = DatabricksEnvironmentBuilder.build("nome_do_notebook", dbutils, ...)

setup_langsmith(dbutils, env.spark, stage=env.settings.stage.value)
```

O nome do projeto no LangSmith é derivado automaticamente do `appName` da SparkSession
(o primeiro argumento do `DatabricksEnvironmentBuilder.build`). O parâmetro `project`
pode ser passado explicitamente para sobrescrever, mas não é necessário.

O `stage` separa os projetos por ambiente: `nome_do_notebook-staging` vs
`nome_do_notebook-production`.

**Regra:** `langsmith` deve estar **explícito** no `%pip install` de cada notebook —
não confiar na dependência transitiva do langchain-core.

______________________________________________________________________

## Os 3 tipos de agentes e como são instrumentados

### 1. TaggingProcessor (LangChain — output estruturado)

**Quando usar:** pipeline de enriquecimento de produtos com output Pydantic.
Cobre providers: `openai`, `gemini`, `gemini_vertex`, `medgemma`.

**Arquivo:** `maggulake/utils/model_wrappers/tagging_processor.py`

**Tracing:** automático via `@traceable(run_type="chain")` nos métodos da classe.
LangChain auto-captura token counts (input/output/total) via callbacks internos.

**Uso:**

```python
from maggulake.utils.model_wrappers import TaggingProcessor

processor = TaggingProcessor(
    provider="gemini_vertex",
    model=get_model_name(provider="gemini_vertex", size="SMALL"),
    prompt_template=MEU_PROMPT,
    output_schema=MeuSchema,
    project_id=project_id,
    location=location,
)

result = processor.executa_tagging_chain(message)
result = await processor.executa_tagging_chain_async(message)
results = await processor.executa_tagging_chain_async_batch(messages)
```

**Notebooks usando TaggingProcessor:** 14 notebooks em
`2_standard_2_refined/enriquecimento/` e `clientes/`.

______________________________________________________________________

### 2. TracedAgent (Agno — agentes com busca/raciocínio)

**Quando usar:** agentes Agno que precisam de web search, raciocínio multi-step
ou output livre (não estruturado via Pydantic).

**Arquivo:** `maggulake/utils/model_wrappers/agno_tracer.py`

**Tracing:** `TracedAgent` envolve `agno.agent.Agent` com `@traceable(run_type="llm")`.
Token counts (input/output/total) são extraídos de `result.metrics` e anexados ao
span do LangSmith via `usage_metadata`. O nome do span é auto-detectado do `appName`
da SparkSession ativa — não precisa ser passado explicitamente.

**Uso:**

```python
from agno.agent import Agent
from agno.models.google import Gemini
from maggulake.utils.model_wrappers import TracedAgent

agent = TracedAgent(
    Agent(
        model=Gemini(id=GEMINI_MODEL, vertexai=True, project_id=..., location=...),
        description="...",
        output_schema=MeuSchema,
    )
    # name= opcional; auto-detectado do appName se omitido
)

result = agent.run(prompt)
result = await agent.arun(prompt)
```

**Notebooks usando TracedAgent:**

- `enriquecimento/enriquece_marca_fabricante.py`
- `enriquecimento/llm_as_judge.py`
- `avalia_intercambiaveis.py`
- `evals/eval_puro_alternativos_recomendacoes.py`
- `maggulake/utils/web_search.py` (via `BuscaWeb`)

**Nota:** `name=` deve ser passado **explicitamente** em bibliotecas (não notebooks),
pois o `appName` detectado seria o do notebook chamador — o que pode gerar spans com
nomes genéricos. Ex: `web_search.py` mantém `name="agno-web-search"`.

______________________________________________________________________

### 3. MedGemma direto (HTTP raw — endpoint Vertex customizado)

**Quando usar:** endpoint MedGemma em `europe-west4`, chamado via HTTP raw
(sem SDK LangChain ou Agno).

**Arquivo:** `maggulake/utils/model_wrappers/medgemma.py`

**Tracing:** `@traceable(run_type="llm", name="MedGemma")` na função `call_medgemma`.
Token counts extraídos manualmente do response JSON e anexados via
`get_current_run_tree()` + `run.patch()`.

**Uso:** a função `call_medgemma` é chamada internamente pelo `TaggingProcessor`
quando `provider="medgemma"`. Não é necessário chamar diretamente nos notebooks.

______________________________________________________________________

## Coluna de custo no LangSmith

| Provider                     | Tokens                       | Custo                                   |
| ---------------------------- | ---------------------------- | --------------------------------------- |
| LangChain (TaggingProcessor) | Automático                   | Automático (para modelos suportados)    |
| Agno (TracedAgent)           | Extraído de `result.metrics` | Vazio — Gemini Vertex não retorna custo |
| MedGemma HTTP raw            | Extraído manualmente         | Vazio — endpoint custom sem pricing     |

Para modelos não suportados pelo LangSmith (Vertex AI Gemini), o custo absoluto pode
ser calculado externamente com base nos tokens registrados e na tabela de preços do Google.
