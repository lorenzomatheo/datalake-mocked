from agno.agent import Agent
from agno.run.agent import RunOutput
from langsmith import get_current_run_tree, traceable


class TracedAgent:
    """Wrapper em torno do agno.agent.Agent que instrumenta automaticamente
    as chamadas run() e arun() com tracing via LangSmith.

    Substitui o uso direto de Agent + @traceable manual nos notebooks.
    O tracing só ocorre quando LANGSMITH_TRACING=true estiver configurado
    (via setup_langsmith); caso contrário, se comporta como Agent normal.

    Token counts (input/output/total) são extraídos de result.metrics e
    anexados ao span do LangSmith via usage_metadata, no mesmo formato
    usado pelo MedGemma.

    Uso:
        agent = TracedAgent(
            Agent(model=Gemini(...), ...),
            name="meu-agente",
        )
        result = agent.run(prompt)
        result = await agent.arun(prompt)
    """

    def __init__(self, agent: Agent, name: str | None = None) -> None:
        if name is None:
            try:
                from pyspark.sql import (  # pylint: disable=import-outside-toplevel
                    SparkSession,
                )

                active = SparkSession.getActiveSession()
                if active:
                    name = active.conf.get(
                        "maggu.app.name", active.sparkContext.appName
                    )
            except (ImportError, AttributeError, RuntimeError):
                pass
        name = name or "agno-agent"
        self._agent = agent
        self.run = traceable(run_type="llm", name=name)(self._invoke)
        self.arun = traceable(run_type="llm", name=name)(self._ainvoke)

    def _attach_tokens(self, result: RunOutput) -> None:
        metrics = getattr(result, "metrics", None)
        if metrics is None:
            return
        run = get_current_run_tree()
        if run is None:
            return
        run.metadata.update(
            {
                "usage_metadata": {
                    "input_tokens": metrics.input_tokens,
                    "output_tokens": metrics.output_tokens,
                    "total_tokens": metrics.total_tokens,
                }
            }
        )
        run.patch()

    def _invoke(self, prompt: str, **kwargs) -> RunOutput:
        result = self._agent.run(prompt, **kwargs)
        self._attach_tokens(result)
        return result

    async def _ainvoke(self, prompt: str, **kwargs) -> RunOutput:
        result = await self._agent.arun(prompt, **kwargs)
        self._attach_tokens(result)
        return result

    def print_response(self, prompt: str, **kwargs):
        """Passthrough para Agent.print_response (uso em testes/debug)."""
        return self._agent.print_response(prompt, **kwargs)
