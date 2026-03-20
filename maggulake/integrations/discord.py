import os

from omni import OmniClient

ID_CANAL_DISCORD = "1484310403227062343"

TAMANHO_MAXIMO_MENSAGEM = 2000


def _get_env_or_secret(key: str, scope: str = "omni") -> str:
    """Lê de os.environ (CI) ou dbutils.secrets (Databricks)."""
    value = os.environ.get(key)
    if value:
        return value
    try:
        from pyspark.dbutils import (
            DBUtils,  # noqa: PLC0415  # pylint: disable=import-outside-toplevel
        )
        from pyspark.sql import (
            SparkSession,  # noqa: PLC0415  # pylint: disable=import-outside-toplevel
        )

        spark = SparkSession.getActiveSession()
        if spark:
            dbutils = DBUtils(spark)
            return dbutils.secrets.get(scope=scope, key=key)
    except (ImportError, RuntimeError):
        pass
    raise ValueError(
        f"Variável '{key}' não encontrada em os.environ"
        f" nem em dbutils.secrets(scope='{scope}')"
    )


def _get_omni_client() -> tuple[OmniClient, str]:
    """Retorna um OmniClient configurado e o instance_id do Discord."""
    base_url = _get_env_or_secret("OMNI_BASE_URL")
    api_key = _get_env_or_secret("OMNI_API_KEY")
    instance_id = _get_env_or_secret("OMNI_DISCORD_INSTANCE_ID")
    client = OmniClient(base_url=base_url, api_key=api_key)
    return client, instance_id


def enviar_mensagem_discord(id_canal: int | str, mensagem: str) -> None:
    """
    Envia uma mensagem para um canal específico no Discord via Omni.

    :param id_canal: ID do canal do Discord
    :param mensagem: Conteúdo da mensagem a ser enviada
    """
    if not id_canal or not mensagem:
        raise ValueError("Mensagem não enviada, verifique os parametros passados")

    client, instance_id = _get_omni_client()

    partes_mensagem = [
        mensagem[i : i + TAMANHO_MAXIMO_MENSAGEM]
        for i in range(0, len(mensagem), TAMANHO_MAXIMO_MENSAGEM)
    ]

    for parte in partes_mensagem:
        result = client.messages.send(
            instance_id=instance_id,
            to=str(id_canal),
            text=parte,
        )
        print(f"Mensagem enviada para o canal {id_canal}: {result}")
