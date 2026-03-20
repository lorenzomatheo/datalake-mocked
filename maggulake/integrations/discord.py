import time

import requests
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

# TODO: centralizar Discord channel IDs em config/env vars em vez de hardcoded em cada arquivo.
ID_CANAL_DISCORD = "1211857582939963483"


# TODO: type hint de id_canal está como int mas aceita str (ver NOTE na linha 19). Corrigir para int | str.
def enviar_mensagem_discord(spark: SparkSession, id_canal: int, mensagem: str) -> None:
    """
    Envia uma mensagem para um canal específico no Discord.

    :param id_canal: ID do canal do Discord
    :param mensagem: Conteúdo da mensagem a ser enviada
    """

    # NOTE: apesar de o type hinting indicar um inteiro, o id do canal tbm pode ser um string...

    if not id_canal or not mensagem:
        raise ValueError("Mensagem não enviada, verifique os parametros passados")

    try:
        dbutils = DBUtils(spark)
        token = dbutils.secrets.get(scope="discord_bot", key="BOT_TOKEN")
    except Exception as e:  # TODO: trocar para o exception especifico.
        raise ValueError(f"Não foi possível obter o token do bot: {str(e)}")

    url = f"https://discord.com/api/v10/channels/{id_canal}/messages"
    headers = {"Authorization": f"Bot {token}", "Content-Type": "application/json"}

    # Dividir a mensagem em partes se for muito longa
    tamanho_maximo = 2000
    partes_mensagem = [
        mensagem[i : i + tamanho_maximo]
        for i in range(0, len(mensagem), tamanho_maximo)
    ]

    for parte in partes_mensagem:
        payload = {"content": parte}
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            print(f"Mensagem enviada com sucesso para o canal {id_canal}")
        else:
            print(
                f"Falha ao enviar mensagem para o canal {id_canal}. Código de status: {response.status_code}"
            )
            print(f"Resposta: {response.text}")

        # Discord tem rate limit para envios de mensagens em menos de 2 segundos entre si
        time.sleep(2)


# Exemplo de uso

# Para pegar o id, basta clicar com o botão direito no canal do Discord
# e selecionar "copiar link", exemplo: https://discord.com/channels/1069732052078952470/1211857582939963483
# O id será o último número no link

# id_canal = 1211857582939963483  # canal de alertas
# mensagem = "Olá, esta é uma mensagem de teste!"
# enviar_mensagem_discord(id_canal, mensagem)
