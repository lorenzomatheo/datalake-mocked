from typing import Literal

import pandas as pd
import pytz


def gerar_saudacao_com_base_na_hora(
    tz: pytz.timezone = pytz.timezone("America/Sao_Paulo"),
) -> Literal['Bom dia', 'Boa tarde', 'Boa noite']:
    agora = pd.Timestamp.now(tz=tz)
    hora_atual = agora.hour

    if hora_atual < 12:
        return "Bom dia"
    elif hora_atual < 18:
        return "Boa tarde"

    return "Boa noite"


def extrair_vocativo(nome: str) -> str | None:
    # TODO: isso aqui poderia ser melhorado com LLM... Tem gente com nome "Dra. Fulana" no banco
    if pd.isnull(nome) or not isinstance(nome, str) or nome.strip() == "":
        return None
    return nome.strip().split(" ")[0].title()
