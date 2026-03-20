from datetime import datetime

import pytz


def agora_em_sao_paulo() -> datetime:
    return datetime.now(pytz.timezone("America/Sao_Paulo"))


def agora_em_sao_paulo_str() -> str:
    return agora_em_sao_paulo().strftime("%H:%M:%S")
