from enum import Enum


class TipoMensagemReguaAtendente(Enum):
    DESAFIO_COMECOU = "desafio_comecou"
    VIROU_LENDA_SEMANA = "virou_lenda_semana"
    REPORTE_DIARIO = "reporte_diario"
    PROGRESSAO_NIVEL_1 = "progressao_nivel_1"
    PROGRESSAO_NIVEL_2 = "progressao_nivel_2"
    PROGRESSAO_NIVEL_3 = "progressao_nivel_3"
    PROGRESSAO_NIVEL_4 = "progressao_nivel_4"
    PROGRESSAO_NIVEL_5 = "progressao_nivel_5"
    VIROU_LENDA_VIVA_AGORA = "virou_lenda_viva_agora"
    RESUMO_RODADA_COM_GANHO = "resumo_rodada_com_ganho"
    RESUMO_RODADA_SEM_GANHO = "resumo_rodada_sem_ganho"


class AbasPlanilhaReguasAtendente(Enum):
    CONTATOS = "contatos_atendentes"
    TEMPLATES = "templates"
    MENSAGENS_ENVIADAS = "m_enviadas"
    IDS_IMAGENS = "imagens_magguletes"
    IDS_MISSOES = "ids_missoes"


class NomeMissao(Enum):
    MISSAO_INTERNA = "Missao Interna"
    MISSAO_MAGGU = "Missao Maggu"
