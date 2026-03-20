import random
import time
import warnings
from datetime import datetime
from typing import Literal

import gspread
import pytz
import requests

from maggulake.ativacao.whatsapp.enums import (
    AbasPlanilhaReguasAtendente,
    NomeMissao,
    TipoMensagemReguaAtendente,
)
from maggulake.ativacao.whatsapp.hyperflow.config import CLIENT_ID_HYPERFLOW_PROD
from maggulake.ativacao.whatsapp.hyperflow.enviar_mensagem_via_hyperflow import (
    enviar_mensagem_whatsapp_via_hyperflow_regua,
)
from maggulake.ativacao.whatsapp.hyperflow.utils import padroniza_numero_whatsapp
from maggulake.ativacao.whatsapp.regua_atendente.models import MensagemEnviadaRow

# TODO: validar se posso usar o mesmo valor pra INTERNA e MAGGU
PREMIO_POTENCIAL = "100"


class GerenciadorResumos:
    def __init__(
        self,
        spreadsheet: gspread.Spreadsheet,
        tipo_resumo: Literal["diario", "semana"],
        nome_missao: NomeMissao,
        min_delay_seconds: float = 0,
        max_delay_seconds: float = 3,
    ):
        self.sheet = spreadsheet.worksheet(
            AbasPlanilhaReguasAtendente.MENSAGENS_ENVIADAS.value
        )
        self.tipo_resumo = tipo_resumo
        self.nome_missao = nome_missao
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds

        self._hyperflow_client_id = CLIENT_ID_HYPERFLOW_PROD

    def enviar_mensagem_e_salvar_na_planilha(
        self, df
    ) -> tuple[list[MensagemEnviadaRow], list[dict]]:
        rows = df.select(
            "numero_whatsapp",
            "username",
            "vocativo",
            "eh_usuario_somente_teste",
            "tipo_template",
            "status_label",
        ).collect()

        # Inicializar variável para garantir que exista no bloco finally
        msgs_enviadas: list[MensagemEnviadaRow] = []
        errors: list[dict] = []

        try:
            msgs_enviadas, errors = self.__enviar_mensagens(rows)
        finally:
            self.__salvar_mensagens_na_planilha(msgs_enviadas)
            print("Finalizando o envio...")
            print(f"Quantidade de mensagens processadas: {len(msgs_enviadas)}")

        return msgs_enviadas, errors

    def __enviar_mensagens(self, rows) -> list[MensagemEnviadaRow]:
        timestamp = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        new_rows: list[MensagemEnviadaRow] = []
        errors: list[dict] = []  # TODO: no futuro criar model para Errors

        for row in rows:
            numero_destinatario = row["numero_whatsapp"]
            username = row["username"]

            if not numero_destinatario:
                warnings.warn(
                    f"'Dados incompletos para usuario '{username}', pulando. "
                    f"Tipo resumo: {self.tipo_resumo}"
                )
                continue

            tipo_mensagem = self.__decidir_tipo_mensagem(row)
            payload = self.__gerar_payload_hyperflow(tipo_mensagem, row)

            try:
                self.enviar_mensagem_via_hyperflow(payload)
            except requests.exceptions.RequestException as e:
                warnings.warn(
                    f"Falha ao enviar mensagem para o numero '{numero_destinatario}' no "
                    f"tipo resumo '{self.tipo_resumo}': {e}\n"
                )
                errors.append(
                    {
                        "payload": payload,
                        "message": e,
                    }
                )
                continue

            new_rows.append(
                MensagemEnviadaRow(
                    id_remetente="hyperflow",
                    numero_destinatario=numero_destinatario,
                    username=username,
                    mensagem="<GerenciadorResumoBase>",
                    data_hora_envio=timestamp,
                    testes=str(row["eh_usuario_somente_teste"]).upper(),
                    tipo_mensagem=tipo_mensagem.value,
                    status_anterior="",
                    status_enviado="",
                    id_missao="",
                    nome_missao=self.nome_missao.value,
                )
            )

            # Random delay to avoid hitting meta's radar
            time.sleep(random.uniform(self.min_delay_seconds, self.max_delay_seconds))

        return new_rows, errors

    def __salvar_mensagens_na_planilha(
        self, new_rows: list[MensagemEnviadaRow]
    ) -> None:
        if not new_rows:
            print(f"Nenhuma mensagem para registrar. Tipo resumo: {self.tipo_resumo}")

        new_rows_formatted = [r.to_sheet_row() for r in new_rows]

        self.sheet.append_rows(new_rows_formatted)
        print(
            f"Gravadas {len(new_rows_formatted)} mensagens. Tipo resumo: {self.tipo_resumo}"
        )

    def __decidir_tipo_mensagem(self, row) -> TipoMensagemReguaAtendente:
        tipo = row["tipo_template"]
        return TipoMensagemReguaAtendente(tipo)

    def __gerar_payload_hyperflow(
        self, tipo_mensagem: TipoMensagemReguaAtendente, row
    ) -> dict:
        telefone = padroniza_numero_whatsapp(str(row["numero_whatsapp"]))
        payload = {
            "telefone_destinatario": telefone,
            "tipo_regua": "atendente",
        }

        match tipo_mensagem:
            # Resumo Diario (ontem)
            case TipoMensagemReguaAtendente.SUBIU_ONTEM:
                payload.update(
                    {
                        "tipo_mensagem": TipoMensagemReguaAtendente.SUBIU_ONTEM.value,
                        "nome_atendente": row["vocativo"],
                        "nome_missao": self.nome_missao.value,
                        "nivel_atingido": row["status_label"],
                    }
                )
            case TipoMensagemReguaAtendente.NAO_SUBIU_ONTEM:
                payload.update(
                    {
                        "tipo_mensagem": TipoMensagemReguaAtendente.NAO_SUBIU_ONTEM.value,
                        "nome_atendente": row["vocativo"],
                        "premio_potencial": PREMIO_POTENCIAL,
                    }
                )
            case TipoMensagemReguaAtendente.NAO_SUBIU_ONTEM_ALT:
                payload.update(
                    {
                        "tipo_mensagem": TipoMensagemReguaAtendente.NAO_SUBIU_ONTEM_ALT.value,
                        "nome_atendente": row["vocativo"],
                        "premio_potencial": PREMIO_POTENCIAL,
                        "nome_missao": self.nome_missao.value,
                    }
                )
            case TipoMensagemReguaAtendente.EH_LENDA_VIVA:
                payload.update(
                    {
                        "tipo_mensagem": TipoMensagemReguaAtendente.EH_LENDA_VIVA.value,
                        "nome_atendente": row["vocativo"],
                        "nome_missao": self.nome_missao.value,
                    }
                )
            case TipoMensagemReguaAtendente.NENHUM_PONTO:
                payload.update(
                    {
                        "tipo_mensagem": TipoMensagemReguaAtendente.NENHUM_PONTO.value,
                        "nome_atendente": row["vocativo"],
                        "nome_missao": self.nome_missao.value,
                    }
                )
            # Resumo Semanal
            case TipoMensagemReguaAtendente.NAO_SUBIU_SEMANA:
                payload.update(
                    {
                        "tipo_mensagem": TipoMensagemReguaAtendente.NAO_SUBIU_SEMANA.value,
                        "nome_atendente": row["vocativo"],
                    }
                )
            case TipoMensagemReguaAtendente.SUBIU_SEMANA:
                payload.update(
                    {
                        "tipo_mensagem": TipoMensagemReguaAtendente.SUBIU_SEMANA.value,
                        "nome_atendente": row["vocativo"],
                        "nome_missao": self.nome_missao.value,
                        "nivel_atingido": row["status_label"],
                    }
                )
            case TipoMensagemReguaAtendente.VIROU_LENDA_SEMANA:
                payload.update(
                    {
                        "tipo_mensagem": TipoMensagemReguaAtendente.VIROU_LENDA_SEMANA.value,
                        "nome_atendente": row["vocativo"],
                        "nome_missao": self.nome_missao.value,
                    }
                )
            case _:
                raise ValueError(f"Tipo de mensagem desconhecido: {str(tipo_mensagem)}")

        return payload

    def enviar_mensagem_via_hyperflow(self, payload: dict) -> None:
        enviar_mensagem_whatsapp_via_hyperflow_regua(self._hyperflow_client_id, payload)
