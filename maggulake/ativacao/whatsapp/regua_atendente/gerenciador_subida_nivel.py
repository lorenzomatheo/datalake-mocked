import random
import time
import warnings
from datetime import datetime, timezone
from typing import Optional

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
from maggulake.ativacao.whatsapp.regua_atendente.models import MensagemEnviadaRow

# TODO: precisa tomar cuidado com essa premiacao que vai para a regua interna. Double check depois.
PREMIO_POTENCIAL_SEMANA = "100"


class GerenciadorSubidaNivel:
    def __init__(
        self,
        spreadsheet,
        min_delay_seconds: int = 0,
        max_delay_seconds: int = 3,
    ):
        self.sheet = spreadsheet.worksheet(
            AbasPlanilhaReguasAtendente.MENSAGENS_ENVIADAS.value
        )
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds

        if self.min_delay_seconds > self.max_delay_seconds:
            raise ValueError("O tempo max de delay nao pode ser menor que o minimo")

    def registrar_promovidos(
        self,
        df_promovidos,  # spark df
        tz: Optional[timezone] = None,
        para_lenda_viva: bool = False,
        nome_missao: NomeMissao = NomeMissao.MISSAO_INTERNA,
    ):
        tz = tz or pytz.timezone('America/Sao_Paulo')

        rows = df_promovidos.select(
            "numero_whatsapp",
            "username",
            "vocativo",
            "eh_usuario_somente_teste",
            "tipo_template",
            "status_enviado",
            "status_atual",
        ).collect()

        # Inicializar variável para garantir que exista no bloco finally
        new_rows: list[MensagemEnviadaRow] = []

        try:
            for row in rows:
                user = row["username"]
                numero = row["numero_whatsapp"]

                if not numero or not user:
                    warnings.warn(f"Dados incompletos para o user '{user}', pulando.")
                    continue

                ts_envio = datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S")
                payload = self.__gerar_payload_hyperflow(
                    row, para_lenda_viva, nome_missao
                )

                try:
                    enviar_mensagem_whatsapp_via_hyperflow_regua(
                        client_id=CLIENT_ID_HYPERFLOW_PROD,
                        payload=payload,
                    )
                    print(
                        f"Mensagem enviada para o numero '{numero}' do usuario '{user}'"
                    )
                except requests.exceptions.RequestException as e:
                    warnings.warn(f"Falha ao enviar para o numero '{numero}': {e}")
                    continue

                # Fiz isso para gerar aleatoriedade nos tempos entre envios
                time.sleep(
                    random.uniform(self.min_delay_seconds, self.max_delay_seconds)
                )

                new_rows.append(
                    MensagemEnviadaRow(
                        "hyperflow",
                        numero,
                        user,
                        "Mensagem de 'subiu de nivel'",
                        ts_envio,
                        str(row["eh_usuario_somente_teste"]).upper(),
                        row["tipo_template"],
                        row["status_enviado"],
                        row["status_atual"],
                        id_missao="",
                        nome_missao=nome_missao.value,
                    )
                )
        finally:  # Garante que as mensagens sempre serao salvas na planilha
            self.__gravar_mensagens_enviadas_na_planilha(new_rows)
            print("Finalizando o envio...")
            print(f"Quantidade de mensagens processadas: {len(new_rows)}")

    def __gerar_payload_hyperflow(
        self, row, para_lenda_viva: bool, nome_missao: NomeMissao
    ) -> dict:
        if para_lenda_viva:
            tipo_mensagem = TipoMensagemReguaAtendente.SUBIU_NIVEL_LENDA.value
            data = {
                "nome_atendente": row["vocativo"],
                "nome_missao": nome_missao.value,
            }

        else:  # Se atingiu qualquer outro nivel que nao seja Lenda Viva
            tipo_mensagem = TipoMensagemReguaAtendente.SUBIU_NIVEL.value
            data = {
                "nome_atendente": row["vocativo"],
                "nivel_atingido": row["status_atual"].replace("_", " ").title(),
                "nome_missao": nome_missao.value,
                "premio_potencial": PREMIO_POTENCIAL_SEMANA,
                "nivel_final": "Lenda Viva",
            }

        payload = {
            "telefone_destinatario": str(
                row["numero_whatsapp"]
            ),  # Sem o codigo do pais
            "tipo_regua": "atendente",
            "tipo_mensagem": tipo_mensagem,
            **data,
        }

        return payload

    def __gravar_mensagens_enviadas_na_planilha(self, new_rows):
        # TODO: Salvar essas mensagens em uma tabela no datalake e criar dashes a partir disso
        if not new_rows:
            print("Nenhum promovido para registrar na planilha.")
            return

        new_rows_formatted = [row.to_sheet_row() for row in new_rows]
        self.sheet.append_rows(new_rows_formatted)
        print(f"Gravadas {len(new_rows_formatted)} pessoas promovidas na planilha.")
