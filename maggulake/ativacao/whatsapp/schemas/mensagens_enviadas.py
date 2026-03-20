from dataclasses import dataclass
from datetime import datetime
from textwrap import dedent


@dataclass
class MensagemWhatsapp:
    id: str  # UUID
    remetente: str
    mensagem: str
    numero_destinatario: str
    id_atendente: str  # UUID
    username_atendente: str
    data_hora_envio: datetime
    eh_somente_teste: bool
    tipo_mensagem: str
    subtipo_mensagem: str | None

    def to_mensagem_enviada(
        self, status_request: str | None, response_request: dict | None
    ) -> "MensagemEnviada":
        return MensagemEnviada(
            id=self.id,
            remetente=self.remetente,
            mensagem=self.mensagem,
            numero_destinatario=self.numero_destinatario,
            id_atendente=self.id_atendente,
            username_atendente=self.username_atendente,
            data_hora_envio=self.data_hora_envio,
            eh_somente_teste=self.eh_somente_teste,
            tipo_mensagem=self.tipo_mensagem,
            subtipo_mensagem=self.subtipo_mensagem,
            status_request=status_request,
            response_request=response_request,
        )


@dataclass
class MensagemEnviada(MensagemWhatsapp):
    status_request: str | None
    """Status retornado pela API ao enviar a mensagem"""
    response_request: dict | None
    """Payload retornado pela API ao enviar a mensagem"""

    @staticmethod
    def to_sql_schema() -> str:
        # Atualizado para refletir os campos da classe
        return dedent("""\
            id STRING NOT NULL,
            remetente STRING NOT NULL,
            mensagem STRING NOT NULL,
            numero_destinatario STRING NOT NULL,
            id_atendente STRING NOT NULL,
            username_atendente STRING NOT NULL,
            data_hora_envio TIMESTAMP NOT NULL,
            eh_somente_teste BOOLEAN NOT NULL,
            tipo_mensagem STRING NOT NULL,
            subtipo_mensagem STRING,
            status_request STRING,
            response_request STRING
        """)


mensagens_enviadas_schema = MensagemEnviada.to_sql_schema()
