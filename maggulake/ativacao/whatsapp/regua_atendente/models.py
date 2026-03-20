from dataclasses import dataclass


@dataclass
class MensagemEnviadaRow:
    id_remetente: str
    numero_destinatario: str
    username: str
    mensagem: str
    data_hora_envio: str
    testes: str
    tipo_mensagem: str
    status_anterior: str
    status_enviado: str
    id_missao: str = ""
    nome_missao: str = ""

    def to_dict(self) -> dict:
        return {
            "id_remetente": self.id_remetente,
            "numero_destinatario": self.numero_destinatario,
            "username": self.username,
            "mensagem": self.mensagem,
            "data_hora_envio": self.data_hora_envio,
            "testes": self.testes,
            "tipo_mensagem": self.tipo_mensagem,
            "status_anterior": self.status_anterior,
            "status_enviado": self.status_enviado,
            "id_missao": self.id_missao,
            "nome_missao": self.nome_missao,
        }

    def to_sheet_row(self) -> list:
        # Devolve do jeito que precisa para o colocar no append do sheet
        return [
            self.id_remetente,
            self.numero_destinatario,
            self.username,
            self.mensagem,
            self.data_hora_envio,
            self.testes,
            self.tipo_mensagem,
            self.status_anterior,
            self.status_enviado,
            self.id_missao,
            self.nome_missao,
        ]
