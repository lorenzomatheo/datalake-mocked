from typing import List

from pydantic import BaseModel, Field

pergunta_doencas_cronicas = """Liste as doenças crônicas com base nos medicamentos controlados adquiridos pelo cliente.
Exemplos:
- Se o cliente compra medicamentos como insulina, sertralina ou antihipertensivos, a resposta poderia incluir 'Diabetes', 'Depressão', 'Hipertensão'.
- Se o cliente compra medicamentos como levotiroxina ou metformina, a resposta poderia incluir "Hipotireoidismo" ou "Sobrepeso".
- Se o cliente adquire medicamentos como Timoptol-XE Timolol Mundipharma , a resposta poderia incluir "Perda da visão", "Glaucoma" ou "Hipertensão ocular".
"""

pergunta_doencas_agudas = """Liste as doenças agudas associadas aos medicamentos comprados pelo cliente, normalmente de uso temporário curto.
Exemplos:
- Se o cliente compra antibióticos ou anti-inflamatórios, a resposta poderia incluir 'Resfriado', 'Infecção Urinária', 'Influenza'.
- Se o cliente compra medicamentos como a dipirona, que é um anti-inflamatório, a resposta poderia incluir 'Febre', 'Dor de garganta' ou 'Dor de cabeça'.
- Se o cliente compra medicamentos como cetirizina ou loratadina, a resposta poderia incluir 'Alergia' ou 'Rinite alérgica'.
- Se o cliente adquire medicamentos como omeprazol, a resposta poderia incluir 'Gastrite' ou 'Refluxo gastroesofágico'.
- Se o cliente compra medicamentos como ibuprofeno ou paracetamol, a resposta poderia incluir 'Dor muscular', 'Dores articulares' ou 'Inflamação pós-cirúrgica'.
"""


class EnriquecimentoTagsCliente(BaseModel):
    id: str = Field(description="Indique o id do cliente, sem realizar alterações.")
    cpf_cnpj: str = Field(
        description="Indique o número de identificação fiscal do cliente (CPF ou CNPJ), sem alterações."
    )
    doencas_cronicas: List[str] = Field(description=pergunta_doencas_cronicas)
    doencas_agudas: List[str] = Field(description=pergunta_doencas_agudas)
