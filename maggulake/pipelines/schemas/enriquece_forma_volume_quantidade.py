from pydantic import BaseModel, ConfigDict, Field

from maggulake.enums import UnidadeMedida


class EnriquecimentoFormaVolumeQuantidade(BaseModel):
    model_config = ConfigDict(use_enum_values=True)
    forma_farmaceutica: str = Field(
        description="Seleciona o campo 'forma_farmaceutica' do produto sem modificação."
    )
    volume_quantidade: str = Field(
        description="Seleciona o campo 'volume_quantidade' do produto sem modificação."
    )
    quantidade: float = Field(
        description="Utilize os campos 'volume_quantidade' e 'unidade_medida' para calcular a quantidade total do produto. Por exemplo: Se um produto tem forma farmaceutica 'comprimido' e seu volume_quantidade é '10 comprimidos', então a quantidade é 10. Se o volume_quantidade é '17 ml', então a quantidade é 17. Se o volume_quantidade é '30 g', então a quantidade é 30."
    )
    unidade_medida: UnidadeMedida = Field(
        description="Identifique qual a unidade de medida mais apropriada para o produto, utilizando uma das opções disponíveis. Por exemplo, se o produto é comprimido, utilize 'comprimido'. Se o produto parece ser um liquido, então utilize 'ml'."
    )
