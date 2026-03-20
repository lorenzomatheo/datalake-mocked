from typing import Optional

from pydantic import BaseModel, Field


class MarcaFabricanteResponse(BaseModel):
    marca: Optional[str] = Field(
        default=None,
        description="Nome comercial / Marca do produto (ex: Dorflex, Nivea, Vult). Máximo 30 caracteres. Null se não identificar.",
    )
    fabricante: Optional[str] = Field(
        default=None,
        description="Empresa que produz o produto (ex: Sanofi, Beiersdorf). Máximo 50 caracteres. Null se não identificar.",
    )
