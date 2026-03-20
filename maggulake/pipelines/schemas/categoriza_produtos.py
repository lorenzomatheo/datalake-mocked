from textwrap import dedent

import pyspark.sql.types as T
from pydantic import BaseModel, ConfigDict, Field

from maggulake.enums.categorias import CategoriasWrapper

# === SCHEMAS PYDANTIC =========================================================

# NOTE: os números no max_itens foram um pouco arbitrários, confesso


class CategorizacaoNivel1(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    ean: str = Field(description="Seleciona o EAN do produto sem modificação.")

    super_categorias: list[str] = Field(
        default_factory=list,
        description=dedent(f"""
        [OBJETIVO]: Identificar a(s) SUPER CATEGORIA(S) do produto.

        [OPÇÕES DISPONÍVEIS]: {', '.join(CategoriasWrapper().get_all_super_categorias())}

        [REGRAS]:
        1. Analise as características do produto (nome, descrição, indicação).
        2. Escolha UMA ou MAIS super categorias que se aplicam.
        3. Um produto pode ter múltiplas categorias (ex: um kit pode ser "Perfumaria e Cuidados" + "Materiais para Saúde").
        4. Retorne uma lista com os nomes exatos das super categorias.
        5. IMPORTANTE: Se nenhuma categoria se encaixar perfeitamente, retorne uma lista vazia [].
        6. NÃO force o produto em uma categoria aproximada - seja rigoroso na classificação.

        [EXEMPLOS]:
        - "Shampoo Anticaspa Clear Men 400ml" → ["Perfumaria e Cuidados"]
        - "Kit Teste de Glicose + Lancetas" → ["Materiais para Saúde"]
        - "Sabão em Pó Omo 1kg" → ["Produtos para Casa"]
        - "Kit Higiene: Shampoo + Sabonete + Desodorante" → ["Perfumaria e Cuidados"]
        - "Produto sem categoria adequada" → []

        [O QUE NÃO FAZER]:
        - Não invente categorias que não estejam na lista.
        - Não retorne "Medicamentos" (isso é tratado separadamente).
        - NÃO escolha a "categoria mais próxima" se nenhuma for adequada.
        - Se nenhuma se aplicar perfeitamente, retorne uma lista vazia [].
        """),
    )


class CategorizacaoNivel2(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    ean: str = Field(description="Seleciona o EAN do produto sem modificação.")

    super_categoria: str = Field(
        description="A super categoria já identificada para este produto."
    )

    categorias_meso_micro: list[str] = Field(
        default_factory=list,
        description=dedent("""
        [OBJETIVO]: Identificar a(s) categoria(s) no formato "Meso Categoria -> Micro Categoria" OU apenas "Meso Categoria".

        [FORMATOS ACEITOS]:
        - "Meso Categoria -> Micro Categoria" (quando há uma micro categoria adequada)
        - "Meso Categoria" (quando NÃO há micro categoria adequada)

        [REGRAS IMPORTANTES]:
        1. Analise o produto considerando a super categoria já identificada.
        2. Escolha UMA ou MAIS categorias que se aplicam.
        3. Um produto pode ter múltiplas categorias (ex: shampoo anticaspa pode ser "Cuidado Capilar -> Shampoos" E "Cuidado Capilar -> Anti-caspa").
        4. **PRIORIDADE MÁXIMA**: Se o produto se encaixa na Meso Categoria MAS NÃO se encaixa em NENHUMA das Micro Categorias disponíveis, retorne APENAS "Meso Categoria".
        5. As categorias devem ser da lista de opções fornecida no prompt.
        6. IMPORTANTE: Se nenhuma categoria (nem meso) se encaixar perfeitamente, retorne uma lista vazia [].
        7. **MELHOR DEIXAR VAZIO DO QUE FORÇAR UMA CATEGORIA ERRADA!**

        [EXEMPLOS]:
        Super: "Perfumaria e Cuidados", Produto: "Shampoo Anticaspa"
        → ["Cuidado Capilar -> Shampoos", "Cuidado Capilar -> Anti-caspa"]

        Super: "Materiais para Saúde", Produto: "Medidor de Glicose"
        → ["Testes e Aparelhos -> Glicosímetros"]

        Super: "Medicamentos", Produto: "Hidroquinona 4%" (dermatologia mas não se encaixa em Acne/Anestésicos/Renovação)
        → ["Dermatologia"]

        Super: "Medicamentos", Produto: "Rivotril 2mg"
        → ["Sistema Nervoso -> Ansiedade"]

        Super: "Perfumaria e Cuidados", Produto: "Produto sem categoria adequada"
        → []

        [O QUE NÃO FAZER]:
        - Não invente categorias fora da lista fornecida.
        - NÃO force o produto em uma micro categoria se nenhuma for adequada.
        - NÃO escolha a "categoria mais próxima" por aproximação.
        - Se nenhuma se aplicar perfeitamente, retorne uma lista vazia [].
        """),
    )


# ==== SCHEMA PYSPARK ==========================================================

categorias_cascata_schema = T.StructType(
    [
        T.StructField("ean", T.StringType(), False),
        T.StructField(
            "categorias",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("super_categoria", T.StringType(), False),
                        T.StructField("meso_categoria", T.StringType(), False),
                        T.StructField(
                            "micro_categoria", T.StringType(), True
                        ),  # Pode ser null
                    ]
                )
            ),
            False,
        ),
        T.StructField("gerado_em", T.TimestampType(), False),
    ]
)

# Schema em formato string para create_table_if_not_exists
categorias_cascata_schema_string = """
    ean STRING NOT NULL,
    categorias ARRAY<STRUCT<
        super_categoria: STRING,
        meso_categoria: STRING,
        micro_categoria: STRING
    >> NOT NULL,
    gerado_em TIMESTAMP NOT NULL
"""
