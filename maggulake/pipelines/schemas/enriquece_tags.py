from textwrap import dedent
from typing import ClassVar, Optional

from pydantic import BaseModel, ConfigDict, Field


class EnriquecimentoTags(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    # garante formato com | nas tags
    TAGS_PATTERN: ClassVar[str] = r"^[^|]+(\|[^|]+)*$"

    ean: str = Field(
        description=dedent("""
        [OBJETIVO]: Seleciona o ean do produto sem modificação.
        [REGRAS DE EXTRAÇÃO]:
        1. Este campo é OBRIGATÓRIO.
        2. NÃO retorne 'null' ou 'None'.
        """)
    )

    tags_complementares: Optional[str] = Field(
        default=None,
        pattern=TAGS_PATTERN,
        description=dedent("""
        [OBJETIVO]: Sugerir 3 a 5 tags de produtos vendidos em farmácia que COMPLEMENTAM o uso do produto principal.
        [CONTEXTO]:
        - Definição de COMPLEMENTAR: Produtos que são usados juntos ou imediatamente antes para maximizar o resultado ou eficácia do produto.
        [REGRAS DE EXTRAÇÃO]:
        1. Apenas produtos tipicamente vendidos em farmácias.
        2. Produtos que aumentam a eficácia do tratamento quando usados em conjunto.
        3. Produtos que fazem parte do mesmo protocolo de tratamento.
        4. Não inclua medicamentos controlados ou que exigem prescrição.
        5. A saída deve ser uma única string, com as tags separadas por '|' (pipe) e em UTF-8.
        6. A saída deve ter no máximo 200 caracteres.
        [EXEMPLOS]:
        - Para vitamina C: Zinco|Vitamina D|Própolis|Água de coco em pó|Complexo B
        - Para protetor solar: Hidratante labial com FPS|Vitamina C|Ácido hialurônico|Água termal|Antioxidantes
        - Para suplemento pré-treino feminino: Colágeno hidrolisado|L-carnitina|Vitamina D|Ômega 3|Biotina
        - Para vitamina infantil: Ômega 3 infantil|Vitamina D em gotas|Probiótico infantil|Multivitamínico infantil|Ferro quelado infantil
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        """),
    )

    tags_substitutos: Optional[str] = Field(
        default=None,
        pattern=TAGS_PATTERN,
        description=dedent("""
        [OBJETIVO]: Sugerir 3 a 5 tags para alternativas disponíveis em farmácias que podem SUBSTITUIR o produto principal.
        [CONTEXTO]:
        - Definição de SUBSTITUTO: Produtos com a mesma finalidade ou que podem ser usados no lugar do produto principal para obter um efeito similar.
        [REGRAS DE EXTRAÇÃO]:
        1. Produtos da mesma categoria terapêutica.
        2. Produtos com propósito similar ou mesma finalidade.
        3. Considere o gênero e faixa etária quando especificados no contexto.
        4. Produtos que são medicamento obrigatoriamente precisam seguir os critérios da anvisa de Intercambiabilidade:
            a. MEDICAMENTOS DE PRESCRIÇÃO (Tarjados): A intercambialidade é mandatória. As tags devem focar EXCLUSIVAMENTE em produtos com o MESMO princípio ativo, MESMA concentração e MESMA forma farmacêutica (Ex: Genéricos, Similares, ou variações de embalagem do mesmo AI). É PROIBIDO sugerir outros princípios ativos, mesmo que da mesma classe terapêutica.
            b. MEDICAMENTOS DE VENDA LIVRE (MIPs/OTCs): A intercambialidade é flexível. As tags PODEM sugerir produtos com princípios ativos diferentes ou concentrações diferentes, desde que sigam a Regra 1 (Mesma categoria) e Regra 2 (Propósito similar).
        5. Foque em CARACTERÍSTICAS FUNCIONAIS para a geração da tag
        6. A saída deve ser uma única string, com as tags separadas por '|' (pipe) e em UTF-8.
        7. A saída deve ter no máximo 200 caracteres.
        [REGRAS CRÍTICAS]:

        [EXEMPLOS]:
        - Para protetor solar facial FPS 50: Protetor físico mineral|BB cream com FPS 50|Protetor facial com cor|Sérum com FPS
        - Para whey protein: Proteína isolada da soja|Albumina|Colágeno hidrolisado|Proteína da ervilha|Blend proteico vegetal
        - Para desodorante masculino: Desodorante roll-on masculino|Antitranspirante aerosol masculino|Desodorante em barra masculino|Desodorante spray masculino
        - Para shampoo infantil: Shampoo bebê hipoalergênico|Sabonete líquido infantil|Shampoo sem lágrimas|Shampoo e condicionador 2 em 1 infantil
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        """),
    )

    tags_potencializam_uso: Optional[str] = Field(
        default=None,
        pattern=TAGS_PATTERN,
        description=dedent("""
        [OBJETIVO]: Sugerir 3 a 5 tags para produtos vendidos em farmácia que aumentam a EFICÁCIA ou BENEFÍCIOS do produto principal.
        [CONTEXTO]:
        - Definição de POTENCIALIZADOR: Produtos cuja aplicação conjunta visa aumentar o desempenho ou prolongar o benefício do produto.
        [REGRAS DE EXTRAÇÃO]:
        1. Produtos que melhoram a absorção, otimizam resultados ou maximizam benefícios.
        2. Itens que agem em sinergia para um resultado superior.
        3. Não incluir medicamentos controlados.
        4. Considere o gênero e faixa etária quando especificados no contexto.
        5. Foque em CARACTERÍSTICAS FUNCIONAIS para a geração da tag
        6. A saída deve ser uma única string, com as tags separadas por '|' (pipe) e em UTF-8.
        7. A saída deve ter no máximo 200 caracteres.
        [EXEMPLOS]:
        - Para colágeno: Vitamina C|Ácido hialurônico|Biotina|Zinco|Água de coco em pó
        - Para probiótico: Prebióticos|Fibras solúveis|Vitaminas do complexo B|Zinco|Água de coco
        - Para creme anti-idade feminino: Sérum de vitamina C|Ácido hialurônico|Retinol|Protetor solar FPS 50|Coenzima Q10
        - Para suplemento de cálcio pediátrico: Vitamina D3 infantil|Vitamina K2|Magnésio quelado|Zinco infantil|Ômega 3 para crianças
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        """),
    )

    tags_atenuam_efeitos: Optional[str] = Field(
        default=None,
        pattern=TAGS_PATTERN,
        description=dedent("""
        [OBJETIVO]: Sugerir 3 a 5 tags para produtos vendidos em farmácia que MINIMIZAM possíveis efeitos colaterais ou desconfortos.
        [CONTEXTO]:
        - Definição de ATENUADOR: Itens usados para reverter ou prevenir um possível dano/desconforto causado pelo produto.
        [REGRAS DE EXTRAÇÃO]:
        1. Produtos que aliviam desconfortos ou equilibram o organismo (ex: para ressecamento ou náuseas).
        2. Suplementos ou produtos que reduzem efeitos adversos comuns da categoria.
        3. Não incluir medicamentos controlados.
        4. Considere o gênero e faixa etária quando especificados no contexto.
        5. Foque em CARACTERÍSTICAS FUNCIONAIS para a geração da tag
        6. A saída deve ser uma única string, com as tags separadas por '|' (pipe) e em UTF-8.
        7. A saída deve ter no máximo 200 caracteres.
        [EXEMPLOS]:
        - Para antibiótico: Probióticos|Vitaminas do complexo B|Zinco|Hidratante labial|Protetor solar
        - Para isotretinoína: Hidratante labial|Protetor solar|Vitamina E|Ômega 3|Hidratante corporal
        - Para finasterida: Shampoo antiqueda|Biotina|Vitaminas do complexo B|Minoxidil tópico|Suplemento capilar masculino
        - Para ferro pediátrico: Vitamina C mastigável|Probiótico infantil em gotas|Fibras infantil|Multivitamínico líquido|Água de coco infantil
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        """),
    )

    tags_agregadas: Optional[str] = Field(
        default=None,
        pattern=TAGS_PATTERN,
        description=dedent("""
        [OBJETIVO]: Sugerir 3 a 5 tags para produtos de CONVENIÊNCIA ou relacionados ao perfil do consumidor (compra agregada/impulso).
        [CONTEXTO]:
        - Definição de AGREGADO: Itens que são comumente comprados junto com o produto principal para remoção, organização, portabilidade ou facilidade.
        [REGRAS DE EXTRAÇÃO]:
        1. Produtos da mesma linha de cuidados ou itens relacionados à condição de saúde geral.
        2. Produtos de prevenção associados ou itens de portabilidade e organização.
        3. Apenas produtos típicos de farmácia.
        4. Considere o gênero e faixa etária quando especificados no contexto.
        5. Foque em CARACTERÍSTICAS FUNCIONAIS para a geração da tag
        6. A saída deve ser uma única string, com as tags separadas por '|' (pipe) e em UTF-8.
        7. A saída deve ter no máximo 200 caracteres.
        [EXEMPLOS]:
        - Para vitamina pré-natal: Teste de gravidez|Ácido fólico|Protetor solar|Hidratante gestante|Creme para estrias
        - Para proteína em pó: Multivitamínico|Creatina|Repositor hidroeletrolítico|BCAA|Colágeno
        - Para absorvente feminino: Lenço íntimo feminino|Sabonete íntimo|Coletor menstrual|Calcinha absorvente|Analgésico para cólica
        - Para fralda infantil: Lenço umedecido|Pomada para assadura|Talco líquido|Sabonete líquido infantil|Hidratante infantil
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        """),
    )
