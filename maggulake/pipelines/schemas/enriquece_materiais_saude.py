"""
Schema Pydantic para enriquecimento de produtos da categoria "Materiais para Saúde".

Meso categorias incluídas:
- Testes e Aparelhos (Glicosímetros, Tiras, Teste COVID, Teste de Gravidez, Medidor de Pressão,
  Oxímetros, Termômetros, Seringas, Acesso Venoso, Tesoura, Lancetas)
- Primeiros Socorros (Curativos, Gazes, Esparadrapos, Soluções, Máscaras, Luvas,
  Porta Comprimidos, Água Oxigenada, Algodão)
- Ortopedia (Joelheiras, Cintas, Muletas, Meias de Compressão, Tipoia, Munhequeira,
  Bermuda Térmica, Protetor Palmar, Bola de Massagem, Eletroestimulador)
- Inalação (Nebulizadores, Inaladores)
"""

from textwrap import dedent
from typing import Optional

import pyspark.sql.types as T
from pydantic import BaseModel, ConfigDict, Field


class EnriquecimentoMateriaisSaude(BaseModel):
    """Schema para enriquecimento de produtos de Materiais para Saúde."""

    model_config = ConfigDict(use_enum_values=True)

    ean: str = Field(description="Seleciona o EAN do produto sem modificação.")

    nome_comercial: str = Field(
        description=dedent("""\
        [OBJETIVO]: Criar um nome comercial padronizado do produto com no máximo 60 caracteres.
        [REGRAS]:
        1. Siga EXATAMENTE esta estrutura: [Tipo] [Marca] [Modelo] [Especificação] [Quantidade].
        2. Utilize apenas informações presentes no texto-fonte.
        [EXEMPLOS]:
        - 'Glicosímetro Accu-Chek Guide Kit Completo'
        - 'Tiras de Glicemia Accu-Chek Active 50un'
        - 'Curativo Tegaderm 3M Transparente 10x12cm 10un'
        - 'Termômetro Digital Geratherm Rapid'
        - 'Medidor de Pressão Omron HEM-7122'
        - 'Máscara N95 3M 9820 Sem Válvula'
        - 'Meia de Compressão Kendall 20-30mmHg M'
        - 'Nebulizador Omron NE-C803 Compressor'
        [O QUE NÃO FAZER]: Não invente informações. Não exceda 60 caracteres.
        """),
    )

    volume_quantidade: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Extrair a quantidade de itens na embalagem.
        [EXEMPLOS]:
        - '50 unidades'
        - 'Kit com 10 lancetas'
        - 'Caixa com 100 seringas'
        - 'Par (2 unidades)'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    indicacao: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever para que situação/condição o produto é indicado.
        [REGRAS]:
        1. Foque no uso clínico/doméstico principal.
        2. Máximo de 200 caracteres.
        [EXEMPLOS POR SUBCATEGORIA]:
        - Glicosímetros: 'Monitoramento de glicemia capilar em diabéticos'
        - Tiras de Glicemia: 'Tiras para glicosímetro Accu-Chek Active. Leitura rápida em 5 segundos.'
        - Teste Gravidez: 'Detecção precoce de gravidez. Resultado em 1 minuto.'
        - Teste COVID: 'Teste rápido de antígeno para SARS-CoV-2. Resultado em 15 minutos.'
        - Oxímetros: 'Medição de saturação de oxigênio (SpO2) e frequência cardíaca.'
        - Termômetros: 'Medição de temperatura corporal axilar, oral ou retal.'
        - Curativos: 'Proteção de feridas cirúrgicas, cateter venoso, queimaduras de 1º grau.'
        - Gazes: 'Limpeza e cobertura de ferimentos. Curativo estéril.'
        - Máscaras N95: 'Proteção respiratória contra aerossóis e partículas. Uso hospitalar.'
        - Luvas: 'Procedimentos médicos e odontológicos. Proteção contra contaminação.'
        - Meias de Compressão: 'Prevenção e tratamento de varizes. Edema em membros inferiores.'
        - Joelheiras: 'Suporte e estabilização do joelho. Prática esportiva ou recuperação.'
        - Nebulizadores: 'Administração de medicamentos por via inalatória. Tratamento respiratório.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    instrucoes_de_uso: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever como utilizar o produto corretamente.
        [REGRAS]:
        1. Ser objetivo e prático.
        2. Incluir passos principais. Máximo 300 caracteres.
        [EXEMPLOS POR SUBCATEGORIA]:
        - Tiras de Glicemia: 'Inserir tira no glicosímetro. Aplicar gota de sangue na área indicada.'
        - Teste Gravidez: 'Molhar a ponta absorvente na urina por 5 segundos. Aguardar 1 minuto.'
        - Curativos: 'Limpar e secar a ferida. Remover película, aplicar e alisar bordas.'
        - Gazes: 'Abrir embalagem estéril sem tocar na gaze. Aplicar sobre o ferimento.'
        - Meias de Compressão: 'Calçar pela manhã, antes de levantar. Posicionar o calcanhar corretamente.'
        - Nebulizadores: 'Conectar máscara ou bucal. Adicionar medicamento. Ligar o aparelho.'
        - Termômetros: 'Posicionar na axila por 60 segundos ou até sinal sonoro.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    advertencias_e_precaucoes: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Extrair alertas de segurança e precauções de uso.
        [EXEMPLOS]:
        - Tiras: 'Uso em até 6 meses após aberto. Não reutilizar. Manter frasco fechado.'
        - Seringas: 'Uso único. Descartar em recipiente adequado para perfurocortantes.'
        - Curativos: 'Não usar em feridas infectadas sem orientação médica.'
        - Nebulizadores: 'Higienizar máscara após cada uso. Não compartilhar acessórios.'
        - Meias Compressão: 'Não usar em caso de trombose aguda sem orientação médica.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )

    condicoes_de_armazenamento: Optional[str] = Field(
        default=None,
        description=dedent("""\
        [OBJETIVO]: Descrever como armazenar o produto.
        [EXEMPLOS]:
        - 'Manter em local seco, ao abrigo de luz e umidade.'
        - 'Conservar em temperatura ambiente (15-30°C).'
        - 'Tiras: manter frasco bem fechado. Não refrigerar.'
        [EM CASO DE AUSÊNCIA]: Retorne null.
        """),
    )


# Schema PySpark para criação de tabela Delta
enriquece_materiais_saude_schema = T.StructType(
    [
        T.StructField("ean", T.StringType(), False),
        T.StructField("nome_comercial", T.StringType(), True),
        T.StructField("volume_quantidade", T.StringType(), True),
        T.StructField("indicacao", T.StringType(), True),
        T.StructField("instrucoes_de_uso", T.StringType(), True),
        T.StructField("advertencias_e_precaucoes", T.StringType(), True),
        T.StructField("condicoes_de_armazenamento", T.StringType(), True),
        T.StructField("atualizado_em", T.TimestampType(), True),
    ]
)

colunas_obrigatorias_materiais_saude = [
    "ean",
    "nome_comercial",
]

schema_enriquece_materiais_saude = """
    ean STRING NOT NULL,
    nome_comercial STRING,
    volume_quantidade STRING,
    indicacao STRING,
    instrucoes_de_uso STRING,
    advertencias_e_precaucoes STRING,
    condicoes_de_armazenamento STRING,
    atualizado_em TIMESTAMP
"""
