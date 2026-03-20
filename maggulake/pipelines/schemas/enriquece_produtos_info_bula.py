from textwrap import dedent

import pyspark.sql.types as T
from pydantic import BaseModel, ConfigDict, Field

from maggulake.enums import (
    FaixaEtaria,
    FormaFarmaceutica,
    SexoRecomendado,
    TiposReceita,
    ViaAdministracao,
)

# TODO: Me parece super obvio... Deveria incluir o campo `tarja` aqui tambem.
# So precisa conferir se tem como conseguir tarja a partir da bula. Provavelmente sim!
# Mas como disse... Meio obvio, sera que ja nao tentaram?

enriquece_info_bula_schema = T.StructType(
    [
        T.StructField("ean", T.StringType(), True),
        T.StructField("nome_comercial", T.StringType(), True),
        T.StructField("principio_ativo", T.StringType(), True),
        T.StructField("dosagem", T.StringType(), True),
        T.StructField("eh_otc", T.BooleanType(), True),
        T.StructField("contraindicacoes", T.StringType(), True),
        T.StructField("efeitos_colaterais", T.StringType(), True),
        T.StructField("interacoes_medicamentosas", T.StringType(), True),
        T.StructField("instrucoes_de_uso", T.StringType(), True),
        T.StructField("advertencias_e_precaucoes", T.StringType(), True),
        T.StructField("condicoes_de_armazenamento", T.StringType(), True),
        T.StructField("validade_apos_abertura", T.StringType(), True),
        T.StructField("eh_controlado", T.BooleanType(), True),
        T.StructField("tipo_receita", T.StringType(), True),
        T.StructField("forma_farmaceutica", T.StringType(), True),
        T.StructField("via_administracao", T.StringType(), True),
        T.StructField("volume_quantidade", T.StringType(), True),
        T.StructField("idade_recomendada", T.StringType(), True),
        T.StructField("sexo_recomendado", T.StringType(), True),
        T.StructField("atualizado_em", T.TimestampType(), True),
    ]
)


# NOTE: https://www.gov.br/anvisa/pt-br/assuntos/farmacopeia/dcb


class EnriquecimentoInfoBula(BaseModel):
    model_config = ConfigDict(use_enum_values=True)
    # NOTE: A ideia é transformar cada description em um mini-prompt autocontido
    # e ultra-específico. Isso aumenta drasticamente a precisão do modelo ao
    # preencher cada campo individualmente.

    ean: str = Field(description="Seleciona o ean do produto sem modificação.")

    nome_comercial: str = Field(
        description=dedent("""
        [OBJETIVO]: Criar um nome comercial padronizado do medicamento com no máximo 60 caracteres.
        [REGRAS]:
        1.  Siga EXATAMENTE esta estrutura: [Nome Comercial] [Princípio Ativo] [Laboratório] [Dosagem] [Quantidade] [Forma Farmacêutica].
        2.  Utilize apenas informações presentes no texto-fonte.
        3.  Se necessário, abrevie ou remova palavras menos importantes para respeitar o limite de 60 caracteres.
        [EXEMPLO]: 'Crestor Rosuvastatina AstraZeneca 5mg 10 Comprimidos'.
        [O QUE NÃO FAZER]: Não invente informações. Não exceda o limite de caracteres.
        """),
    )
    # NOTE: no passado comecamos a usar esse enum porem comecou a estourar o enriquecimento...
    # O enum eh grande demais. Isso ja deu muito, muito problema.
    principio_ativo: str = Field(
        description=dedent("""
        [OBJETIVO]: Extrair o(s) princípio(s) ativo(s) do medicamento. Geralmente encontrados na seção "COMPOSIÇÃO" da bula.
        [REGRAS]:
        1.  Use a nomenclatura DCB (Denominação Comum Brasileira).
        2.  Se houver múltiplos princípios ativos, separe-os com ' + '.
        [EXEMPLO]: 'Paracetamol + Cafeína'.
        [O QUE NÃO FAZER]: Não inclua dosagens, laboratório, forma farmacêutica ou qualquer outra informação.
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        """),
    )
    dosagem: str = Field(
        description=dedent("""
        [OBJETIVO]: Extrair a dosagem/concentração do princípio ativo por unidade do medicamento.
        [DEFINIÇÃO]: Dosagem é a quantidade de princípio ativo presente na menor unidade do medicamento (1ml, 1g, etc).
        [REGRAS DE EXTRAÇÃO]:
        1.  Extraia APENAS a concentração do(s) princípio(s) ativo(s) com sua unidade de medida.
        2.  SEMPRE inclua a unidade de medida (mg, g, mcg, UI, mL, %, etc).
        3.  Para medicamentos com múltiplos princípios ativos, separe com ' + '.
        4.  Para concentrações por volume/peso, use o formato "quantidade/volume" (ex: 25mg/mL, 150mg/5mL).
        [EXEMPLOS CORRETOS]:
        - Comprimido de paracetamol 500mg → '500mg'
        - Comprimido com losartana 50mg + hidroclorotiazida 12,5mg → '50mg + 12,5mg'
        - Solução oral 25mg/mL → '25mg/mL'
        - Suspensão 150mg/5mL → '150mg/5mL'
        - Pomada com hidrocortisona 1% → '1%'
        - Insulina 100 UI/mL → '100UI/mL'
        [O QUE NÃO FAZER]:
        - Não confunda dosagem com quantidade total da embalagem (ex: "30 comprimidos" é volume_quantidade, não dosagem)
        - Não inclua informações sobre frequência de uso (ex: "tomar 2x ao dia")
        - Não invente dosagens que não estejam explícitas na bula
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        [DIFERENÇA IMPORTANTE]:
        - DOSAGEM = quantidade de princípio ativo por unidade (ex: '500mg' por comprimido)
        - VOLUME_QUANTIDADE = quantidade total de unidades na embalagem (ex: '30 comprimidos')
        """),
    )
    eh_otc: bool = Field(
        description=dedent("""
        [OBJETIVO]: Determinar se o medicamento é de venda livre (OTC - Over The Counter).
        [LÓGICA DE DECISÃO]:
        - Retorne `True` SE a bula NÃO mencionar necessidade de receita OU explicitamente indicar "venda livre".
        - Retorne `False` SE a bula mencionar termos como "Venda sob prescrição médica", "Receituário", "Retenção de receita".
        [FORMATO]: Booleano (`True` ou `False`).
        [O QUE NÃO FAZER]: Não use conhecimento externo. Baseie-se apenas na informação sobre prescrição contida na bula.
        """)
    )
    contraindicacoes: str = Field(
        description=dedent("""
        [OBJETIVO]: Resumir as contraindicações clinicamente mais importantes.
        [REGRAS DE EXTRAÇÃO]:
        1.  Liste as condições e grupos de pacientes onde o uso é proibido ou desaconselhado.
        2.  Priorize: alergias conhecidas, gestantes, lactantes, crianças e condições médicas graves (ex: insuficiência renal/hepática).
        [EXEMPLO]: 'Hipersensibilidade aos componentes da fórmula. Gravidez e lactação. Pacientes com insuficiência hepática grave.'
        [O QUE NÃO FAZER]: Não liste todas as contraindicações. Foque nas mais relevantes e de maior risco.
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        """),
    )
    efeitos_colaterais: str = Field(
        description=dedent("""
        [OBJETIVO]: Listar os efeitos colaterais mais comuns e frequentes.
        [REGRAS DE EXTRAÇÃO]: Extraia apenas as reações adversas descritas na bula como "comuns", "frequentes" ou que ocorrem em pelo menos ">1% a 10%" dos pacientes.
        [EXEMPLOS]:
        1. 'Náusea, dor de cabeça, tontura, dor abdominal.'
        2. Para o Roacutan, 'Ressecamento da pele' e 'olhos secos' são efeitos extremamente comuns, já a depressão e a síndrome de Stevens-Johnson são efeitos raros e muitas vezes não cientificamente comprovados.
        [O QUE NÃO FAZER]: Não inclua efeitos "raros", "muito raros" ou "incomuns". Nao invente efeitos que nao estejam na bula.
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        """),
    )
    interacoes_medicamentosas: str = Field(
        description=dedent("""
        [OBJETIVO]: Resumir as interações medicamentosas de maior relevância clínica.
        [LIMITE CRITICO]: Responda com no maximo 1024 caracteres, evitando que a resposta seja muito longa. Caso a resposta seja muito longa, priorize as interações mais comuns e clinicamente relevantes.
        [REGRAS DE EXTRAÇÃO]:
        1.  Extraia interações com outros medicamentos (classes ou nomes), alimentos e bebidas (álcool).
        2.  Para cada interação, descreva o efeito de forma sucinta (ex: "aumenta o risco de sangramento", "diminui a eficácia").
        [EXEMPLO]: 'Com anticoagulantes: aumenta risco de sangramento. Com antiácidos: diminui absorção. Álcool: potencializa efeito sedativo.'
        [O QUE NÃO FAZER]: Não liste interações de baixa relevância ou sem efeito clínico claro.
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        """),
    )
    instrucoes_de_uso: str = Field(
        description=dedent("""
        [OBJETIVO]: Extrair um guia prático de posologia e administração.
        [LIMITE CRITICO]: Mantenha a resposta com no máximo 1024 caracteres. Se a bula for muito detalhada, priorize as informações mais comuns e relevantes.
        [REGRAS DE EXTRAÇÃO]:
        1.  Descreva a dose recomendada por faixa etária (adultos, crianças).
        2.  Especifique a frequência (ex: "1 vez ao dia") e horários (ex: "após as refeições").
        3.  Inclua instruções sobre preparo e o que fazer em caso de esquecimento da dose.
        [EXEMPLO]: 'Adultos: 1 comprimido a cada 8 horas, após as refeições. Crianças acima de 30kg: 5ml, 2 vezes ao dia. Não exceder 7 dias de tratamento. Se esquecer uma dose, tomar assim que lembrar e reajustar os horários.'
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        """),
    )
    advertencias_e_precaucoes: str = Field(
        description=dedent("""
        [OBJETIVO]: Resumir os principais alertas de segurança para o paciente.
        [REGRAS DE EXTRAÇÃO]:
        1.  Extraia alertas para grupos de risco (idosos, gestantes, portadores de doenças crônicas).
        2.  Liste os sinais de alerta que exigem a interrupção do uso.
        3.  Inclua precauções sobre dirigir veículos ou operar máquinas.
        [EXEMPLO]: 'Monitorar a função renal em idosos. Interromper o uso em caso de reação alérgica. Pode causar sonolência, cuidado ao dirigir.'
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        """),
    )
    condicoes_de_armazenamento: str = Field(
        description=dedent("""
        [OBJETIVO]: Descrever as condições ideais de armazenamento.
        [REGRAS DE EXTRAÇÃO]: Extraia a faixa de temperatura, e cuidados com luz e umidade.
        [EXEMPLO]: 'Conservar em temperatura ambiente (entre 15°C e 30°C). Proteger da luz e umidade.'
        [O QUE NÃO FAZER]: Não inclua informações genéricas como "manter fora do alcance de crianças".
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        """),
    )
    validade_apos_abertura: str = Field(
        description=dedent("""
        [OBJETIVO]: Extrair o prazo de validade específico após o produto ser aberto.
        [REGRAS DE EXTRAÇÃO]:
        1.  Procure por frases como "após aberto, válido por X dias" ou "usar em até X meses".
        2.  Se a bula não especificar, retorne EXATAMENTE a string 'Não especificado na bula'.
        [EXEMPLO]: 'Válido por 30 dias após aberto.' ou 'Usar imediatamente após reconstituição.'
        """),
    )
    eh_controlado: bool = Field(
        description=dedent("""
        [OBJETIVO]: Determinar se o medicamento é de controle especial (Portaria 344/98). Leve em consideração o princípio ativo, dose e forma farmacêutica.
        [LÓGICA DE DECISÃO]:
        - Retorne `True` SE a bula mencionar "tarja preta", "receita azul/amarela", "psicotrópico", "entorpecente", ou "controle especial".
        - Retorne `False` SE a bula mencionar apenas "receita branca" ou "venda sob prescrição médica".
        [FORMATO]: Booleano (`True` ou `False`).
        [O QUE NÃO FAZER]: Não confundir receita branca comum com receita de controle especial.
        """)
    )

    volume_quantidade: str = Field(
        description=dedent("""
        [OBJETIVO]: Descrever a apresentação comercial completa do produto.
        [REGRAS]: Especifique a quantidade total de unidades e a apresentação.
        [EXEMPLO]: '20 comprimidos' ou '1 frasco com 120ml e copo-medida'.
        [O QUE NÃO FAZER]: Não liste apenas o número.
        [EM CASO DE AUSÊNCIA]: Retorne `null`.
        """),
    )

    # Enriquecimentos baseados em enums
    tipo_receita: TiposReceita = Field(
        description=dedent("""
        [OBJETIVO]: Indique qual é o tipo exato de receita exigida para a venda do produto.
        [OPÇÕES PRINCIPAIS]:
        - 'isento de prescrição médica': Para medicamentos OTC/venda livre
        - 'branca comum': Para medicamentos com receita médica simples
        - 'branca 2 vias': Para antibióticos com retenção de receita
        - 'a1 amarela', 'a2 amarela', 'a3 amarela': Para psicotrópicos (receita amarela)
        - 'b1 azul', 'b2 azul': Para entorpecentes (receita azul)
        - 'c1 branca 2 vias': Para substâncias controladas com retenção
        [EXEMPLO]: Se a bula mencionar "venda livre", use 'isento de prescrição médica'. Se mencionar "receita médica", use 'branca comum'.
        """)
    )

    forma_farmaceutica: FormaFarmaceutica = Field(
        description=dedent("""
        [OBJETIVO]: Atraves da bula do produto, indique a forma farmacêutica do produto.
        [EXEMPLOS COMUNS]:
        - 'comprimido': Para comprimidos sólidos
        - 'capsula dura': Para cápsulas gelatinosas duras
        - 'capsula mole': Para cápsulas gelatinosas moles
        - 'solucao oral': Para xaropes e soluções líquidas
        - 'creme': Para produtos tópicos cremosos
        - 'pomada': Para produtos tópicos oleosos
        - 'solucao gotas': Para colírios e soluções em gotas
        [REGRA]: Use exatamente a terminologia da ANVISA. Se houver dúvida entre opções similares, escolha a mais específica.
        """),
    )

    via_administracao: ViaAdministracao = Field(
        description=dedent("""
        [OBJETIVO]: Informe a via de administração correta do produto conforme a bula.
        [EXEMPLOS PRINCIPAIS]:
        - 'Oral': Para comprimidos, cápsulas, xaropes ingeridos
        - 'Oftálmica': Para colírios e pomadas oculares
        - 'Nasal': Para sprays e gotas nasais
        - 'Intravenosa': Para injeções endovenosas
        - 'Intramuscular': Para injeções no músculo
        - 'Retal': Para supositórios
        - 'Vaginal': Para óvulos e cremes vaginais
        [REGRA]: Base-se nas instruções de uso da bula. Se houver múltiplas vias, escolha a principal.
        """)
    )
    idade_recomendada: FaixaEtaria = Field(
        description=dedent("""
        [OBJETIVO]: Indique a faixa etária para a qual o produto é recomendado, com base na bula.
        [OPÇÕES DISPONÍVEIS]:
        - 'bebe': Para medicamentos indicados especificamente para bebês (ex: "indicado para menores de 2 anos")
        - 'crianca': Para medicamentos indicados para crianças (ex: "indicado para crianças acima de 2 anos")
        - 'adolescente': Para medicamentos indicados para adolescentes (ex: "indicado para adolescentes entre 12 e 18 anos")
        - 'jovem': Para medicamentos indicados para jovens adultos (ex: "indicado para jovens entre 18 e 25 anos")
        - 'adulto': Para medicamentos indicados para adultos (maiores de 18 anos)
        - 'idoso': Para medicamentos com indicação específica para idosos (ex: "indicado para maiores de 65 anos")
        - 'todos': Para medicamentos sem restrição etária específica
        [EXEMPLOS]:
        - Use 'bebe' se a bula mencionar "indicado para lactentes ou menores de 2 anos"
        - Use 'crianca' se a bula mencionar "indicado para crianças acima de 2 anos"
        - Use 'adolescente' se a bula especificar "para adolescentes entre 12 e 18 anos"
        - Use 'jovem' se a bula especificar "para jovens adultos entre 18 e 25 anos"
        - Use 'adulto' se a bula especificar "para adultos e adolescentes acima de 16 anos"
        - Use 'idoso' se a bula mencionar "indicado para idosos" ou "maiores de 65 anos"
        - Use 'todos' se não houver restrição etária mencionada
        [REGRA]: Base-se apenas nas informações explícitas da bula sobre faixa etária. Se houver indicação para mais de uma faixa, escolha a mais restritiva. Se não houver menção, use 'todos'.
        """)
    )

    sexo_recomendado: SexoRecomendado = Field(
        description=dedent("""
        [OBJETIVO]: Informe para qual sexo o produto é indicado, se houver especificação na bula.
        [OPÇÕES DISPONÍVEIS]:
        - 'homem': Para medicamentos específicos masculinos (ex: tratamento de próstata)
        - 'mulher': Para medicamentos específicos femininos (ex: anticoncepcionais, reposição hormonal)
        - 'todos': Para medicamentos sem restrição de sexo (maioria dos casos)
        [EXEMPLOS]:
        - Use 'mulher' para anticoncepcionais ou medicamentos ginecológicos
        - Use 'homem' para medicamentos urológicos específicos masculinos
        - Use 'todos' se não houver indicação específica de sexo na bula
        [REGRA]: Se não houver restrição mencionada na bula, use 'todos'.
        """)
    )


campos_obrigatorios_enriquece_bula = [
    "ean",
    "nome_comercial",
    "principio_ativo",
    "dosagem",
    "eh_otc",
    "contraindicacoes",
    "efeitos_colaterais",
    "interacoes_medicamentosas",
    "instrucoes_de_uso",
    "advertencias_e_precaucoes",
    "condicoes_de_armazenamento",
    "validade_apos_abertura",
    "eh_controlado",
    "tipo_receita",
    "forma_farmaceutica",
    "via_administracao",
    "volume_quantidade",
    "idade_recomendada",
    "sexo_recomendado",
]
