# Databricks notebook source
# MAGIC %md
# MAGIC # Define Coluna `eh_medicamento` via Regras e Regex
# MAGIC
# MAGIC Este notebook classifica produtos em medicamentos ou não-medicamentos usando:
# MAGIC
# MAGIC **Fases de classificação:**
# MAGIC 1. **Fase 1 - Regex (produtos com `eh_medicamento` NULL):**
# MAGIC    - Match com regex → `eh_medicamento = True`
# MAGIC    - Sem match → `eh_medicamento = False`
# MAGIC 2. **Fase 2 - Correções (base inteira da standard):**
# MAGIC    - Expressões de não-medicamentos → `eh_medicamento = False`
# MAGIC    - Nomes hardcoded de não-medicamentos → `eh_medicamento = False`
# MAGIC
# MAGIC Produtos que não correspondem a nenhuma regra não são salvos nesta tabela.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment

# COMMAND ----------
from datetime import datetime, timezone

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas.coluna_eh_medicamento_correcoes_manuais import schema

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "define_coluna_eh_medicamento",
    dbutils,
)

# COMMAND ----------

# Configurações
DEBUG = dbutils.widgets.get("debug") == "true"
spark = env.spark


# COMMAND ----------

# MAGIC %md
# MAGIC ## Lendo produtos da camada standard
# MAGIC

# COMMAND ----------

produtos_standard = env.table(Table.produtos_standard).cache()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Definição de Regras de Classificação

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seção 1: Padrões Regex para Medicamentos Genéricos
# MAGIC
# MAGIC Os padrões abaixo identificam produtos com alta probabilidade de serem
# MAGIC medicamentos genéricos com base em características do nome:
# MAGIC
# MAGIC | Padrão | Descrição | Exemplo |
# MAGIC |--------|-----------|---------|
# MAGIC | `^G\s+[A-Z\s\-\+]+\s+\d+(?:\+\d+)*\s*(MG|MCG)\b` | Genérico (inicia com G) | G Desogestrel 75 MCG |
# MAGIC | `^[^\d]+\s+\d+(?:\+\d+)*\s*(MG|MCG)\b` | Princípio ativo + dosagem | Ciclobenzaprina 5MG |
# MAGIC | `conte[uú]do` | Descrição com "conteúdo" | Conteudo: 500ml |

# COMMAND ----------

# Padrões regex para identificar medicamentos genéricos
medicamentos_genericos_patterns = [
    r"(?i)^G\s+[A-Z\s\-\+]+\s+\d+(?:\+\d+)*\s*(MG|MCG)\b",  # Genérico (inicia com G)
    r"(?i)^[^\d]+\s+\d+(?:\+\d+)*\s*(MG|MCG)\b",  # Princípio ativo + dosagem
    r"(?i)conte[uú]do",  # Descrições com "conteúdo"
]

# Combina todos os padrões em um único regex
regex_medicamentos = "|".join(medicamentos_genericos_patterns)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Seção 2: Expressões para Não-Medicamentos
# MAGIC
# MAGIC Produtos que contenham qualquer um desses termos no nome com certeza
# MAGIC não são medicamentos.

# COMMAND ----------

# Expressões LIKE para identificar não-medicamentos
expressoes_nao_medicamentos = [
    "%AGUA MINERAL%",
    "%FIO DENTAL%",
    "%WHEY PROTEIN%",
    "%PASTA DE AMENDOIM%",
    "%LOCAO BLOQUEADORA SOLAR FPS%",
    "%MASCARA DE PROTECAO%",
    "%REPARADOR DE PONTAS%",
    "%ESPARADRAPO%",
    "%PROTETOR SOLAR%",
    "%VICTORIA SECRET%",
    "%UNICORNIO%",
    "%TOUCA%",
    "%TINT MAXTON%",
    "%TEMPERO%",
    "%ESM RISQUE%",
    "%esmalte impala%",
    "%PROTETOR LABIAL%",
    "%ABS ALWAYS%",
    "%ABS CAREFREE%",
    "%ABS INTIMUS%",
    "%SAMSUNG%",
    "%PHILCO%",
    "%PHILIPS%",
    "%ACHOCOLATADO%",
    "%AGUA SANITARIA%",
    "%AGUARDENTE%",
    "%ALICATE%",
    "%ALMOFADA%",
    "%AMACIANTE%",
    "%ANA HICKMANN%",
    "%BOZZANO%",
    "%AP BARBEAR%",
    "%APAR BARBEAR%",
    "%APAR GILLET%",
    "%APARELHO DE BARBEAR%",
    "%GILLETE%",
    "%APARADOR DE PELOS%",
    "%APONTADOR%",
    "%BANANINHA%",
    "%BANHEIRA%",
    "%BARBEADOR%",
    "%BARRA DE CEREAL%",
    "%BERMUDA%",
    "%BISCOITO%",
    "%CABIDE%",
    "%CADERNO%",
    "%CANECA%",
    "%CHAPINHA%",
    "%COCA-COLA%",
    "%DISPLAY%",
    "%DROPS HALLS%",
    "%DROPS MENTOS%",
    "%DROPS FREEG%",
    "%EAU DE TOILETTE%",
    "%COLGATE%",
    "%ESM KOLOSS%",
    "%ESM IMPALA%",
    "%ESM HITS %",
    "% REXONA%",  # deixar com o espaço pra não pegar medicamento
    "%Tic Tac%",
    "%Vaselina%",
    "%LIXEIRA%",
    "%DHEA%",
]


# COMMAND ----------

# MAGIC %md
# MAGIC ### Seção 3: Nomes Hardcoded de Não-Medicamentos
# MAGIC
# MAGIC Lista de nomes completos de produtos que sabemos com certeza
# MAGIC que não são medicamentos.

# COMMAND ----------

nomes_nao_medicamentos = [
    "1 litro alcool 70%",
    "ALCOOL  70°     Conteudo: 5 LITROS",
    "ALCOOL  DE CEREAIS; -  Conteudo: 500ml (PURO)",
    "ALCOOL  GEL 70%; -  Conteudo: 1 Litro",
    "ALCOOL  GEL 70º GL; -  Conteudo: 250g;;(CUIDADO IN",
    "ALCOOL  ISOPROPILICO   Conteudo: 500 ml",
    "ALCOOL  ISOPROPILICO  -  Conteudo: 500 ml",
    "ALCOOL  ISOPROPILICO - Conteudo: 500 ml",
    "ALCOOL  ISOPROPILICO (50ML); -  Conteudo: 50 ml",
    "ALCOOL  ISOPROPILICO (70%);BASE GEL CARBOPOL (QSP)",
    "ALCOOL  ISOPROPILICO (100ML); -  Conteudo: 100 ml",
    "ALCOOL  ISOPROPILICO (QSP); -  Conteudo: 2 ml",
    "ALCOOL  ISOPROPILICO (QSP); -  Conteudo: 500 l",
    "ALCOOL  ISOPROPILICO; -  Conteudo: 500 ml",
    "ALCOOL (70%) -  Conteudo: 5 lt",
    "ALCOOL (70%); -  Conteudo: 500 ml",
    "ALCOOL (70%); -  Conteudo: 1000 ml",
    "ALCOOL 70  GEL 5KG (ASSEPTCIN)",
    "ALCOOL 70 °G  (QSP); -  Conteudo: 100ml;",
    "ALCOOL 70 5 LITROS LORENZETTI",
    "ALCOOL 70 1000ML",
    "ALCOOL 70 ANTISSEPTICO 150ML",
    "ALCOOL 70 GEL 170 G C/HIDRAT.",
    "ALCOOL 70 GRAUS 1L VANTAX",
    "ALCOOL 70%    Conteudo: 1 litro",
    "ALCOOL 70%  -  Conteudo: 10 l",
    "ALCOOL 70%  -  Conteudo: 250 ml",
    "ALCOOL 70%  SOLUÇAO PARA ASSEPSIA   Conteudo: 1LIT",
    "alcool 70% -  Conteudo: 1000 ml",
    "ALCOOL 70% - 1 LITRO - SPRAY",
    "ALCOOL 70% (70%);AGUA DESTILADA (QSP); -  Conteudo",
    "alcool 70% 1 litro",
    "ALCOOL 70% 5 LITROS",
    "ALCOOL 70% 100ML",
    "ALCOOL 70% 500ML SP VITA CELULAS,",
    "ALCOOL 70% 1000ML",
    "ALCOOL 70% ESS ALFAZEMA",
    "Álcool 70% Hm Cosméticos frasco com 500mL de gel de uso dermatológico",
    "ALCOOL 70% LIQ 5LITROS",
    "ALCOOL 70% LIQUIDO 1 L",
    "Álcool 70% Zulu frasco com 1L de solução de uso dermatológico",
    "ALCOOL 70%-  Conteudo: 5 LITROS",
    "ALCOOL 70º G  -  Conteúdo: 5 Litros (CUIDADO LÍQUI",
    "ALCOOL 70º G (CUIDADO INFLAMAVEL); -  Conteudo: 5",
    "ALCOOL 70º G (CUIDADO INFLAMAVEL); -  Conteudo: 5 Litros ;;",
    "ALCOOL 70º G (CUIDADO INFLAMAVEL); -  Conteudo: 5Litros",
    "ALCOOL 70º G (CUIDADO LIQUIDO INFLAMAVEL; -  Conte",
    "ALCOOL 70º GL (CUIDADO INFLAMAVEL); -  Conteudo: 5",
    "ALCOOL 70º; -  Conteudo: 1 LITRO(CUIDADO LIQUIDO I",
    "ALCOOL 70ºGL (7%);CREME NAO IONICO (QSP); -  Conte",
    "ALCOOL 70ºGL (70%); -  Conteudo: 100 ml",
    "ALCOOL 96 GL (70%) COM ESSENCIA;CREME NAO IONICO (",
    "ALCOOL 96 GL (70%)-  Conteudo: 5 l",
    "ALCOOL 96 GL (70%); -  Conteudo: 1 kg",
    "ALCOOL 96 GL (70%); -  Conteudo: 100 g",
    "ALCOOL 96 GL (70%); -  Conteudo: 500 g",
    "ALCOOL 96 GL (70%);ESSENCIA ERVA DOCE (0,02%); -",
    "ALCOOL 96 GL (70%);ESSENCIA VANILLA (1%);CREME NAO",
    "ALCOOL 96 GL (QSP); -  Conteudo: 250 g",
    "ALCOOL 96%",
    "ALCOOL 96º G  -  Conteudo: 500ml;",
    "ALCOOL 96º G ; -  Conteudo: 1Litro (CUIDADO LIQUID",
    "ALCOOL 96º G ; -  Conteudo: 5 Litros (CUIDADO LIQU",
    "ALCOOL 96º G (puro); -  Conteudo: 30ml;;",
    "ALCOOL ANHANGUERA 1L 70% HIDRATADO,",
    "BALAS DE GENGIBRE EX FORTE 40G",
    "BALAS GENGIBRE E HORTELA  40G,",
    "BALAS GENGIBRE ROMA E ACEROLA 40G,",
    "BANDAGEM ADESIVA PRETO 2,5CMX5M",
    "BARRA DE CEREAL TRIO BANANA AVEIA  E MEL 20G,",
    "BATATA BEM BRASIL HASH BROWN 400G MINI,",
    "BATATA DOCE CHIPS 200G",
    "BATATA PALHA BUEN APETIT 300GR,",
    "BATATA POTATA CHIPS 110G QUEIJO",
    "BATATA PRINGLES SABOR ORIGINAL 146 G",
    "BATOM ESSENCIAL, COR 82",
    "BATOM LIQ MAT MET-BONECA L3497",
    "BATOM LIQ. MATTE ULT HD GLOW",
    "BATOM MATTE RK FUCHSIA",
    "BATOM MATTE RK RISS RED",
    "ESCOVA DE CABELO",
    "ESCOVA DENTAL GUM BEBE PATRULHA CANINA",
    "ESM ANITA CUIDAR DA MENTE 10ML",
    "ESM EXTRAVASA DIAMOND, 9ML ROXO AMETISTA",
    "ESM RISQUE HIPOALERG, 10,5ML PINK FLUOR",
    "ESM UP COLORS CREMOSO, 10ML NUDE CLASSICO",
    "ESMALTE  CICLOPIROX  8%  (KIT 10 LIXAS ACETONA)",
    "ESMALTE RISQUE, BIANCO PURISSIMO",
    "ESPONJA BELLESA APLICADORA DE BASE",
    "ESPONJA DE MAQUIAGEM FRACIONADA",
    "ESPONJA DE PÓ, COM SEDA",
    "ESPONJA ESF LIMPEZA FACIAL VERM,",
    "ESPONJA FACIAL MARCO BON, C/6",
    "ESPONJA KONJAC CARV RK CLE05BR",
    "ESPONJA LIMPPANO MULTIUSO 4X1",
    "ESPONJA MAGIC MAKEUP",
    "ESPONJA MAQ. PO/BASE",
    "ESPONJA P/ BASE CHARM 06UN",
    "ESPONJA P/BASE QUADRAD SPO02BR",
    "ESPONJA PARA MAQUIAGEM BEAUTY BLENDER 360 PL01 PCT",
    "HALLS CREAM MORANGO, 28G",
    "KAIAK MASCULINO DESODORANTE CO",
    "LENCO REMOVEDOR MAQUIAGEM DETOX 25UN KISS",
    "LEVEDO CERVEJA  (500MG); -  Conteudo: 120 caps",
    "LEVEDO CERVEJA (500MG); -  Conteudo: 30 caps",
    "LEVEDO CERVEJA PO (500MG); -  Conteudo: 60 caps",
    "LEVEDO DE CERVEJA 200G 400CPR",
    "LEVEDO DE CERVEJA 450MG C/120",
    "LEVEDO DE CERVEJA 500MG 120CPS, SOFTGEL",
    "LEVEDO DE CERVEJA PLUS + COMP B  400CP",
    "LEVEDURA DE CERVEJA (20MG);EXCIPIENTE 1 (QSP); -",
    "LEVEDURA DE CERVEJA (300MG); -  Conteudo: 30 caps",
    "LEVEDURA DE CERVEJA (QSP); -  Conteudo: 100 g",
    "LEVEDURA DE CERVEJA 250MG 60CAPS",
    "LEVEDURA DE CERVEJA 500 MG C/200 CAPS - A.E",
    "LEVEDURA DE CERVEJA 500MG  60CAPS",
    "LEVEDURA DE CERVEJA, C/400 CPR",
    "LIXA ESFOLIANTE FINGIRS, PEDRA",
    "LOC PENTEAR DESALFY HAIR, 290ML CRESCIMENT VEGETAL",
    "LOCAO BRONZEADORA FPS 8 c/ oleo de urucum; -  Conteudo: 150ml (com perfume)",
    "Loção Capilar Minoxidil 5% Miligrama frasco spray com 250mL de loção de uso capilar",
    "LOCAO ESFOLIANTE PARA O CORPO c/ microesferas; -",
    "LOCAO HIDRATANTE C/ ureia; -  Conteudo: 110ml;;",
    "LOCAO HIDRATANTE CORPORAL c/ oleo de amendoas doce",
    "LOCAO HIDRATANTE PARA O CORPO c/ oleo de amendoas, oleo de macadamia, silicone; -  Conteudo: 110ml;; (com perfume)",
    "LOCAO HIDRATANTE PARA PELE EXTRA SECO C/ UREIA;OLE",
    "MASC BIO EXTRATUS, 250GR SPECIALISTE C/PIMEN",
    "MASC BOTOX D-TOX S/FORMOL C/1KG,",
    "MASC DESALFY HAIR, 250GR RECONSTRUTORA",
    "MASC DESALFY HAIR, 300GR COLOR GOLD TONALIZANTE COR PRETA",
    "MASC DESALFY HAIR, 300GR COLOR GOLD TONALIZANTE COR VERMELHA",
    "MASC DESALFY HAIR, 1000GR NEUTRALIZANTE",
    "MASC RASSI, 350GR EFEITO MODELADOR",
    "MASC TRIPLA CIRURGICA PROTECT CARE C/50UN BRANCA,",
    "MASC. CIRURGICA TRIPLA DESCARTAVEL C/50 INFANTIL DRF",
    "MASCARA   KN95  1 UNIDADE",
    "MÁSCARA CAPILAR RECONSTRUÇÃO INTENSIVA FORBELLE PROFESSIONAL - 300G",
    "MASCARA CIRURGICA ELAST 1UN",
    "MASCARA CIRURGICA ELASTICA PRIME HEALTH  50UN",
    "MASCARA CIRURGICA FACEMASK TRIPLA C/5 UN",
    "MASCARA CIRURGICA TRIPLA BRANCA 20UN",
    "MASCARA DE PROT PRETA KN95 C/1 UNI,",
    "MASCARA DE PROTEÇÃO",
    "MASCARA DE TECIDO ALGODAO LAVAVEL 3UND",
    "MASCARA DESC. TUBERCULOSE KN-95 - PFF2 ROSA",
    "MASCARA KN95 PRETA 1X1**",
    "MASCARA PFF2 KN95 PRETA CX.C/10 UND",
    "MASCARA PROTECAO,",
    "MASCARA RESPIRATORIA COM VALVULA PFF1",
    "MASCARA TRIPLA DISPOSABLE FACE AZUL C/50UN, NAO INFORMADA",
    "MASCARAS CIRURGICA TRIPLA  BRA",
    "MASCARAS PROTETORAS FACIAIS",
    "NATURA - BATOM FACES CORAL DOC",
    "NATURA - COL MASC BIOGRA/VERAO",
    "NATURA - CR PENT EKOS ANDIROBA",
    "NATURA - DES REFIL KAIAK PULSO",
    "NATURA - DES ROLL-ON KAIAK FEM",
    "NATURA - DES SPRAY F  KAIAK UR",
    "NATURA - DES SPRAY FACES INSPI",
    "NATURA - DES SPRAY HUMOR AR",
    "NATURA - EMB PASCOA 2011",
    "NATURA BASE AQUAR BEGE TRANSLC",
    "NATURA BATOM LAPIS ROSA",
    "NATURA DEOCORPORAL SINT NOITE",
    "NATURA DES ERVA DOCE 100 ML",
    "NATURA ERVA DOCE DES ROLLON",
    "NATURA FACES BATOM",
    "NATURA SOU SHAMPOO",
    "NATURA TODO DIA HIDRAT 300 ML ALGODAO",
    "OLEO ALHO 250MG CAP C60 MAIS ALHO",
    "OLEO COCO 1000MG 60CAPS BIOFHI",
    "OLEO CORP PAIXAO, 200ML SEDUTORA",
    "OLEO CORPO/CORPO NATURA",
    "OLEO DE ALGODAO (5%); UREIA (5%);COLD CREAM (QSP);",
    "OLEO DE ALHO 60CPS",
    "OLEO DE ALHO 250MG  -  Conteudo: 60 doses",
    "OLEO DE ALHO 250MG C/60CPS",
    "OLEO DE ALHO 500MG 60 CAPS,",
    "OLEO DE ALHO 500MG 60CP",
    "OLEO DE ALHO C/45CP HERBARIUM",
    "OLEO DE ALHO CAPS OLEOSA 60CPS); -  Conteudo: 60 c",
    "OLEO DE ALHO, 250 MG C/40 CPS",
    "OLEO DE AMENDOAS;UREIA (10%); -  Conteudo: 100 g",
    "OLEO DE BORRAGEM (900MG); -  Conteudo: 30 caps",
    "OLEO DE BORRAGEM 500MG",
    "OLEO DE BORRAGEM 500MG 30 CAP",
    "OLEO DE BORRAGEM SEVERAL, 500MG C/60CPS",
    "OLEO DE CANFORA (QSP); -  Conteudo: 100 ml",
    "OLEO DE CARTAMO+COCO 60CAPS",
    "OLEO DE CARTAMO+COCO, 1200 MG C/60 CAPS",
    "OLEO DE CARTAMO+OLEO DE COCO 1000MG C/60CAPS,",
    "OLEO DE COCO 20 ML EXTRA VIRGEM",
    "OLEO DE COCO 60CAPS 1GR,",
    "OLEO DE COCO 60CPAS 1000MG LABOR",
    "OLEO DE COCO 100MG C/60 CAPS.",
    "OLEO DE COCO 200ML",
    "OLEO DE COCO 500ML EXTRA VIRGE",
    "OLEO DE COCO 1000MG 60 CAPS FITO VIDA,",
    "OLEO DE COCO 1000MG 60CAPS DENATURE",
    "OLEO DE COCO 1000MG 120CPS, SOFTGEL",
    "OLEO DE COCO 1000MG C/60 CAPS",
    "OLEO DE COCO 1000MG C/60CAPS",
    "OLEO DE COCO EXTRA VIRGEM 100ML UNIFABRA",
    "OLEO DE COCO EXTRA VIRGEM 120ML VILA",
    "OLEO DE COCO EXTRA VIRGEM 500 ML NAC POUCH",
    "OLEO DE COCO EXTRA VIRGEM 500ML",
    "OLEO DE COCO EXTRAVIRGEM 200ML",
    "OLEO DE COCO NATUSER 1000MG 60",
    "OLEO DE COCO VIRGEM ORGAN 300ML POTE,",
    "OLEO DE COPAIBA 20ML BIO FLORA",
    "OLEO DE COPAIBA 250MG - 30 CAPS",
    "OLEO DE COPAIBA 500MG C/60 CAPS",
    "OLEO DE COPAIBA DA AMAZONIA 30ML",
    "OLEO DE COPAIBA UNIMEL 15ML",
    "OLEO DE COPAIBA,",
    "OLEO DE LAVANDA BOURBON, 100G",
    "OLEO DE LINHAÇA 1GR 30 CAPS",
    "OLEO DE LINHACA 1GR C/ 60 CAPS",
    "OLEO DE LINHACA 500 MG -  Conteudo: 30un;",
    "OLEO DE LINHACA 500 MG CAPS -  Conteudo: 40un;;",
    "OLEO DE LINHACA 500 MG CAPS -  Conteudo: 180un;;",
    "OLEO DE LINHACA 500 MG CAPS ; -  Conteudo: 60un;;",
    "OLEO DE LINHAÇA 500MG C/50CAPS (M3007)",
    "OLEO DE LINHACA 1000MG 60CAPS,",
    "OLEO DE LINHACA CASAGRANDE 60 CPS 500 MG",
    "OLEO DE PEIXE 60CPS 42GR,",
    "PAR DE LUVAS DESCARPAC TAM P C",
    "PASTA AMENDOIM BEIJINHO 500GR",
    "PINCEL P/ TINTURA GRANDE",
    "PINCEL PARA BASE PEQUENO, CORRETIVO CURVEX",
    "PROT SOLAR BANANA BOAT, 236ML FPS 8",
    "PROT SOLAR PAYOT, FPS 30",
    "PROT SOLAR TOPZ 120ML FPS 2 OLEO",
    "PROT. SOLAR ISACARE FACIAL FPS50 60ML BEGE CLARO",
    "PROTEINA DE AMENDOIM PACOCA 900G",
    "PROTETOR BIOAGE CORPORAL BIOSUN PROTECT FPS30 150G",
    "PROTETOR SPECTRABAN , FPS50 60G",
    "PROTETOR SUN FPS 10",
    "REMOV SEM ACETONA 100ML",
    "REMOVEDOR DE ESMALTES S/ACETON",
    "REMOVEDOR DE UNHAS POSTIÇ, POTE GIVE ME5",
    "REMOVEDOR S/ ACETONA 100ML",
    "REP DE PONTAS MARIANA 30ML CERAMIDAS",
    "REP DE PONTAS NATIVA HAIR ALOE VERA 30ML",
    "REP DE PONTAS OLEO REPARADOR",
    "REP DE PONTAS SILI PONTAS 35ML CERAMIDAS",
    "REP PONTAS ABACATE, 30ML",
    "REP PONTAS NATIVA HAIR MANTEIGA KARITE 30ML",
    "REPAR DE PONTAS 35ML QUERATINA,",
    "REPAR DE PONTAS DESALFY , 30ML",
    "REPAR DE PONTAS EXTRACTUS, 30ML OIL BRILHO DA SEDA",
    "REPAR DE PONTAS FORTEBOM ARGAN 30ML",
    "REPAR PONTAS FIOVIT ABACATE 30",
    "REPAR.DE PONTAS BELEZZURA 35ML CERAMIDAS",
    "REPARADOR PONTAS OLEO ARGAN",
    "REPARADOR PONTAS UMID HAIR QUERATINA",
    "SAB 123 BABY GLICERINADO 90G",
    "SAB ACT LIV ENXOFRE ANTIACNE 90GR",
    "SAB BABY GLICERINA 90G",
    "SAB INTIMO FINA FLOR, 160ML BRISA FRESCA",
    "SAB LIQ BABY CARE COM CAMOMILA 150ML",
    "SAB LIQ CAMOMILA E CAPIM LIMÃO 500ML",
    "SAB LIQ HIDRAD REF PESSEGO 500ML,",
    "SAB LIQ THERACNE 120ML,",
    "SAB OTILY REFIL, 1000ML FRUTAS VERMELHAS",
    "SAB PHEBO, 100G ALFAZEMA",
    "SAB PHYTOERVAS ESFOLIANTE 90G MARACUJA E MORANGO",
    "SAB.SENSUS ANTIBACTERIANO 90G HIDRATANTE",
    "SAB.SENSUS VEGETAL 120G ESFOLIANTE",
    "SABAO LIQUIDO PITANGA",
    "SABAO OLA 0500ML LIQ LAVA ROUPA COCO(E)",
    "SABONETE ACIDO GLICOLICO (10%)(QSP); -  Conteudo:",
    "SABONETE DE AC SALICLICO 5%   Conteudo: 100 ml",
    "SABONETE DE AROEIRA",
    "SABONETE FACIAL c/ glicerina. calendula; -  Conteudo: 200ml;;(com perfume)",
    "SABONETE FLORAL 1L",
    "SABONETE GLICERINA  90 g",
    "SABONETE INTIMO BELKIT 200ML, EQUILIBRIO",
    "SABONETE INTIMO CEREJA 200ML",
    "SABONETE INTIMO CIA DA NATREZA OLEO DE COCO 210ML",
    "SABONETE INTIMO DERMYTRAT 200ML, CRISTAL",
    "SABONETE INTIMO MENTA BIOINNOVA 200ML",
    "SABONETE LIQ CAPIM LIMAO 250ML",
    "SABONETE LIQ INTIMO",
    "SABONETE LIQ INTIMO BLACK ICE 200ML",
    "SABONETE LIQ PROTEGE LAVANDA 500ML",
    "SABONETE LIQ. ESSENCIADIFIORI NATURE P MAOS E FACE 350ML",
    "SABONETE LIQUIDO BABY CARE GLICERINADO 500ML",
    "SABONETE LÍQUIDO C/ CALÊNDULA; CAMOMILA -  Conteúd",
    "SABONETE LIQUIDO FRUTAS VERMELHAS 500ML",
    "SABONETE LIQUIDO HIDRATANTE-  Conteudo: 100 ml",
    "SABONETE LIQUIDO INTIMO ABOVE PH EQUILIBRADO 200ML",
    "SABONETE LIQUIDO INTIMO AVVIO DERMAHIGI NEUTRO 2 UN 200ML",
    "SABONETE LIQUIDO SOFTUS FRUTAS VERMELHAS 500ML",
    "SACO LIXO EMBALIXO PRETO 15L,",
    "SACOLA EMBAL BAGI BRANCA, 30/40 OXIBIODEGRADAVEL",
    "SACOLA MULTIFARMA, 60X80 PCT 500",
    "SACOLA PLAST SLIM 30X40X015 BR",
    "SACOLA VIRGEM BRANCA PP 25X35 1000X1",
    "SEM.LINHACA NATURAL.130G",
    "SEMENTE DE LINHACA 200G",
    "SEMENTE DE LINHAÇA 200GR",
    "SEMENTE DE LINHACA DOURADA FAVO DE MEL 150G ,",
    "SEMENTE DE SUCUPIRA 30G",
    "SEMENTE LINHACA DOURADA 200G",
    "VODKA SMIRNOFF RED GAR VD 998M",
    "SORO FIS BASA 500 ML",
    "SORO FISIOLOGICO 0,9 % 500ML",
    "SORO FISIOLOGICO 0,9% 125ML HALEX",
    "SORO FISIOLOGICO 0,9% 250ML",
    "SORO FISIOLOGICO 0,9% 1000ML",
    "Soro Fisiológico 0,9% Farmax 0,9%, frasco com 100mL de solução de uso dermatológico",
    "Soro Fisiológico 0,9% Panvel, frasco com 100mL de solução",
    "SORO FISIOLOGICO 0,9% SORIMAX",
    "SORO FISIOLOGICO 240ML C/T(12)- FARMAX IVA:O",
    "SORO FISIOLOGICO 500ML ADV",
    "Soro Fisiológico Sorimax, 1 frasco com 240mL de solução de uso dermatológico",
    "Soro Fisiológico Sorimax, frasco com 500mL de solução de uso dermatológico",
    "WHEY 3W MORANGO 907G",
    "WHEY ABSOLUT HIGH PROTEIN REFIL BAUNILHA 900GR",
    "WHEY ESSENTIAL BAUNILHA 900G",
    "WHEY MIX PROTEIN ABSOLUT 900G SB SORVETE DE BAUNILHA",
    "WHEY NO2 MORANGO 900G,",
    "TV 50 LED CONSUMO",
    "TV 50P LCD LED PHIL.50PUG7406",
    "CAFETEIRA 15 CAFES 220W",
    "Acai Mineirinho Guarana E Leite Em Po 220g",
    "Barra Chocolate Branco Supremo Divine 70g",
    "Condicionador Malbec Club  Antiqueda 50ml Boticario",
    "Maleta De Maquiagem, Aluminio Vermelha",
    "Mentos Stick Frutas",
    "Pirulito Pop Gum Pinta Lingua",
    "Tintura De Maracuja (qsp); -  Conteudo: 100 Ml",
    "Tintura De Passiflora (qsp); -  Conteudo: 100 Ml",
    "Ultravita  Cha Verde Po",
    "Umidificador Ar Ultrassonico Wu125 2,5l,",
    "Umidificador Branco/rosa 3,3l Lg",
    "Umidificador De Ar Lelong",
    "Wafer Bauducco Chocolate 140g,",
    "Xarope Commel Misto De Maracujá",
    "Shampoo Dove Cachos Ativos + Biotina 370ml",
    "*brilho Labial Ruby Rose",
    "Abs.mili C/abas Suave C/8",
    "Mascara Facial Alfalagos Kn95 ,",
    "Mascara Para Cilios E Delinead",
    "Protetor Punho Elastico Largo",
    "Tablete Garoto Serenata De Amor 90g,",
    "Tintura De Propolis",
    "Voxx Whey Isolate Choc 900g Pt,",
    "Abs.absorbex Gel Abas 10un",
    "Ac Fitico (2%);ac Kojico (3%);alfa Arbutim (2%);ac",
    "Agulha Hip  13 X 4,5",
    "Agulha, 30x08",
    "Vitamina C 500mg",
    "Vitanol-a Creme 0 05% 30gr",
    "Aromatizante Bucal Beijao Limao Com Mel 15 Ml",
    "Barra Proteina Choc/ca",
    "Bcaa 2000 120cps  Fitoway,",
    "Cafeina 1gr 60 Cpls",
    "Alcool  Isopropilico (1%); -  Conteudo: 2000 Ml",
    "Alcool  Isopropilico (70%);creme Nao Ionico (qsp);",
    "Chocolate Lacta Tabua Ao Leite 150g",
    "Sab Liq Folha Nativa, Anis Verde 500ml",
    "Chiclete Fini Classicos 14g",
    "Doce Beijo C/ Chocolate Ouro De Minas",
    "Bcaa Pro Endurance C/60 Caps (",
]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Classificação dos Produtos

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fase 1: Classificar Produtos sem `eh_medicamento` via Regex
# MAGIC
# MAGIC Filtra apenas produtos com `eh_medicamento IS NULL` da camada standard.
# MAGIC - Match com regex → `eh_medicamento = True`
# MAGIC - Sem match → `eh_medicamento = False`

# COMMAND ----------

# Filtra apenas produtos sem classificação (eh_medicamento IS NULL)
produtos_sem_classificacao = produtos_standard.filter(F.col("eh_medicamento").isNull())

if DEBUG:
    total_sem_classificacao = produtos_sem_classificacao.count()
    print(f"\n📊 Produtos com eh_medicamento NULL: {total_sem_classificacao}")

# COMMAND ----------

# Aplica regex: match → True, no match → False
produtos_classificados_regex = produtos_sem_classificacao.select(
    F.col("ean"),
    F.when(F.col("nome").rlike(regex_medicamentos), F.lit(True))
    .otherwise(F.lit(False))
    .alias("eh_medicamento"),
    F.lit(datetime.now(timezone.utc)).alias("atualizado_em"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fase 2: Correções via Expressões e Nomes Hardcoded
# MAGIC
# MAGIC Atua na **base inteira** da standard para corrigir classificações
# MAGIC incorretas, forçando `eh_medicamento = False`.

# COMMAND ----------

# Cria condição para expressões de não-medicamentos
condicao_expressoes = F.lit(False)
for exp in expressoes_nao_medicamentos:
    condicao_expressoes = condicao_expressoes | F.col("nome").like(exp)

# Cria condição para nomes hardcoded
condicao_nomes = F.col("nome").isin(nomes_nao_medicamentos)

# Combina expressões e nomes
condicao_correcao = condicao_expressoes | condicao_nomes

# Filtra da base INTEIRA (não apenas NULL) para correção
correcoes_nao_medicamentos = produtos_standard.filter(condicao_correcao).select(
    F.col("ean"),
    F.lit(False).alias("eh_medicamento"),
    F.lit(datetime.now(timezone.utc)).alias("atualizado_em"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### União dos Resultados
# MAGIC
# MAGIC Correções (Fase 2) sobrescrevem classificações da Fase 1 quando há conflito.

# COMMAND ----------

# Une regex (Fase 1) com correções (Fase 2)
# Em caso de duplicatas, a correção (expressões/nomes) tem prioridade
produtos_unidos = produtos_classificados_regex.unionByName(correcoes_nao_medicamentos)

# Remove duplicatas: correções (False) prevalecem sobre regex (True)
window_prioridade = Window.partitionBy("ean").orderBy(F.col("eh_medicamento").asc())
produtos_para_salvar = (
    produtos_unidos.withColumn("row_num", F.row_number().over(window_prioridade))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview dos Resultados

# COMMAND ----------

if DEBUG:
    count_regex_true = produtos_classificados_regex.filter(
        F.col("eh_medicamento") == True
    ).count()
    count_regex_false = produtos_classificados_regex.filter(
        F.col("eh_medicamento") == False
    ).count()
    count_correcoes = correcoes_nao_medicamentos.count()
    count_total = produtos_para_salvar.count()

    print("\n" + "=" * 70)
    print("📊 REPORT: Classificação de Produtos")
    print("=" * 70)
    print("\n📋 Fase 1 — Regex (produtos NULL):")
    print(f"   ✅ Medicamentos (regex match): {count_regex_true}")
    print(f"   ❌ Não-medicamentos (regex no match): {count_regex_false}")
    print("\n📋 Fase 2 — Correções (base inteira):")
    print(f"   🔧 Correções (expressões + nomes): {count_correcoes}")
    print(f"\n📦 Total de produtos classificados (deduplicado): {count_total}")
    print("=" * 70)

    # Exemplos de medicamentos identificados via regex
    print("\n>>> Exemplos de MEDICAMENTOS identificados via regex:")
    medicamentos_exemplo = (
        produtos_classificados_regex.filter(F.col("eh_medicamento") == True)
        .alias("med")
        .join(
            produtos_standard.select("ean", "nome").alias("standard"),
            on="ean",
            how="inner",
        )
        .select("ean", "nome", "eh_medicamento")
        .limit(10)
    )
    medicamentos_exemplo.display()

    # Exemplos de correções
    print("\n>>> Exemplos de CORREÇÕES (expressões + nomes):")
    correcoes_exemplo = (
        correcoes_nao_medicamentos.alias("corr")
        .join(
            produtos_standard.select("ean", "nome").alias("standard"),
            on="ean",
            how="inner",
        )
        .select("ean", "nome", "eh_medicamento")
        .limit(10)
    )
    correcoes_exemplo.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar na tabela de enriquecimento usando MERGE

# COMMAND ----------

# Cria a tabela se não existir
env.create_table_if_not_exists(
    Table.coluna_eh_medicamento_correcoes_manuais,
    schema,
)

# COMMAND ----------

# Realiza o MERGE (upsert) na tabela de enriquecimento
delta_table = DeltaTable.forName(
    spark, Table.coluna_eh_medicamento_correcoes_manuais.value
)

delta_table.alias("target").merge(
    produtos_para_salvar.alias("source"), "target.ean = source.ean"
).whenMatchedUpdate(
    set={
        "ean": "source.ean",
        "eh_medicamento": "source.eh_medicamento",
        "atualizado_em": "source.atualizado_em",
    }
).whenNotMatchedInsert(
    values={
        "ean": "source.ean",
        "eh_medicamento": "source.eh_medicamento",
        "atualizado_em": "source.atualizado_em",
    }
).execute()

print(
    f"✅ Classificações salvas na tabela {Table.coluna_eh_medicamento_correcoes_manuais}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar resultado final

# COMMAND ----------

if DEBUG:
    df_resultado = env.table(Table.coluna_eh_medicamento_correcoes_manuais)
    total_registros = df_resultado.count()

    print(f"\nTotal de produtos na tabela de correções: {total_registros}")

    # Distribuição por eh_medicamento
    print("\n📊 Distribuição por eh_medicamento:")
    df_resultado.groupBy("eh_medicamento").count().orderBy("eh_medicamento").display()

    print("\n>>> Últimas atualizações:")
    df_resultado.orderBy(F.col("atualizado_em").desc()).limit(10).display()

# COMMAND ----------

# TODO: criar uma lógica para permitir excluir determinados produtos (id ou ean) dessa tabela caso necessário.
# Isso pode ser importante caso algum termo seja erroneamente inserido nesse notebook e depois a gente precise voltar atrás.
