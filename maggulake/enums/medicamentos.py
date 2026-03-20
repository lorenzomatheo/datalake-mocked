from .extended_enum import ExtendedEnum


# opções oriundas do filtro de Tarja do site da Anvisa (https://consultas.anvisa.gov.br/#/medicamentos/)
class TiposTarja(ExtendedEnum):
    PRETA = "Preta"
    VERMELHA = "Vermelha"
    VERMELHA_SOB_RESTRICAO = "Vermelha Sob Restrição"
    PRETA_E_VERMELHA = "Preta e Vermelha"
    SEM_TARJA = "Sem Tarja"


# enum alinhado às formas farmacêuticas consolidadas a partir do catálogo interno de formas farmacêuticas (ENUMs_01/2026 - https://docs.google.com/spreadsheets/d/1Z3s7jJD4i4iAwc0ahObouNlkXX--K3iGOZXGN8t3HYw/edit?gid=540268361#gid=540268361)
class FormaFarmaceutica(ExtendedEnum):
    ADESIVO = "adesivo"
    ANEL = "anel"
    BARRA = "barra"
    BASTAO = "bastao"
    CAPSULA_DURA = "capsula dura"
    CAPSULA_DURA_DE_LIBERACAO_PROLONGADA = "capsula dura de liberacao prolongada"
    CAPSULA_DURA_DE_LIBERACAO_RETARDADA = "capsula dura de liberacao retardada"
    CAPSULA_MOLE = "capsula mole"
    CAPSULA_MOLE_DE_LIBERACAO_PROLONGADA = "capsula mole de liberacao prolongada"
    CAPSULA_MOLE_DE_LIBERACAO_RETARDADA = "capsula mole de liberacao retardada"
    COMPRIMIDO = "comprimido"
    COMPRIMIDO_DE_LIBERACAO_MODIFICADA = "comprimido de liberacao modificada"
    COMPRIMIDO_DE_LIBERACAO_PROLONGADA = "comprimido de liberacao prolongada"
    COMPRIMIDO_EFERVESCENTE = "comprimido efervescente"
    COMPRIMIDO_MASTIGAVEL = "comprimido mastigavel"
    COMPRIMIDO_ORODISPERSIVEL = "comprimido orodispersivel"
    COMPRIMIDO_PARA_COLUTORIO = "comprimido para colutorio"
    COMPRIMIDO_PARA_SOLUCAO = "comprimido para solucao"
    COMPRIMIDO_PARA_SUSPENSAO = "comprimido para suspensao"
    COMPRIMIDO_REVESTIDO = "comprimido revestido"
    COMPRIMIDO_REVESTIDO_DE_LIBERACAO_PROLONGADA = (
        "comprimido revestido de liberacao prolongada"
    )
    COMPRIMIDO_REVESTIDO_DE_LIBERACAO_RETARDADA = (
        "comprimido revestido de liberacao retardada"
    )
    DISPOSITIVO_INTRA_UTERINO = "dispositivo intra-uterino"
    FILME = "filme"
    GLOBULO = "globulo"
    GOMA_DE_MASCAR = "goma de mascar"
    GRANULADO = "granulado"
    GRANULADO_EFERVESCENTE = "granulado efervescente"
    GRANULADO_PARA_SOLUCAO = "granulado para solucao"
    GRANULADO_PARA_SUSPENSAO = "granulado para suspensao"
    GRANULADO_REVESTIDO = "granulado revestido"
    GRANULADO_REVESTIDO_DE_LIBERACAO_PROLONGADA = (
        "granulado revestido de liberacao prolongada"
    )
    GRANULADO_REVESTIDO_DE_LIBERACAO_RETARDADA = (
        "granulado revestido de liberacao retardada"
    )
    IMPLANTE = "implante"
    PASTILHA_DURA = "pastilha dura"
    PASTILHA_GOMOSA = "pastilha gomosa"
    PO = "po"
    PO_AEROSOL = "po aerosol"
    PO_EFERVESCENTE = "po efervescente"
    PO_LIOFILIZADO_PARA_SOLUCAO_INJETAVEL = "po liofilizado para solucao injetavel"
    PO_LIOFILIZADO_PARA_SUSPENSAO_INJETAVEL = "po liofilizado para suspensao injetavel"
    PO_LIOFILIZADO_PARA_SUSPENSAO_INJETAVEL_DE_LIBERACAO_PROLONGADA = (
        "po liofilizado para suspensao injetavel de liberacao prolongada"
    )
    PO_PARA_COLUTORIO = "po para colutorio"
    PO_PARA_SOLUCAO = "po para solucao"
    PO_PARA_SOLUCAO_INJETAVEL = "po para solucao injetavel"
    PO_PARA_SOLUCAO_PARA_INFUSAO = "po para solucao para infusao"
    PO_PARA_SUSPENSAO = "po para suspensao"
    PO_PARA_SUSPENSAO_INJETAVEL = "po para suspensao injetavel"
    PO_PARA_SUSPENSAO_INJETAVEL_DE_LIBERACAO_PROLONGADA = (
        "po para suspensao injetavel de liberacao prolongada"
    )
    RASURA = "rasura"
    SABONETE = "sabonete"
    SUPOSITORIO = "supositorio"
    OVULO = "ovulo"
    TABLETE = "tablete"
    EMULSAO = "emulsao"
    EMULSAO_AEROSOL = "emulsao aerosol"
    EMULSAO_GOTAS = "emulsao gotas"
    EMULSAO_INJETAVEL = "emulsao injetavel"
    EMULSAO_PARA_INFUSAO = "emulsao para infusao"
    EMULSAO_SPRAY = "emulsao spray"
    ESMALTE = "esmalte"
    ESPUMA = "espuma"
    LIQUIDO = "liquido"
    OLEO = "oleo"
    SABONETE_LIQUIDO = "sabonete liquido"
    COLUTORIO = "colutorio"
    COLUTORIO_SPRAY = "colutorio spray"
    ELIXIR = "elixir"
    SOLUCAO = "solucao"
    SOLUCAO_AEROSOL = "solucao aerosol"
    SOLUCAO_DE_LIBERACAO_PROLONGADA = "solucao de liberacao prolongada"
    SOLUCAO_GOTAS = "solucao gotas"
    SOLUCAO_INJETAVEL = "solucao injetavel"
    SOLUCAO_PARA_DILUICAO = "solucao para diluicao"
    SOLUCAO_PARA_DILUICAO_INJETAVEL = "solucao para diluicao injetavel"
    SOLUCAO_PARA_DILUICAO_PARA_COLUTORIO = "solucao para diluicao para colutorio"
    SOLUCAO_PARA_DILUICAO_PARA_INFUSAO = "solucao para diluicao para infusao"
    SOLUCAO_PARA_INFUSAO = "solucao para infusao"
    SOLUCAO_PARA_IRRIGACAO = "solucao para irrigacao"
    SOLUCAO_SPRAY = "solucao spray"
    SUSPENSAO = "suspensao"
    SUSPENSAO_AEROSOL = "suspensao aerosol"
    SUSPENSAO_DE_LIBERACAO_PROLONGADA = "suspensao de liberacao prolongada"
    SUSPENSAO_DE_LIBERACAO_RETARDADA = "suspensao de liberacao retardada"
    SUSPENSAO_GOTAS = "suspensao gotas"
    SUSPENSAO_INJETAVEL = "suspensao injetavel"
    SUSPENSAO_INJETAVEL_DE_LIBERACAO_PROLONGADA = (
        "suspensao injetavel de liberacao prolongada"
    )
    SUSPENSAO_SPRAY = "suspensao spray"
    XAMPU = "xampu"
    XAROPE = "xarope"
    CREME = "creme"
    EMPLASTO = "emplasto"
    GEL = "gel"
    POMADA = "pomada"
    PASTA = "pasta"
    GAS = "gas"


class ClasseTerapeutica(ExtendedEnum):
    ACAO_INIBIDORES_PREOTEINA_KINASE_ANTINEOPLASICOS_BCR_ABL = (
        "Ação Inibidores Preoteína Kinase Antineoplásicos, BCR-ABL"
    )
    ADSTRINGENTE = "Adstringente"
    AGENTE_DIAGNOSTICO_POR_IMAGEM_PARA_GASTROENTEROGRAFIA = (
        "Agente Diagnóstico Por Imagem Para Gastroenterografia"
    )
    AGENTE_DIAGNOSTICO_POR_IMAGEM_PARA_RESSONANCIA_MAGNETICA = (
        "Agente Diagnóstico Por Imagem Para Ressonância Magnética"
    )
    AGENTE_DIAGNOSTICO_POR_IMAGEM__BAIXA_OSMOLARIDADE_PARA_ANGIO_UROGRAFIA = (
        "Agente Diagnóstico Por Imagem, Baixa Osmolaridade Para Angio-Urografia"
    )
    AGENTE_DIAGNOSTICO_POR_IMAGEM__IONICOS_PARA_ANGIO_UROGRAFIA = (
        "Agente Diagnóstico Por Imagem, Iônicos Para Angio-Urografia"
    )
    AGENTES_ANTI_REUMATICOS_ESPECIFICOS = "Agentes Anti-Reumáticos Específicos"
    AGENTES_ANTIALERGICOS_NASAIS = "Agentes Antialérgicos Nasais"
    AGENTES_ANTINEOPLASICOS_ALCALOIDES_DA_VINCA = (
        "Agentes Antineoplásicos Alcalóides da Vinca"
    )
    AGENTES_ANTINEOPLASICOS_ALQUILANTES = "Agentes Antineoplásicos Alquilantes"
    AGENTES_ANTINEOPLASICOS_ANTIBIOTICOS = "Agentes Antineoplásicos Antibióticos"
    AGENTES_ANTINEOPLASICOS_ANTIMETABOLITOS = "Agentes Antineoplásicos Antimetabólitos"
    AGENTES_ANTINEOPLASICOS_CAMPTOTECINAS = "Agentes Antineoplásicos Camptotecinas"
    AGENTES_ANTINEOPLASICOS_PODOFILOTOXINAS = "Agentes Antineoplásicos Podofilotoxinas"
    AGENTES_ANTINEOPLASICOS_TAXANOS = "Agentes Antineoplásicos Taxanos"
    AGENTES_ANTINEOPLASICOS_VINCA_ALCALOIDES = (
        "Agentes Antineoplásicos Vinca Alcalóides"
    )
    AGENTES_CARDIACOS_DOPAMINERGICOS = "Agentes Cardíacos Dopaminérgicos"
    AGENTES_DESINTOXICANTES_PARA_O_TRATAMENTO_DE_NEOPLASIAS = (
        "Agentes Desintoxicantes Para O Tratamento De Neoplasias"
    )
    AGENTES_DIURETICOS_POUPADORES_DE_POTASSIO_COM_DIURETICOS_DE_ALCA = (
        "Agentes Diuréticos Poupadores De Potássio Associados Com Diuréticos De Alça"
    )
    AGENTES_DIURETICOS_POUPADORES_DE_POTASSIO_COM_TIAZIDAS_E_OU_ANALOGOS = "Agentes Diuréticos Poupadores De Potássio Associados Com Tiazidas E/Ou Análogos"
    BACTERIOSTATICO = "Bacteriostático"
    BETABLOQUEADORES_COM_ANTIHIPERTENSIVOS_E_OU_DIURETICOS = (
        "Betabloqueadores Associados com Antihipertensivos e/ou Diuréticos"
    )
    BETABLOQUEADORES_PUROS = "Betabloqueadores Puros"
    BISFOSFONATOS_PARA_ALTERACOES_DO_CALCIO_RELACIONADAS_A_TUMORES = (
        "Bisfosfonatos Para Alterações Do Cácio Relacionadas A Tumores"
    )
    BISFOSFONATOS_PARA_OSTEOPOROSE_E_ALTERACOES_RELACIONADAS = (
        "Bisfosfonatos Para Osteoporose E Alterações Relacionadas"
    )
    BPH_ANTAGONISTAS_ALFA_ADRENERGICOS_PUROS = (
        "Bph Antagonistas Alfa-Adrenérgicos Puros"
    )
    BPH_COMBINACOES_ALFA_ANTAGONISTAS_E_INIBIDORES_DA_5_ALFA_REDUTASE = "Bph Combinações De Alfa-Antagonistas E Inibidores Da 5-Alfa Testosterona Redutase"
    BPH_COMBINACOES_INIBIDORES_DA_5_ALFA_REDUTASE_E_OU_ALFA_ANTAGONISTAS = "BPH Combinações de Inibidores da 5-Alfa Testosterona Redutase e/ou Alfa-Antagonistas com Outras Substâncias"
    BPH_INIBIDORES_DA_5_ALFA_REDUTASE_PUROS = (
        "Bph Inibidores Da 5-Alfa Testosterona Redutase (5-Ari) Puros"
    )
    BRONCODILATADORES = "Broncodilatadores"
    BRONCODILATOR_E_EXPECTORANTE = "Broncodilator e Expectorante"
    CALCITONINAS = "Calcitoninas"
    CALICIDA = "Calicida"
    CALMANTE_FITOTERAPICO = "Calmante Fitoterápico"
    CARBAPENEMES_E_PENEMES = "Carbapenemes E Penemes"
    CARBENICILINAS_E_SIMILARES = "Carbenicilinas E Similares"
    CARDIOGLICOSIDEOS_PUROS = "Cardioglicosídeos Puros"
    CASTANHA_DA_INDIA = "Castanha da Índia"
    CEFALOSPORINAS_INJETAVEIS = "Cefalosporinas Injetáveis"
    CEFALOSPORINAS_ORAIS = "Cefalosporinas Orais"
    CICATRIZANTE = "Cicatrizante"
    CITOSTATICOS_INIBIDORES_DA_AROMATASE = "Citostáticos Inibidores Da Aromatase"
    CLORANFENICOIS_E_ASSOCIACOES = "Cloranfenicois E Associações"
    COLERETICOS_E_COLECINETICOS = "Coleréticos E Colecinéticos"
    COMBINACOES_ANTIDIABETICAS_DE_INIBIDORES_DE_SGLT2_E_DPP_IV = (
        "Combinações Antidiabéticas de Inibidores de SGLT2 E DPP-IV"
    )
    COMPLEXO_ANTIINIBIDOR_COAGULACAO = "Complexo Antiinibidor-Coagulação"
    COMPLEXO_B_PURO = "Complexo B Puro"
    COMPOSTOS_ANTINEOPLASICOS_DE_PLATINA = "Compostos Antineoplásicos De Platina"
    CONTRACEPTIVO_SISTEMICO_DE_EMERGENCIA = "Contraceptivo Sistêmico De Emergência"
    CORTICOESTEROIDES_COM_ANTIBACTERIANOS = (
        "Corticoesteróides Associados A Antibacterianos"
    )
    CORTICOESTEROIDES_COM_ANTIMICOTICOS = "Corticoesteróides Associados A Antimicoticos"
    DEPURATIVO_E_DIGESTIVO = "Depurativo e Digestivo"
    DESCONGESTIONANTE_NASAL = "Descongestionante Nasal"
    DESCONGESTIONANTES_NASAIS = "Descongestionantes Nasais"
    DESCONGESTIONANTES_OFTALMOLOGICOS_SIMPATICOMIMETICOS = (
        "Descongestionantes Oftalmológicos, Simpaticomiméticos"
    )
    DIGESTIVOS_INCLUINDO_ENZIMAS_DIGESTIVAS = "Digestivos, Incluindo Enzimas Digestivas"
    DISTURBIOS_GASTRICOS = "Distúrbios Gástricos"
    DIURETICO_E_ANTIHIPERTENSIVO = "Diurético e Antihipertensivo"
    DIURETICOS = "Diuréticos"
    DIURETICOS_DE_ALCA_PUROS = "Diuréticos De Alça Puros"
    DIURETICOS_SIMPLES = "Diuréticos Simples"
    DIURETICOS_TIAZIDAS_E_ANALOGOS_PUROS = "Diuréticos Tiazidas E Análogos Puros"
    DROGAS_PARA_TRATAMENTO_DE_HANSENIASE = "Drogas Para Tratamento De Hanseníase"
    ENZIMAS_ANTIINFLAMATORIAS = "Enzimas Anti-inflamatórias"
    ECTOPARASITICIDAS_INCLUINDO_ESCABICIDAS = "Ectoparasiticidas Incluindo Escabicidas"
    EMOLIENTES_PROTETORES_DERMATOLOGICOS = "Emolientes Protetores Dermatológicos"
    EMULSOES_DE_GORDURAS_PURAS = "Emulsôes De Gorduras, Puras"
    ERITROPOIETINAS = "Eritropoietínas"
    ESQUISTOSSOMICIDAS = "Esquistossomicidas"
    ESTABILIZADORES_DO_HUMOR = "Estabilizadores Do Humor"
    ESTATINAS_INIBIDORES_DA_REDUTASE_HMG_COA = (
        "Estatinas, Inibidores Da Redutase Hmg-Coa"
    )
    ESTIMULANTE_DO_APETITE = "Estimulante do Apetite"
    ESTIMULANTES_CARDIACOS_EXCETO_DOPAMINERGICOS = (
        "Estimulantes Cardíacos Excluindo Agentes Dopaminérgicos."
    )
    ESTIMULANTES_RESPIRATORIOS = "Estimulantes Respiratórios"
    ESTOMATOLOGICOS = "Estomatológicos"
    ESTROGENOS_EXCETO_G3A_G3E_G3F = "Estrógenos Excluindo G3a, G3e, G3f"
    EXPECTORANTE_FITOTERAPICO = "Expectorante Fitoterápico"
    EXPECTORANTES = "Expectorantes"
    EXPECTORANTES_BALSAMICOS_E_MUCOLITICO = "Expectorantes Balsâmicos e Mucolítico"
    FATOR_VIII = "Fator VIII"
    FATOR_XIII = "Fator XIII"
    FATORES_ESTIMULANTES_DE_COLONIAS = "Fatores Estimulantes De Colônias"
    FATORES_II_VII_IX_X = "Fatores II VII IX X"
    FERRO_PURO = "Ferro Puro"
    FIBRATOS = "Fibratos"
    FIBRINOGENIO = "Fibrinogênio"
    FIBRINOLITICOS = "Fibrinolíticos"
    FITOTERAPIA_HORMONAL = "Fitoterapia Hormonal"
    FITOTERAPICO = "Fitoterápico"
    FLUORQUINOLONAS_INJETAVEIS = "Fluorquinolonas Injetáveis"
    FLUORQUINOLONAS_ORAIS = "Fluorquinolonas Orais"
    GASTROPROCINETICOS = "Gastroprocinéticos"
    GLICOCORTICOIDES_SISTEMICOS = "Glicocorticóides Sistêmicos"
    GLICOCORTICOIDES_TOPICOS_ASSOCIACAO_MEDICAMENTOSA = (
        "Glicocorticóides Tópicos - Associação Medicamentosa"
    )
    GLICOCORTICOIDES_TOPICOS_SIMPLES_EXCETO_USO_OFTALMICO = (
        "Glicocorticóides Tópicos Simples Exceto Uso Oftálmico"
    )
    GLUCAGON = "Glucagon"
    GOMAS = "Gomas"
    GONADOTROFINAS_INCLUINDO_OUTROS_ESTIMULANTES_PARA_OVULACAO = (
        "Gonadotrofinas Incluindo Outros Estimulantes Para Ovulação"
    )
    HEMOSTATICOS_SISTEMICOS = "Hemostáticos Sistêmicos"
    HEPARINAS_FRACIONADAS = "Heparinas Fracionadas"
    HEPARINAS_NAO_FRACIONADA = "Heparinas Não Fracionada"
    HEPATOPROTETOR = "Hepatoprotetor"
    HEPATOPROTETORES_E_LIPOTROPICOS = "Hepatoprotetores e Lipotrópicos"
    HIPNOTICOS_E_SEDATIVOS_HERBACEOS = "Hipnóticos e Sedativos Herbáceos"
    HIPNOTICOS_E_SEDATIVOS_NAO_BARBITURICOS_PUROS = (
        "Hipnóticos E Sedativos Não Barbitúricos Puros"
    )
    HOMEOPATIA = "Homeopatia"
    HORMONIOS_PARATIREOIDEANOS_E_ANALOGOS = "Homônios Paratireoideanos E Análogos"
    HORMONIOS_ANTIANDROGENICOS_CITOSTATICOS = "Hormônios Antiandrogênicos Citostáticos"
    HORMONIOS_ANTICRESCIMENTO = "Hormônios Anticrescimento"
    HORMONIOS_ANTIDIURETICOS = "Hormônios Antidiuréticos"
    HORMONIOS_ANTIESTROGENEOS_CITOSTATICOS = "Hormônios Antiestrogêneos Citostáticos"
    HORMONIOS_CONTRACEPTIVOS_MONOFASICOS_COM_ESTROGENIOS_MENOR_50MCG = (
        "Hormônios Contraceptivos Monofásicos com Estrogênios <50mcg (menor que 50mcg)"
    )
    HORMONIOS_CONTRACEPTIVOS_MONOFASICOS_COM_ESTROGENIOS_MAIOR_IGUAL_50MCG = (
        "Hormônios Contraceptivos Monofásicos Com Estrogênios >=50mcg"
    )
    HORMONIOS_DE_LIBERACAO_ANTIGONADOTROFINA = (
        "Hormônios De Liberação Antigonadotrofina"
    )
    HORMONIOS_DO_CRESCIMENTO = "Hormônios Do Crescimento"
    HORMONIOS_ESTROGENEOS_CITOSTATICOS = "Hormônios Estrogêneos Citostáticos"
    HORMONIOS_LIBERADORES_DE_GONADOTROFINA = "Hormônios Liberadores De Gonadotrofina"
    HORMONIOS_PROGESTOGENEOS_CITOSTATICOS = "Hormônios Progestogêneos Citostáticos"
    INIBIDOR_PARP_ANTINEOPLASICO = "Inibidor Parp Antineoplásico"
    IMUNOGLOBULINA_HEPATITE = "Imunoglobulina Hepatite"
    IMUNOGLOBULINA_TETANICA = "Imunoglobulina Tetânica"
    IMUNOGLOBULINAS_POLIVALENTES_INTRAMUSCULARES = (
        "Imunoglobulinas Polivalentes Intramusculares"
    )
    IMUNOGLOBULINAS_POLIVALENTES_INTRAVENOSAS = (
        "Imunoglobulinas Polivalentes Intravenosas"
    )
    INDUTORES_DO_PARTO_INCLUINDO_OXITOCINAS = "Indutores Do Parto Incluindo Oxitocinas"
    INIBIDORES_DA_TRANSCRIPTASE_REVERSA_NAO_NUCLEOSIDEOS = (
        "Inibdores Da Transcriptase Reversa Não Nucleosídeos"
    )
    INIBIDOR_DA_ALFA_REDUTASE = "Inibidor da Alfa-Redutase"
    INIBIDOR_DA_HIPERPLASIA_PROSTATICA_BENIGNA_HPB = (
        "Inibidor da Hiperplasia Prostática Benigna (Hpb)"
    )
    INIBIDORES_DA_AGREGACAO_PLAQUETARIA_ANTAGONISTAS_DOS_RECEPTORES_DA_ADENOSINA_DIFOSFATO = "Inibidores da Agregação Plaquetária, Antagonistas dos Receptores da Adenosina Difosfato"
    INIBIDORES_DA_AGREGACAO_PLAQUETARIA_ANTAGONISTAS_GLICOPROTEINA_IIB_IIIA = (
        "Inibidores Da Agregação Plaquetária, Antagonistas Glicoproteína IIb/IIIa"
    )
    INIBIDORES_DA_AGREGACAO_PLAQUETARIA_CICLO_OXIGENASE_INIBIDORES = (
        "Inibidores Da Agregação Plaquetária, Ciclo-Oxigenase Inibidores"
    )
    INIBIDORES_DA_AGREGACAO_PLAQUETARIA_REANCADORES_DO_AMP_CICLICO = (
        "Inibidores da Agregação Plaquetária, Realçadores do AMP Cíclico"
    )
    INIBIDORES_DA_BOMBA_ACIDA = "Inibidores da Bomba Ácida"
    INIBIDORES_DA_BOMBA_DE_PROTONS = "Inibidores da Bomba de Prótons"
    INIBIDORES_DA_ECA_ASSOCIADOS_A_ANTAGONISTAS_DO_CALCIO_C8 = (
        "Inibidores da ECA Associados a Antagonistas do Cálcio (C8)"
    )
    INIBIDORES_DA_ECA_ASSOCIADOS_A_ANTIHIPERTENSIVOS_C2_E_OU_DIURETICOS_C3 = (
        "Inibidores Da Eca Associados A Anti-Hipertensivos (C2) E/Ou Diuréticos (C3)"
    )
    INIBIDORES_DA_ECA_PUROS = "Inibidores Da Eca Puros"
    INIBIDORES_DA_INTERLEUCINA = "Inibidores Da Interleucina"
    INIBIDORES_DA_MOTILIDADE = "Inibidores da Motilidade"
    LAGRIMAS_ARTIFICIAIS_E_LUBRIFICANTES_OFTALMOLOGICOS = (
        "Lágrimas Artificiais e Lubrificantes Oftalmológicos"
    )
    LAXANTE = "Laxante"
    LAXANTES_ENEMAS = "Laxantes Enemas"
    LAXANTES_ESTIMULANTES = "Laxantes Estimulantes"
    LAXANTES_INCREMENTADORES_DO_BOLO_FECAL = "Laxantes Incrementadores do Bolo Fecal"
    LAXANTES_OSMOTICOS = "Laxantes Osmóticos"
    LAXANTES_OSMOTICOS_COM_ELETROLITOS = "Laxantes Osmóticos Com Eletrólitos"
    LAXANTES_SUAVIZADORES_E_EMOLIENTES_FECAIS = (
        "Laxantes Suavizadores e Emolientes Fecais"
    )
    MACROLIDEOS_E_SIMILARES = "Macrolideos E Similares"
    MIDRIATICOS_E_CICLOPLEGICOS = "Midriáticos E Cicloplégicos"
    MODELADORES_SENSOMOTORES_GASTROINTESTINAIS = (
        "Modeladores Sensomotores Gastrointestinais"
    )
    MODULADORES_DO_REGULADOR_TRANSMEMBRANA_DA_FIBROSE_CISTICA_CFTR = (
        "Moduladores Do Regulador Transmembrana Da Fibrose Cística (Cftr)"
    )
    MODULADORES_SELETIVOS_DO_RECEPTOR_DE_ESTROGENIO = (
        "Moduladores Seletivos Do Receptor De Estrogênio"
    )
    MONOBACTAMICOS = "Monobactâmicos"
    NEUROTONICOS_E_OUTROS = "Neurotônicos E Outros"
    NITRITOS_E_NITRATOS = "Nitritos e Nitratos"
    NOOTROPICOS = "Nootrópicos"
    OREXIGENOS = "Orexígenos"
    OUTRAS_ASSOCIACOES_COM_COMPLEXO_B = "Outras Associações com Complexo B"
    OUTRAS_ASSOCIACOES_DE_ANTIDIABETICOS_COM_INIBIDORES_DPP_IV = (
        "Outras Associações de Antidiabéticos com Inibidores DPP-IV"
    )
    OUTRAS_ASSOCIACOES_DE_ANTI_HIPERTENSIVOS_COM_DIURETICOS = (
        "Outras Associações De Anti-Hipertensivos Com Diuréticos"
    )
    OUTRAS_ASSOCIACOES_DE_CORTICOSTEROIDES = "Outras Associações De Corticosteróides"
    OUTRAS_ASSOCIACOES_DE_VITAMINA_B1 = "Outras Associações de Vitamina B1"
    OUTRAS_DROGAS_PARA_CONSTIPACAO = "Outras Drogas para Constipação"
    OUTRAS_FRACOES_DO_SANGUE = "Outras Frações do Sangue"
    OUTRAS_IMUNOGLOBULINAS_ANTIVIRAIS = "Outras Imunoglobulinas Antivirais"
    OUTRAS_IMUNOGLOBULINAS_ESPECIFICAS = "Outras Imunoglobulinass Específica"
    OUTRAS_INSULINAS_HUMANAS = "Outras Insulinas Humanas"
    OUTRAS_PREPARACOES_DERMATOLOGICAS = "Outras Preparações Dermatológicas"
    OUTRAS_PREPARACOES_PARA_PROBLEMAS_ESTOMACAIS = (
        "Outras Preparações para Problemas Estomacais"
    )
    OUTRAS_PREPARACOES_TERAPEUTICAS = "Outras Preparações Terapêuticas"
    OUTRAS_PREPARACOES_TOPICAS_NASAIS = "Outras Preparações Tópicas Nasais"
    OUTRAS_SOLUCOES_DE_AMINOACIDOS = "Outras Soluções De Aminoácidos"
    OUTRAS_SOLUCOES_DE_INFUSAO = "Outras Soluções de Infusão"
    OUTRAS_SOLUCOES_DE_IRRIGACAO = "Outras Soluções de Irrigação"
    OUTRAS_SOLUCOES_ELETROLITICAS = "Outras Soluções Eletrolíticas"
    OUTRAS_SOLUCOES_INJETAVEIS_OU_ADITIVOS_PARA_INFUSAO_MENOR_100ML = (
        "Outras Soluções Injetáves Ou Aditivos Para Infusão (<100ml)"
    )
    PASTAS_DE_DENTE = "Pastas de Dente"
    PENICILINAS_DE_PEQUENO_E_MEDIO_ESPECTROS_PUROS = (
        "Penicilinas De Pequeno E Médio Espectros Puras"
    )
    PENICILINAS_INJETAVEIS_DE_AMPLO_ESPECTRO = (
        "Penicilinas Injetaveis De Amplo Espectro"
    )
    PENICILINAS_ORAIS_DE_AMPLO_ESPECTRO = "Penicilinas Orais de Amplo Espectro"
    POLIMIXINAS = "Polimixinas"
    POLIVITAMINICOS_COM_MINERAIS_GERIATRICO = "Polivitamínicos com Minerais, Geriátrico"
    POLIVITAMINICOS_COM_MINERAIS_PEDIATRICOS = (
        "Polivitamínicos com Minerais, Pediátricos"
    )
    POLIVITAMINICOS_COM_MINERAIS_PRENATAIS = "Polivitamínicos com Minerais, Prenatais"
    POLIVITAMINICOS_SEM_MINERAIS_OUTROS = "Polivitamínicos Sem Minerais, Outros"
    PREPARACAO_DE_ORIGEM_HERBACEA_PROMOTORA_DA_DEFESA_ORGANICA = (
        "Preparação de Origem Herbácea Promotora da Defesa Orgânica Contra Infecções"
    )
    PREPARACOES_ALTERNATIVAS_PARA_TERAPIA_ONCOLOGICA = (
        "Preparações Alternativas Para Terapia Oncológica"
    )
    PREPARACOES_ANTIATEROMATAS_DE_ORIGEM_NATURAL = (
        "Preparações Antiateromatas de Origem Natural"
    )
    PREPARACOES_ANTIGLAUCOMAS_E_MIOTICAS_SISTEMICAS = (
        "Preparações Antiglaucomas E Mióticas Sistêmicas"
    )
    PREPARACOES_ANTIGLAUCOMAS_E_MIOTICAS_TOPICAS = (
        "Preparações Antiglaucomas E Mióticas Tópicas"
    )
    PREPARACOES_ANTIOBESIDADE_EXCETO_OS_DIETETICOS = (
        "Preparações Antiobesidade, Exceto Os Dietéticos"
    )
    PREPARACOES_ANTITIREOIDEANOS = "Preparações Antitireoideanos"
    PREPARACOES_BUCAIS_CONTENDO_FLUOR = "Preparações Bucais contendo Fluor"
    PREPARACOES_CONTRACEPTIVAS_BIFASICAS = "Preparações Contraceptivas Bifásicas"
    PREPARACOES_CONTRACEPTIVAS_TRIFASICAS = "Preparações Contraceptivas Trifásicas"
    PREPARACOES_ORAIS_COM_PROGESTAGENIOS_SOMENTE = (
        "Preparações Orais com Progestagênios Somente"
    )
    QUINOLONAS_URINARIOS = "Quinolonas Urinários"
    RADIOFARMACOS = "Radiofármacos"
    REGULADOR_INTESTINAL = "Regulador Intestinal"
    REGULADORES_DE_GORDURA_COM_OUTROS_REGULADORES_DE_GORDURA = (
        "Reguladores De Gordura Em Combinação Com Outros Reguladores De Gordura"
    )
    REIDRATANTE_ORAL = "Reidratante Oral"
    RELAXANTE_MUSCULAR = "Relaxante Muscular"
    RELAXANTE_MUSCULAR_DE_ACAO_CENTRAL = "Relaxante Muscular De Ação Central"
    RELAXANTE_MUSCULAR_DE_ACAO_PERIFERICA = "Relaxante Muscular De Ação Periférica"
    REPOSITOR_HORMONAL_FITOTERAPICO = "Repositor Hormonal Fitoterápico"
    REPOSITORES_ORAIS_ELETROLITICOS = "Repositores Orais Electrolíticos"
    RIFAMPICINAS_E_RIFAMICINAS = "Rifampicinas E Rifamicinas"
    SAL_MINERAL = "Sal Mineral"
    SANGUE_E_FRACOES_DO_PLASMA = "Sangue e Frações do Plasma"
    SOLUCOES_AMINOACIDAS_PADRAO = "Soluções Aminoácidas Padrão"
    SOLUCOES_AMINOACIDAS_PARA_HEPATOPATAS = "Soluções Aminoácidas Para Hepatopatas"
    SOLUCOES_AMINOACIDAS_PARA_NEFROPATAS = "Soluções Aminoácidas Para Nefropatas"
    SOLUCOES_CALORICAS_MENOR_100ML = "Soluções Calóricas <100ml"
    SOLUCOES_CARBOIDRATOS_MENOR_10 = "Soluções Carbohidratos <10%"
    SOLUCOES_DE_AMINOACIDOS_PEDIATRICAS = "Soluções De Aminoácidos Pediátricas"
    SOLUCOES_DE_CARBOIDRATOS_MAIOR_10 = "Soluções De Carbohidratos >10%"
    SOLUCOES_DE_CLORETO_SODIO = "Soluções de Cloreto Sódio"
    SOLUCOES_DE_CLORETO_SODIO_COM_CARBOIDRATO = (
        "Soluções de Cloreto Sódio Com Carboidrato"
    )
    SOLUCOES_DE_HEMOFILTRACAO = "Soluções De Hemofiltração"
    SOLUCOES_DE_IRRIGACAO_AGUA = "Soluções de Irrigação - Água"
    SOLUCOES_DE_IRRIGACAO_GLICINA = "Soluções de Irrigação - Glicina"
    SOLUCOES_DE_PROTEINAS_MAIOR_5 = "Soluções De Proteinas >5%"
    SOLUCOES_DE_RINGER_E_RINGER_LACTATO = "Soluções De Ringer E Ringer Lactato"
    SOLUCOES_ELETROLITICAS_MENOR_IGUAL_20ML = "Soluções Electrolíticas (<=20ml)"
    SOLUCOES_ELETROLITICAS_MAIOR_20ML_MENOR_100ML = (
        "Soluções Eletrolíticas (>20mL e <100mL)"
    )
    SOLUCOES_ELETROLITICAS_1_1 = "Soluções Eletrolíticas 1/1"
    SOLUCOES_ELETROLITICAS_1_2 = "Soluções Eletrolíticas 1/2"
    TERAPIA_ANTIVARICOSA_TOPICA = "Terapia Antivaricosa Tópica"
    TERAPIA_CORONARIA_EXCLUINDO_ANTAGONISTAS_DO_CALCIO_E_NITRITOS = (
        "Terapia Coronaria Excluindo Antagonistas Do Cálcio E Nitritos"
    )
    TERAPIA_DOS_CALCULOS_BILIARES = "Terapia Dos Cálculos Biliares"
    TETRACICLINAS_E_ASSOCIACOES = "Tetraciclinas E Associações"
    TODAS_AS_OUTRAS_VACINAS_VIRAIS = "Todas As Outras Vacinas Virais"
    TODAS_OUTRAS_PREPARACOES_CARDIACAS = "Todas Outras Preparações Cardíacas"
    TODAS_OUTRAS_VITAMINAS = "Todas Outras Vitaminas"
    TODOS_AS_OUTRAS_PREPARACOES_ANTIENXAQUECOSAS = (
        "Todos As Outras Preparações Antienxaquecosas"
    )
    TODOS_OS_OUTROS_ANTIBIOTICOS = "Todos Os Outros Antibióticos"
    TODOS_OS_OUTROS_ANTINEOPLASICOS = "Todos Os Outros Antineoplásicos"
    TODOS_OS_OUTROS_ANTIULCEROSOS = "Todos os Outros Antiulcerosos"
    TODOS_OS_OUTROS_ESTOMATOLOGICOS = "Todos os Outros Estomatológicos"
    TODOS_OS_OUTROS_FARMACOS_COM_ACAO_MUSCULO_ESQUELETICA = (
        "Todos Os Outros Fármacos Com Ação Músculo-Esquelética"
    )
    TODOS_OS_OUTROS_PRODUTOS_ANTI_ASMATICOS_E_DPOC_SISTEMICOS = (
        "Todos Os Outros Produtos Anti-Asmáticos e DPOC, Sistêmicos"
    )
    TODOS_OS_OUTROS_PRODUTOS_ANTIALZHEIMER = "Todos Os Outros Produtos Antialzheimer"
    TODOS_OS_OUTROS_PRODUTOS_PARA_O_SISTEMA_NERVOSO_CENTRAL = (
        "Todos Os Outros Produtos Para O Sistema Nervoso Central"
    )
    TODOS_OS_OUTROS_PRODUTOS_PARA_O_SISTEMA_RESPIRATORIO = (
        "Todos Os Outros Produtos Para O Sistema Respiratório"
    )
    TODOS_OS_OUTROS_PRODUTOS_TERAPEUTICOS = "Todos Os Outros Produtos Terapêuticos"
    TODOS_OS_OUTROS_PRODUTOS_UROLOGICOS = "Todos Os Outros Produtos Urológicos"
    UNGUENTOS_PERCUTANEOS_E_OUTROS_INALANTES = (
        "Unguentos Percutâneos E Outros Inalantes"
    )
    VACINA_CONTRA_HPV_PAPILOMAVIRUS_HUMANO = "Vacina Contra HPV (Papilomavírus Humano)"
    VACINA_CONTRA_TUBERCULOSE = "Vacina Contra Tuberculose"
    VACINA_CONTRA_VARICELLA = "Vacina Contra Varicella"
    VACINA_PARA_FEBRE_TIFOIDE_E_PARATIFOIDE = "Vacina Para Febre Tifóide E Paratifóide"
    VACINA_PARA_GRIPE_INFLUENZA = "Vacina Para Gripe (Influenza)"
    VACINA_PARA_HEPATITE = "Vacina Para Hepatite"
    VACINA_PARA_PNEUMONIA = "Vacina Para Pneumonia"
    VACINA_PARA_TETANO = "Vacina Para Tétano"
    VACINAS_CONTRA_O_ROTAVIRUS = "Vacinas Contra O Rotavírus"
    VACINAS_HAEMOPHILUS_B = "Vacinas Haemophilus B"
    VACINAS_MENINGOCOCICAS = "Vacinas Meningocócicas"
    VACINAS_PARA_O_CORONAVIRUS = "Vacinas Para o Coronavírus"
    VASODILATADORES = "Vasodilatadores"
    VASOPROTETORES_SISTEMICOS = "Vasoprotetores Sistêmicos"
    VASOTERAPEUTICOS_CEREBRAIS_E_PERIFERICOS_EXC_ANTAGONISTAS_DE_CALCIO = "Vasoterapêuticos Cerebrais E Periféricos, Excluindo Antoagonistas De Cálcio Com Ação Cerebral"
    VITAMINA_A_PURA = "Vitamina A Pura"
    VITAMINA_B1_PURA = "Vitamina B1 Pura"
    VITAMINA_B12_PURA = "Vitamina B12 Pura"
    VITAMINA_B6_PIRIDOXINA_PURA = "Vitamina B6 (Piridoxina) Pura"


# obtido por meio de spark.read.table("production.refined.produtos_refined").select(F.explode("especialidades").alias("especialidade")).distinct()
class Especialidades(ExtendedEnum):
    ALERGIA_E_IMUNOLOGIA = "Alergia e Imunologia"
    ALERGIA_E_IMUNOPATOLOGIA = "Alergia e Imunopatologia"
    ANESTESIOLOGIA = "Anestesiologia"
    ANGIOLOGIA = "Angiologia"
    ANGIOLOGIA_E_CIRURGIA_VASCULAR = "Angiologia e Cirurgia Vascular"
    BRONCOESOFAGOLOGIA = "Broncoesofagologia"
    CANCEROLOGIA = "Cancerologia"
    CANCEROLOGIA_CLINICA = "Cancerologia clínica"
    CANCEROLOGIA_PEDIATRICA = "Cancerologia pediátrica"
    CARDIOLOGIA = "Cardiologia"
    CIRURGIA_GINECOLOGICA = "Cirurgia Ginicológica"
    CIRURGIA_CARDIOVASCULAR = "Cirurgia cardiovascular"
    CIRURGIA_DIGESTIVA = "Cirurgia digestiva"
    CIRURGIA_DO_APARELHO_DIGESTIVO = "Cirurgia do aparelho digestivo"
    CIRURGIA_GASTROENTEROLOGICA = "Cirurgia gastroenterológica"
    CIRURGIA_GERAL = "Cirurgia geral"
    CIRURGIA_OCULAR_LEVE = "Cirurgia ocular leve"
    CIRURGIA_ONCOLOGICA = "Cirurgia oncológica"
    CIRURGIA_PEDIATRICA = "Cirurgia pediátrica"
    CIRURGIA_VASCULAR = "Cirurgia vascular"
    CIRURGIA_VASCULAR_PERIFERICA = "Cirurgia vascular periférica"
    CLINICA_MEDICA = "Clínica Médica"
    COLOPROCTOLOGIA = "Coloproctologia"
    DERMATOLOGIA = "Dermatologia"
    DIAGNOSTICO_POR_IMAGEM = "Diagnóstico por imagem"
    ENDOCRINOLOGIA = "Endocrinologia"
    ENDOSCOPIA = "Endoscopia"
    FISIATRIA = "Fisiatria"
    FISIOTERAPIA = "Fisioterapia"
    GASTROENTEROLOGIA = "Gastroenterologia"
    GENETICISTA_CLINICO = "Geneticista Clínico"
    GENETICA_CLINICA = "Genética clínica"
    GENETICA_LABORATORIAL = "Genética laboratorial"
    GENETICA_MEDICA = "Genética médica"
    GERIATRIA = "Geriatria"
    GINECOLOGIA = "Ginecologia"
    HEMATOLOGIA = "Hematologia"
    HEMOTERAPIA = "Hemoterapia"
    HEPATOLOGIA = "Hepatologia"
    HOMEOPATIA = "Homeopatia"
    IMUNOLOGIA_CLINICA = "Imunologia clínica"
    INFECTOLOGIA = "Infectologia"
    MEDICINA_CLINICA = "Medicina clínica"
    MEDICINA_DE_FAMILIA_E_COMUNIDADE = "Medicina de família e comunidade"
    MEDICINA_GERAL_COMUNITARIA = "Medicina geral comunitária"
    MEDICINA_INTENSIVA = "Medicina intensiva"
    MEDICINA_LABORATORIAL = "Medicina laboratorial"
    METABOLOGIA = "Metabologia"
    MEDICOS_DE_URGENCIA = "Médicos de urgência"
    NEFROLOGIA = "Nefrologia"
    NEUROCIRURGIA = "Neurocirurgia"
    NEUROLOGIA = "Neurologia"
    NEUROPEDIATRIA = "Neuropediatria"
    NEUROPSIQUIATRIA = "Neuropsiquiatria"
    NUTRICIONISTA = "Nutricionista"
    NUTRICAO_PARENTERAL_E_ENTERAL = "Nutrição parenteral e enteral"
    NUTROLOGIA = "Nutrologia"
    OBSTETRICIA = "Obstetrícia"
    ODONTOLOGIA = "Odontologia"
    OFTALMOLOGIA = "Oftalmologia"
    ONCOLOGIA = "Oncologia"
    ORTOPEDIA = "Ortopedia"
    ORTOPEDIA_E_TRAUMATOLOGIA = "Ortopedia e traumatologia"
    OTORRINOLARINGOLOGIA = "Otorrinolaringologia"
    PEDIATRIA = "Pediatria"
    PNEUMOLOGIA = "Pneumologia"
    PODOLOGIA = "Podologia"
    PROCTOLOGIA = "Proctologia"
    PSICANALISE = "Psicanálise"
    PSICOLOGIA = "Psicologia"
    PSICOPEDAGOGIA = "Psicopedagogia"
    PSIQUIATRIA = "Psiquiatria"
    PSIQUIATRIA_INFANTIL = "Psiquiatria infantil"
    RADIODIAGNOSTICO = "Radiodiagnóstico"
    RADIOLOGIA = "Radiologia"
    RADIOTERAPIA = "Radioterapia"
    REPRODUCAO_HUMANA = "Reprodução Humana"
    REUMATOLOGIA = "Reumatologia"
    SEXOLOGIA = "Sexologia"
    TERAPIA_OCUPACIONAL = "Terapia ocupacional"
    UROLOGIA = "Urologia"
    VETERINARIA = "Veterinária"
    NAO_SE_APLICA = "Não se aplica"


# enum alinhado às formas farmacêuticas consolidadas a partir do catálogo interno de vias de administração(ENUMs_01/2026)
class ViaAdministracao(ExtendedEnum):
    BUCAL = "Bucal"
    CAPILAR = "Capilar"
    DERMATOLOGICA = "Dermatológica"
    EPIDURAL = "Epidural"
    INALATORIA = "Inalatória"
    INALATORIA_POR_VIA_NASAL = "Inalatória por Via Nasal"
    INALATORIA_POR_VIA_ORAL = "Inalatória por Via Oral"
    INTRA_ARTERIAL = "Intra-arterial"
    INTRA_ARTICULAR = "Intra-articular"
    INTRADERMICA = "Intradérmica"
    INTRAMUSCULAR = "Intramuscular"
    INTRATECAL = "Intratecal"
    INTRAUTERINA = "Intrauterina"
    INTRAVENOSA = "Intravenosa"
    INTRAVITREA = "Intravítrea"
    IRRIGACAO = "Irrigação"
    NASAL = "Nasal"
    OFTALMICA = "Oftálmica"
    ORAL = "Oral"
    OTOLOGICA = "Otológica"
    RETAL = "Retal"
    SUBCUTANEA = "Subcutânea"
    SUBLINGUAL = "Sublingual"
    TRANSDERMICA = "Transdérmica"
    URETRAL = "Uretral"
    VAGINAL = "Vaginal"
    NAO_SE_APLICA = "Não se aplica"
