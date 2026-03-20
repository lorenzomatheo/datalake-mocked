from .extended_enum import ExtendedEnum


class TiposReceita(ExtendedEnum):
    A1_AMARELA = "a1 amarela"
    A2_AMARELA = "a2 amarela"
    A3_AMARELA = "a3 amarela"
    B1_AZUL = "b1 azul"
    B2_AZUL = "b2 azul"
    BRANCA_2_VIAS = "branca 2 vias"
    BRANCA_COMUM = "branca comum"
    C1_BRANCA_2_VIAS = "c1 branca 2 vias"
    C2_BRANCA = "c2 branca"
    C3_BRANCA = "c3 branca"
    C4_BRANCA = "c4 branca"
    C5_BRANCA_2_VIAS = "c5 branca 2 vias"
    D1_BRANCA = "d1 branca"
    ISENTO = "isento de prescrição médica"
    CONTROLE_ESPECIAL = "receituário de controle especial"
    RECEITUARIO_SIMPLES = "receituário simples"
    DESCONHECIDO = "desconhecido"


class DescricaoTipoReceita(ExtendedEnum):
    A1_AMARELA_HOSPITAL = "A1 Amarela (Dispensação Sob Prescrição Médica Restrito a Hospitais - Este medicamento pode causar Dependência Física ou Psíquica)"
    A1_AMARELA = "A1 Amarela (Venda sob Prescrição Médica - Atenção: Pode Causar Dependência Física ou Psíquica)"
    A3_AMARELA = "A3 Amarela (Venda sob Prescrição Médica - Atenção: Pode Causar Dependência Física ou Psíquica)"

    B1_AZUL = "B1 Azul (Venda sob Prescrição Médica - O Abuso deste medicamento pode causar dependência)"
    B2_AZUL = "B2 Azul (Venda sob Prescrição Médica - O Abuso deste medicamento pode causar dependência)"

    C3_BRANCA = "C3 Branca (Proibido para mulheres gravidas ou com chance de engravidar - Uso sob prescrição médica sujeito a retenção de receita)"
    C1_BRANCA_2_VIAS_HOSPITAL = (
        "C1 Branca 2 Vias (Dispensação Sob Prescrição Médica Restrito a Hospitais)"
    )
    C1_BRANCA_2_VIAS = "C1 Branca 2 Vias (Venda sob Prescrição Médica - Só Pode ser Vendido com Retenção da Receita)"
    C2_BRANCA = "C2 Branca (Venda sob Prescrição Médica - Atenção: Risco para Mulheres Grávidas, Causa Graves Defeitos na Face, nas Orelhas, no Coração e no Sistema Nervoso do Feto)"
    C5_BRANCA_2_VIAS = "C5 Branca 2 vias (Venda Sob Prescrição Médica - Só Pode ser Vendido com Retenção da Receita)"

    BRANCA_2_VIAS_HOSPITAL = "Branca 2 Vias (Antibiótico - Dispensação Sob Prescrição Médica Restrito a Hospitais)"
    BRANCA_2_VIAS = "Branca 2 vias (Antibiótico - Venda Sob Prescrição Médica só pode ser vendido com Retenção da Receita)"
    BRANCA_COMUM = "branca comum"
    BRANCA_COMUM_VACINAS = "branca comum (vacinas - venda sob prescrição médica)"

    ISENTO = "isento de prescrição médica"
    RECEITUARIO_SIMPLES = "receituário simples"

    DESCONHECIDO = "desconhecido"
