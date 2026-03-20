from .extended_enum import ExtendedEnum


class TipoMedicamento(ExtendedEnum):
    GENERICO = "Medicamento Genérico - Lei 9.787/99"
    REFERENCIA = "Medicamento Referência"
    NOVO = "Medicamento Novo"
    SIMILAR = "Medicamento Similar"
    SIMILAR_INTERCAMBIAVEL = "Medicamento Similar Intercambiável"
    RADIOFARMACO = "Medicamento Radiofármaco"
    FITOTERAPICO = "Medicamento Fitoterápico"
    TERAPIA_AVANCADA = "Produto de Terapia Avançada"
    BIOLOGICO = "Medicamento Biológico"
    ESPECIFICO = "Medicamento Específico"
    DINAMIZADO = "Medicamento Dinamizado"
    BAIXO_RISCO = "Medicamento de Baixo Risco"
    GASES_MEDICINAIS = "Gases Medicinais"
