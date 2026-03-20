from maggulake.enums.extended_enum import ExtendedEnum


class RFMCategories(ExtendedEnum):
    """All the possible categories for the RFM analysis.
    Abra o arquivo misc/rfm/rfm-view.html no navegador para visualizar a escala
    de categorias do RFM.
    """

    CHAMPIONS = "Champions"
    LOYAL_CUSTOMERS = "Loyal Customers"
    POTENTIAL_LOYALIST = "Potential Loyalist"
    NEW_CUSTOMERS = "New Customers"
    PROMISING = "Promising"
    NEEDING_ATTENTION = "Customers Needing Attention"
    ABOUT_TO_SLEEP = "About to Sleep"
    AT_RISK = "At Risk"
    CANT_LOSE = "Can't Lose Them"
    HIBERNATING = "Hibernating"
    LOST = "Lost"
    GENERAL = "General"
