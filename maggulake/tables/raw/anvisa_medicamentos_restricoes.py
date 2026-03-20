from maggulake.tables.base_table import BaseTable


class AnvisaMedicamentosRestricoesTable(BaseTable):
    table_name = "raw.anvisa_medicamentos_restricoes"
    force_schema = True

    def validate(self, new):
        stats = self.get_stats(new)
        display(stats)
