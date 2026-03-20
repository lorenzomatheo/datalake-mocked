from maggulake.tables.base_table import BaseTable


class FarmarcasTable(BaseTable):
    table_name = "raw.farmarcas"
    force_schema = True

    def validate(self, new):
        stats = self.get_stats(new)
        display(stats)
