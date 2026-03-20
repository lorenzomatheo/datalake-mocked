from maggulake.tables.base_table import BaseTable


class AnvisaTable(BaseTable):
    table_name = "raw.anvisa"
    force_schema = True

    def validate(self, new):
        stats = self.get_stats(new)
        display(stats)
