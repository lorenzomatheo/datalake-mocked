        from pyspark.sql import functions as F
        from pyspark.sql import types as T

        from maggulake.utils.numbers import calculate_mode
        from maggulake.utils.strings import converte_array_para_string

mode_udf = F.udf(calculate_mode, T.IntegerType())

    array_to_string_udf = F.udf(converte_array_para_string, T.StringType())
