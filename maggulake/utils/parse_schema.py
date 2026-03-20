from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def parse_schema(schema_str: str) -> StructType:
    """Convert a string schema definition to StructType."""
    TYPE_MAPPING = {
        "STRING": StringType(),
        "BOOLEAN": BooleanType(),
        "BIGINT": LongType(),
        "TIMESTAMP": TimestampType(),
    }

    fields = []
    for line in schema_str.strip().split(","):
        parts = line.strip().split()
        field_name = parts[0]
        field_type = TYPE_MAPPING[parts[1]]
        nullable = "NOT NULL" not in line  # Assume nullable if NOT NULL isn't present
        fields.append(StructField(field_name, field_type, nullable))

    return StructType(fields)
