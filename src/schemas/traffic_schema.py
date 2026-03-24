from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    ArrayType
)

TRAFFIC_JSON_SCHEMA = StructType([
    StructField("flowSegmentData", StructType([
        StructField("frc", StringType(), True),
        StructField("currentSpeed", IntegerType(), True),
        StructField("freeFlowSpeed", IntegerType(), True),
        StructField("currentTravelTime", IntegerType(), True),
        StructField("freeFlowTravelTime", IntegerType(), True),
        StructField("confidence", DoubleType(), True),
        StructField("roadClosure", BooleanType(), True),
        StructField("coordinates", StructType([
            StructField("coordinate", ArrayType(
                StructType([
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True)
                ])
            ), True)
        ]), True)
    ]), True)
])
