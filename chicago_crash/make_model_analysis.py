from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType


def make_model_analysis(crash_df, vehicle_crash_df):
    for vehicle_category in ['MAKE', 'MODEL']:
        for vehicle_column in ['INJURIES_TOTAL', 'INJURIES_FATAL']:
            crash_df_tmp = crash_df.withColumn(vehicle_column, crash_df[vehicle_column].cast(IntegerType()))
            crash_df_int = crash_df_tmp.where(crash_df_tmp[vehicle_column] > 0)
            joined_df_total = crash_df_int.join(vehicle_crash_df, crash_df_int['RD_NO'] == vehicle_crash_df['RD_NO']).select(vehicle_category,
                                                                                                               vehicle_column, crash_df_int['RD_NO'])
            total_joined = joined_df_total.dropDuplicates([vehicle_category, 'RD_NO']).groupBy(vehicle_category).agg({vehicle_column: 'sum'})
            total_joined.sort(col("sum({})".format(vehicle_column)).desc()).show(10, False)

            total_count = total_joined.toPandas().sum()
            print(total_count)
