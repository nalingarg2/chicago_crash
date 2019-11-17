from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType


def null_filter(word):
    if word == "UNKNOWN" or word == "null":
        return(word, 1)


def make_analysis(crash_df, vehicle_crash_df):
    crash_df.join(vehicle_crash_df, crash_df['RD_NO'] == vehicle_crash_df['RD_NO']).select('MAKE', 'INJURIES_FATAL'). \
        show(10, False)


def model_analysis(crash_df, vehicle_crash_df):

    # total
    for vehicle_column in ['INJURIES_TOTAL', 'INJURIES_FATAL']:
        crash_df_tmp = crash_df.withColumn(vehicle_column, crash_df[vehicle_column].cast(IntegerType()))
        crash_df_int = crash_df_tmp.where(crash_df_tmp[vehicle_column] > 0)
        joined_df_total = crash_df_int.join(vehicle_crash_df, crash_df_int['RD_NO'] == vehicle_crash_df['RD_NO']).select('MODEL',
                                                                                                           vehicle_column, crash_df_int['RD_NO'])
        total_joined = joined_df_total.dropDuplicates(['MODEL', 'RD_NO']).groupBy('MODEL').agg({vehicle_column: 'sum'})
        total_joined.sort(col("sum({})".format(vehicle_column)).desc()).show(10, False)

    # Fatal
    # fatal_crash_df_tmp = crash_df.withColumn("INJURIES_FATAL", crash_df["INJURIES_FATAL"].cast(IntegerType()))
    # fatal_crash_df_int = fatal_crash_df_tmp.where(fatal_crash_df_tmp["INJURIES_FATAL"] > 0)
    # joined_df_fatal = fatal_crash_df_int.join(vehicle_crash_df, fatal_crash_df_int['RD_NO'] == vehicle_crash_df['RD_NO']).select('MODEL', 'INJURIES_FATAL')
    # joined_df_fatal.groupBy('MODEL').agg({'INJURIES_FATAL':'sum'}).sort(col("sum(INJURIES_FATAL)").desc()).show(10, False)
    #
    # group_by_model = vehicle_crash_df.select('MODEL').where(vehicle_crash_df["MODEL"].isNotNull()).\
    #     rdd.map(lambda model: null_filter(model)).\
    #     reduceByKey(lambda a, b: a + b)
    # print(group_by_model.count())
    #
    total_count = total_joined.toPandas().sum()
    print(total_count)
    # print(group_by_model.count())
