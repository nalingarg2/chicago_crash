from pyspark.sql.functions import col


def null_filter(word):
    if word == "UNKNOWN" or word == "null":
        return(word, 1)


def make_analysis(crash_df, vehicle_crash_df):
    crash_df.join(vehicle_crash_df, crash_df['RD_NO'] == vehicle_crash_df['RD_NO']).select('MAKE', 'INJURIES_FATAL'). \
        show(10, False)


def model_analysis(crash_df, vehicle_crash_df):
    joined_df = crash_df.join(vehicle_crash_df, crash_df['RD_NO'] == vehicle_crash_df['RD_NO']).select('MODEL',
                                                                                                       'INJURIES_TOTAL')
    joined_df.groupBy('MODEL').agg({'INJURIES_TOTAL': 'sum'}).sort(col("sum(INJURIES_TOTAL)").desc()).show(10, False)

    joined_df = crash_df.join(vehicle_crash_df, crash_df['RD_NO'] == vehicle_crash_df['RD_NO']).select('MODEL', 'INJURIES_FATAL')
    joined_df.groupBy('MODEL').agg({'INJURIES_FATAL':'sum'}).sort(col("sum(INJURIES_FATAL)").desc()).show(10, False)

    group_by_model = vehicle_crash_df.select('MODEL').where(vehicle_crash_df["MODEL"].isNotNull()).\
        rdd.map(lambda model: null_filter(model)).\
        reduceByKey(lambda a, b: a + b)
    print(group_by_model.count())

    total_count = vehicle_crash_df.select('MODEL').where(vehicle_crash_df["MODEL"].rdd.map(lambda model: null_filter(model)).\
        reduceByKey(lambda a, b: a + b)
    print(group_by_model.count())
