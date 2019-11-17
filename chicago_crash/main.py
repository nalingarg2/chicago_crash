import sys
import matplotlib.pyplot as plt
from visualization import visulation_df
from age_crash_analysis import group_df

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as f
    from pyspark.sql.types import IntegerType
    print("Successfully imported Spark Modules")

except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)


def main():
    spark = SparkSession.\
        builder.\
        appName(" Chicago Chrash Analysis").\
        getOrCreate()
    people_crash_df = spark.read.csv(path="dataset/Traffic_Crashes_-_People.csv",
                                     header=True)
    crash_df = spark.read.csv(path="dataset/Traffic_Crashes_-_Crashes.csv",
                                     header=True)
    vehicle_crash_df = spark.read.csv(path="dataset/Traffic_Crashes_-_Vehicles.csv",
                                     header=True)
    # print(people_crash_df.count())
    # print(crash_df.count())
    # print(vehicle_crash_df.count())
    # print(crash_df.join(vehicle_crash_df, crash_df['RD_NO'] == vehicle_crash_df['RD_NO']).count())

    # model vs fatal
    # crash_df.join(vehicle_crash_df, crash_df['RD_NO'] == vehicle_crash_df['RD_NO']).select('MODEL', 'INJURIES_FATAL',
    #                                                                                        'INJURIES_TOTAL').show()

    # accident vs age
    age_vs_accident = crash_df.join(people_crash_df, crash_df['RD_NO'] == people_crash_df['RD_NO']).select('AGE',
                                                                                           'INJURIES_TOTAL')
    # age_vs_accident.show()

    temp_df = age_vs_accident.where(age_vs_accident['INJURIES_TOTAL'] > 0).where(age_vs_accident["AGE"].isNotNull())

    int1_temp_df = temp_df.withColumn("AGE", temp_df["AGE"].cast(IntegerType()))
    int2_temp_df = int1_temp_df.withColumn("INJURIES_TOTAL", int1_temp_df["INJURIES_TOTAL"].cast(IntegerType()))
    # print(temp_df.rdd.collect(10))

    counts = int2_temp_df.rdd.map(lambda word: group_df(word)) \
        .reduceByKey(lambda a, b: a + b)
    print(counts.collect())
    df = counts.toDF()
    plot_test = df.toPandas().sort_values("_1", ascending=True)

    # plot_test = age_vs_accident.toPandas().sort_values(by = "AGE")
    visulation_df(df=plot_test, x_axis="_1", y_axis="_2",
                  x_label="Age", y_label="count",
                  title="Age Vs Accidents", sorting='x_axis')

    # data = [go.Histogram(x=age_vs_accident.toPandas()['AGE'],
    #                      y=age_vs_accident.toPandas()['INJURIES_TOTAL'])]
    # py.iplot(data, filename="plot_age_vs_accident")


if __name__ == '__main__':
    main()
