import sys
from age_crash_analysis import age_crash_analysis
from make_model_analysis import make_model_analysis

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

    # 1. accident vs age
    # 2. Weighted: accident vs age
    age_crash_analysis(crash_df, people_crash_df)

    # 3. make injury
    # 4. model injury
    make_model_analysis(crash_df, vehicle_crash_df)




if __name__ == '__main__':
    main()
