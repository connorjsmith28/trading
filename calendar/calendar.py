# SCRIPT DOESN'T CURRENTLY WORK. I likely don't understand how pyspark is supposed to function
# This was borrowed from https://www.tackytech.blog/how-to-create-a-date-dimension-table-in-python-and-pyspark/

# load packages
from pyspark.sql.functions import *
from datetime import datetime, timedelta

# COMMAND ----------

# define boundaries
startdate = datetime.strptime('2000-01-01','%Y-%m-%d')
enddate   = (datetime.now() + timedelta(days=365 * 3)).replace(month=12, day=31)  # datetime.strptime('2023-10-01','%Y-%m-%d')

# COMMAND ----------

# define column names and its transformation rules on the Date column
column_rule_df = spark.createDataFrame([
    ("DateID",              "cast(date_format(date, 'yyyyMMdd') as int)"),     # 20230101
    ("Year",                "year(date)"),                                     # 2023
    ("Quarter",             "quarter(date)"),                                  # 1
    ("Month",               "month(date)"),                                    # 1
    ("Day",                 "day(date)"),                                      # 1
    ("Week",                "weekofyear(date)"),                               # 1
    ("QuarterNameLong",     "date_format(date, 'QQQQ')"),                      # 1st qaurter
    ("QuarterNameShort",    "date_format(date, 'QQQ')"),                       # Q1
    ("QuarterNumberString", "date_format(date, 'QQ')"),                        # 01
    ("MonthNameLong",       "date_format(date, 'MMMM')"),                      # January
    ("MonthNameShort",      "date_format(date, 'MMM')"),                       # Jan
    ("MonthNumberString",   "date_format(date, 'MM')"),                        # 01
    ("DayNumberString",     "date_format(date, 'dd')"),                        # 01
    ("WeekNameLong",        "concat('week', lpad(weekofyear(date), 2, '0'))"), # week 01
    ("WeekNameShort",       "concat('w', lpad(weekofyear(date), 2, '0'))"),    # w01
    ("WeekNumberString",    "lpad(weekofyear(date), 2, '0')"),                 # 01
    ("DayOfWeek",           "dayofweek(date)"),                                # 1
    ("YearMonthString",     "date_format(date, 'yyyy/MM')"),                   # 2023/01
    ("DayOfWeekNameLong",   "date_format(date, 'EEEE')"),                      # Sunday
    ("DayOfWeekNameShort",  "date_format(date, 'EEE')"),                       # Sun
    ("DayOfMonth",          "cast(date_format(date, 'd') as int)"),            # 1
    ("DayOfYear",           "cast(date_format(date, 'D') as int)"),            # 1
    ("MonthNameLong_GER",   "CASE WHEN month(date) = 1 THEN 'Januar' WHEN month(date) = 2 THEN 'Februar' WHEN month(date) = 3 THEN 'März' WHEN month(date) = 4 THEN 'April' WHEN month(date) = 5 THEN 'Mai' WHEN month(date) = 6 THEN 'Juni' WHEN month(date) = 7 THEN 'Juli' WHEN month(date) = 8 THEN 'August' WHEN month(date) = 9 THEN 'September' WHEN month(date) = 10 THEN 'Oktober' WHEN month(date) = 11 THEN 'November' WHEN month(date) = 12 THEN 'Dezember' ELSE '' END"), # Januar
    ("MonthNameShort_GER",  "CASE WHEN month(date) = 1 THEN 'Jan' WHEN month(date) = 2 THEN 'Feb' WHEN month(date) = 3 THEN 'Mär' WHEN month(date) = 4 THEN 'Apr' WHEN month(date) = 5 THEN 'Mai' WHEN month(date) = 6 THEN 'Jun' WHEN month(date) = 7 THEN 'Jul' WHEN month(date) = 8 THEN 'Aug' WHEN month(date) = 9 THEN 'Sep' WHEN month(date) = 10 THEN 'Okt' WHEN month(date) = 11 THEN 'Nov' WHEN month(date) = 12 THEN 'Dez' ELSE '' END"),                                    # Jan
    ("DayofWeekNameLong_GER", "CASE WHEN dayofweek(date) = 1 THEN 'Sonntag' WHEN dayofweek(date) = 2 THEN 'Montag' WHEN dayofweek(date) = 3 THEN 'Dienstag' WHEN dayofweek(date) = 4 THEN 'Mittwoch' WHEN dayofweek(date) = 5 THEN 'Donnerstag' WHEN dayofweek(date) = 6 THEN 'Freitag' WHEN dayofweek(date) = 7 THEN 'Samstag' ELSE '' END")                                                                                                                                          # Sonntag
], ["new_column_name", "expression"])


# COMMAND ----------

# explode dates between the defined boundaries into one column
start = int(startdate.timestamp())
stop  = int(enddate.timestamp())
df = spark.range(start, stop, 60*60*24).select(col("id").cast("timestamp").cast("date").alias("Date"))
# display(df)

# COMMAND ----------

# this loops over all rules defined in column_rule_df adding the new columns
for row in column_rule_df.collect():
    new_column_name = row["new_column_name"]
    expression = expr(row["expression"])
    df = df.withColumn(new_column_name, expression)
# display(df)

# COMMAND ----------

# display(df.withColumn("Playground", expr("date_format(date, 'yyyyMMDD')")))
