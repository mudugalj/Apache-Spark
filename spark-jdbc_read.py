from pyspark.sql import SparkSession

spark = SparkSession.\
    builder.\
    appName('Spark JDBC demo').\
    getOrCreate()


#dataframe_mysql = spark.read.format("jdbc").\
#    option("url", "jdbc:mysql://localhost/company").\
#    option("driver", "com.mysql.jdbc.Driver").\
#    option("dbtable", "employee").\
#    option("user", "root").\
#    option("password", "*******").load()

#dataframe_mysql.show()

jdbcHostname = "localhost"
jdbcDatabase = "company"
jdbcUsername = "root"
jdbcPassword = "******"
jdbcPort = 3306

jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.mysql.jdbc.Driver"
}

pushdown_query = "(select * from employee) emp_alias"

df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)

df.show()