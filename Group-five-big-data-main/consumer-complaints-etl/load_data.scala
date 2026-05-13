// Load consumer complaints data in Spark shell
val complaintsDF = spark.read.option("header", true).option("inferSchema", true).csv("data/raw/Consumer_Complaints.csv")

// Show schema
complaintsDF.printSchema()

// Count records
val totalCount = complaintsDF.count()
println(s"Total complaints: $totalCount")

// Show sample data
complaintsDF.show(10)

// Group by state and count
val complaintsByState = complaintsDF.groupBy("State").count().orderBy(col("count").desc)
complaintsByState.show()

// Cache the dataframe for faster access
complaintsDF.cache()

println("Data loaded successfully! Check Spark UI at http://10.18.60.66:4040")
