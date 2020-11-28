# Request Log Parser
*Application to read gz compressed log file, normalise and structure the data and generate a json report.*

# Generate Fat Jar
```
sbt assembly
```

# Execution on a cluster
*Application can be executed on a cluster by using [spark submit](https://spark.apache.org/docs/latest/submitting-applications.html) with the path to your fat jar as a parameter.* 
```
# Example request
./bin/spark-submit \
  --class RequestLogParser \
  --master spark://xxx.xxx.xxx.xxx:7077 \
  --executor-memory 20G \
  --total-executor-cores 100 \
  /path/to/RequestLogParser-fatjar-1.0.jar \
  1000
```


# Running Tests
[ScalaTest](https://www.scalatest.org/user_guide/using_scalatest_with_sbt) testing framework used.

To run tests:

```
sbt test
```