#### This script and conf file should be used to lighten Spark Logs 
#### Use in case of Need

```
spark-submit --deploy-mode client --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" --files "log4j.properties" your_program.py
```

