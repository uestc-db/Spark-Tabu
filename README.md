# Spark-Tabu

We provide sources codes of our Tabu search algorithm based on spark. And used it to solve the variant of VRP problem.

### Getting Started

Before using the project, make sure you have set up the environment of hadoop and spark.

Building this project requires SBT. After you launch SBT, you can run `sbt package` to build this project.

We use [Archery](https://github.com/meetup/archery) which is a two-dimensional R-Tree written in Scala to partition the data. 
### Usage example

The following commands can be used to submit a spark application. In this application, the tabu tenure is 15, the number of threads is 100 and the diversification rate is 0.02.

```
spark-submit \
  --class "Logistics" \
  --master yarn \
  --jars ./lib/opencsv-4.0.jar,./lib/archery_2.10-MDVRPTW_6.jar \
  --deploy-mode cluster \
  --num-executors 20 \
  --executor-memory 4g \
  --executor-cores 5 \
  --driver-memory 5g \
  ./target/scala-2.10/logistics-project_2.10-1.0.jar \
  logistics 15 6 0.02 100
  ```
