# Spark-Tabu

We provide sources codes of our Tabu search algorithm based on spark. And used it to solve the variant of VRP problem.

# Usage example

The following commands can be used to submit a spark application. In this application, the tabu tenure is 25, the number of partitions is 32 and the diversification rate is 0.02.

```
spark-submit \
  --class "Logistics" \
  --master yarn \
  --jars ./lib/opencsv-4.0.jar,./lib/archery_32.jar \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 3g \
  --executor-cores 8 \
  --driver-memory 4g \
  --conf spark.default.parallelism=64 \
  ./target/scala-2.10/logistics-project_2.10-1.0.jar \
  logistics 25 32 0.02  ./final_solution.txt ./solution_vector.txt
  ```
