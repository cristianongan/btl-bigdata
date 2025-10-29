## init db

docker compose build
docker compose up -d

docker exec -it app-mongos-1 bash

mongosh

use admin
db.createUser({
  user: "root",
  pwd:  "12345678aA@",
  roles: [ { role: "root", db: "admin" } ]
})

docker exec -it mongo-mongos-1 bash
 
mongoimport --db admin --collection big-data --type csv --headerline --file /data/cus.csv

docker exec -it namenode bash

hdfs dfs -mkdir -p /app/data
hdfs dfs -put -f /data/* /app/data/
hdfs dfs -ls /app/data

docker exec -it spark-master bash -lc '
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --class Main \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
    --conf spark.jars.ivy=/tmp/ivy \
    /opt/spark-apps/spark-run-1.0-SNAPSHOT.jar \
    --run-date 2025-10-28
'