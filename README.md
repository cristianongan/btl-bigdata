## init db

docker compose build
docker compose up -d

docker exec -it mongo-mongos-1 bash

mongosh

use admin
db.createUser({
  user: "root",
  pwd:  "12345678aA@",
  roles: [ { role: "root", db: "admin" } ]
})

docker exec -it mongo-mongos-1 bash
 
mongoimport --db admin --collection big-data --type csv --headerline --file /data/cus.csv