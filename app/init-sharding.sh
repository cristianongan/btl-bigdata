#!/bin/sh
wait_up() {
  local svc="$1"
  echo "Waiting for $svc ..."
  until mongosh --host "$svc" --quiet --eval "db.runCommand({ ping: 1 }).ok" >/dev/null 2>&1; do
    sleep 1
  done
}

echo "Init ConfigRS (idempotent)"
mongosh --host cfg1 --quiet <<'JS'
try { rs.status() } catch (e) {
  rs.initiate({
    _id: "cfgReplSet",
    configsvr: true,
    members: [
      { _id: 0, host: "cfg1:27017" },
      { _id: 1, host: "cfg2:27017" },
      { _id: 2, host: "cfg3:27017" }
    ]
  });
}
JS

echo "Init Shard1 RS (idempotent)"
mongosh --host shard1a --quiet <<'JS'
try { rs.status() } catch (e) {
  rs.initiate({
    _id: "shard1rs",
    members: [
      { _id: 0, host: "shard1a:27017" },
      { _id: 1, host: "shard1b:27017" },
      { _id: 2, host: "shard1c:27017" }
    ]
  });
}
JS

echo "Add shard + enable sharding (idempotent)"
mongosh --host mongos --quiet <<'JS'
const s = sh.status();
if (!s.shards || s.shards.length === 0) {
  sh.addShard("shard1rs/shard1a:27017,shard1b:27017,shard1c:27017");
}
db.getSiblingDB("demo");  // chọn DB
try { sh.enableSharding("demo") } catch (e) {}
try { sh.shardCollection("demo.users", { userId: "hashed" }) } catch (e) {}
JS

echo "Sharding initialized."
