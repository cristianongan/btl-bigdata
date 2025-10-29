import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

public class Main {
    private static final String MONGO_URI = "mongodb://root:12345678aA%40@mongos:27017/admin?authSource=admin";

    private static final String HADOOP_URI = "hdfs://namenode:8020";
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    //    private static final String MONGO_URI = "mongodb://root:12345678aA%40@localhost:27017/admin?authSource=admin";
//
//    private static final String HADOOP_URI = "hdfs://localhost:8020";
//
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Hotels")
                .master("local[*]")
                .config("spark.mongodb.write.connection.uri",
                        Main.MONGO_URI)
                .config("spark.mongodb.write.database",   "app-demo")
                .config("spark.mongodb.write.collection", "hotel")
                .config("spark.mongodb.read.connection.uri",
                        Main.MONGO_URI)
                .getOrCreate();

        Dataset<Row> anb = spark.read().format("csv").option("header","true")
                .option("recursiveFileLookup","true").schema(StructTypes.ANB)
                .load(String.format("%s%s", HADOOP_URI, StructTypes.PATTERN.get("anb")))
                .withColumn("city", when(col("location").isNotNull(), col("location")))
                .select(
                        lit("anb").alias("source"),
                        col("id").cast("string").alias("external_id"),
                        col("name").alias("name"),
                        col("location").alias("city"),
                        col("price").cast("double").alias("price"),
                        col("review_scores_rating").alias("rating_score_100")
                );

        Dataset<Row> booking = spark.read().format("csv").option("header","true")
                .option("recursiveFileLookup","true").schema(StructTypes.BOOKING)
                .load(String.format("%s%s", HADOOP_URI, StructTypes.PATTERN.get("booking")))
                .withColumn("city", regexp_extract(input_file_name(), ".*/eu/([^/]+)/.*", 1))
                .withColumn("price_num", regexp_extract(col("Prices"), "\\d+(?:[\\.,]\\d+)?", 0))
                .select(
                        lit("booking").alias("source"),
                        sha2(concat_ws("||", col("Hotels"), col("city")), 256).alias("external_id"),
                        col("Hotels").alias("name"),
                        col("City").alias("city"),
                        translate(col("price_num"), ",", ".").cast("double").alias("price"),
                        lit(null).cast("double").alias("rating_score_100")
                );

        // Other (all_hotels.csv)
        Dataset<Row> other = spark.read().format("csv").option("header","true")
                .option("recursiveFileLookup","true").schema(StructTypes.OTHER)
                .load(String.format("%s%s", HADOOP_URI, StructTypes.PATTERN.get("other")))
                .withColumn("city", col("Region City"))
                .withColumn("price_num", regexp_extract(col("Price"), "\\d+(?:[\\.,]\\d+)?", 0))
                .select(
                        lit("other").alias("source"),
                        col("Unnamed: 0").cast("string").alias("external_id"),
                        col("Hotel name").alias("name"),
                        col("city"),
                        translate(col("price_num"), ",", ".").cast("double").alias("price"),
                        (col("Marks").multiply(10.0)).alias("rating_score_100")
                );

        Dataset<Row> unified = anb.unionByName(booking, true)
                .unionByName(other, true)
                .filter(col("name").isNotNull().and(col("city").isNotNull()))
                .withColumn("city", trim(col("city")))
                .withColumn("_id", concat_ws(":", col("source"), col("external_id"))); // key upsert

        unified = unified.dropDuplicates("_id");

        Dataset<Row> df = unified
                .withColumn("name_norm", trim(regexp_replace(lower(col("name")), "\\s+", " ")))
                .withColumn("name_core", regexp_replace(col("name_norm"),
                        "\\b(hotel|hostel|apart(ment|s)?|bnb|the)\\b", ""))
                .withColumn("block_key", substring(col("name_core"), 1, 4))
                .withColumn("city_norm", lower(regexp_replace(col("city"), "\\s+", "")));

        Dataset<Row> cand = df.as("a").join(df.as("b"),
                col("a.city_norm").equalTo(col("b.city_norm"))
                        .and(col("a.block_key").equalTo(col("b.block_key")))
                        .and(col("a.source").notEqual(col("b.source")))
                        .and(col("a._id").lt(col("b._id")))
        );

        cand = cand
                .withColumn("len_max",
                        greatest(length(col("a.name_core")), length(col("b.name_core"))))
                .withColumn("sim_name_lev",
                        expr("1.0 - cast(levenshtein(a.name_core, b.name_core) as double) / len_max"))
                .withColumn("tokens_a", split(col("a.name_core"), "\\s+"))
                .withColumn("tokens_b", split(col("b.name_core"), "\\s+"))
                .withColumn("inter", size(array_intersect(col("tokens_a"), col("tokens_b"))))
                .withColumn("union", size(array_union(col("tokens_a"), col("tokens_b"))))
                .withColumn("sim_jaccard", expr("case when union=0 then 0 else inter/union end"))
                .withColumn("score", expr("0.6*sim_name_lev + 0.4*sim_jaccard"));

        Dataset<Row> matches = cand.filter(col("score").geq(lit(0.85)))
                .select(col("a._id").alias("id1"), col("b._id").alias("id2"), col("score"));

        Dataset<Row> canonMap = matches.selectExpr("least(id1,id2) as root","greatest(id1,id2) as leaf")
                .groupBy("leaf").agg(min("root").alias("root"));
        Dataset<Row> resolved = df.join(canonMap, df.col("_id").equalTo(canonMap.col("leaf")), "left")
                .withColumn("canonical_id", coalesce(col("root"), col("_id")))
                .drop("root","leaf");

        Dataset<Row> ranked = resolved.withColumn("pri_source",
                        expr("case when source='other' then 3 when source='anb' then 2 else 1 end"))
                .withColumn("name_len", length(col("name")));

        WindowSpec w = Window.partitionBy("canonical_id")
                .orderBy(col("rating_score_100").desc_nulls_last(),
                        col("name_len").desc(),
                        col("pri_source").desc());

        Dataset<Row> canonical = ranked
                .withColumn("rn", row_number().over(w))
                .filter(col("rn").equalTo(1))
                .drop("rn","name_len","pri_source");

        canonical.write()
                .format("mongodb")
                .mode(SaveMode.Overwrite)
                .option("collection","hotels_canonical")
                .save();

        log.info("canonical length: ");
        canonical.show();

        spark.stop();
    }
}
