package rs.raf.pds.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.expressions.Window.partitionBy;

public class NajduziPeriodBezPoraza {

    public static void main(String[] args) {

        String putanjaResults = args.length > 0 ? args[0] : "dataset/results.csv";

        SparkSession spark = SparkSession.builder()
                .appName("20 najduzih perioda bez poraza (posle 1980)")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // --- Ucitavanje i standardizacija datuma ---
        Dataset<Row> results = spark.read()
                .option("header", "true").option("inferSchema", "true")
                .csv(putanjaResults)
                .withColumn("date", to_date(col("date")));

        // --- Skup reprezentacija koje su ikada ucestvovale na zavrsnom FIFA World Cup ---
        Column jeMundijal = lower(col("tournament")).equalTo(lit("fifa world cup"));
        Dataset<Row> wcTimovi = results.filter(jeMundijal)
                .select(col("home_team").alias("team"))
                .unionByName(results.filter(jeMundijal).select(col("away_team").alias("team")))
                .distinct(); // (team)

        // --- Posle 1980. (uzimamo samo meceve nakon 1980-01-01) ---
        Dataset<Row> posle1980 = results.filter(col("date").geq(lit("1980-01-01")))
                .select("date", "home_team", "away_team", "home_score", "away_score");

        // --- Perspektiva oba tima za svaku utakmicu ---
        Dataset<Row> kaoDomacin = posle1980
                .select(
                        col("date"),
                        col("home_team").alias("team"),
                        col("away_team").alias("opponent"),
                        col("home_score").alias("gf"),
                        col("away_score").alias("ga")
                );

        Dataset<Row> kaoGost = posle1980
                .select(
                        col("date"),
                        col("away_team").alias("team"),
                        col("home_team").alias("opponent"),
                        col("away_score").alias("gf"),
                        col("home_score").alias("ga")
                );

        Dataset<Row> sviNastupi = kaoDomacin.unionByName(kaoGost);

        // --- Porazi (gf < ga) ---
        Dataset<Row> porazi = sviNastupi.filter(col("gf").lt(col("ga")))
                .select("team", "opponent", "date");

        // --- Razmotri samo timove koji su ikada igrali na FIFA World Cup (finalni turnir) ---
        Dataset<Row> poraziWCTimovi = porazi.join(wcTimovi, "team"); // semi-join preko USING "team"

        // --- Sort po timu i datumu, pa prethodni poraz + broj dana izmedju poraza ---
        WindowSpec w = partitionBy(col("team")).orderBy(col("date").asc());
        Dataset<Row> saPrethodnim = poraziWCTimovi
                .withColumn("prev_loss_date", lag(col("date"), 1).over(w))
                .withColumn("prev_opponent", lag(col("opponent"), 1).over(w))
                .withColumn("period_dana", datediff(col("date"), col("prev_loss_date")))
                .filter(col("prev_loss_date").isNotNull()); // treba nam bar dva poraza da bismo imali period bez poraza

        // --- Top 20 najduzih perioda bez poraza ---
        Dataset<Row> top20 = saPrethodnim
                .orderBy(col("period_dana").desc(), col("date").desc())
                .limit(20)
                .select(
                        col("team").alias("Reprezentacija"),
                        col("opponent").alias("Poraz od"),
                        col("date").alias("Poraz datum"),
                        col("prev_opponent").alias("Prethodni poraz od"),
                        col("prev_loss_date").alias("Prethodni poraz datum"),
                        col("period_dana").alias("Period-dana")
                );

        // Prikaz lepo formatirane tabele
        top20.show(50, false);

        spark.stop();
    }
}
