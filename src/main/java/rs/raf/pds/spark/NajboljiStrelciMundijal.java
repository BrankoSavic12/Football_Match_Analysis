package rs.raf.pds.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.expressions.Window.partitionBy;

public class NajboljiStrelciMundijal {

    public static void main(String[] args) {
        String putanjaScorers = args.length > 0 ? args[0] : "dataset/goalscorers.csv";
        String putanjaResults = args.length > 1 ? args[1] : "dataset/results.csv";

        SparkSession spark = SparkSession.builder()
                .appName("Najbolji strelci - FIFA World Cup od 1990")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // Ucitavanje
        Dataset<Row> scorers = spark.read()
                .option("header", "true").option("inferSchema", "true")
                .csv(putanjaScorers)
                .withColumn("date", to_date(col("date")))
                .withColumn("own_goal_str", lower(col("own_goal").cast("string")));

        Dataset<Row> results = spark.read()
                .option("header", "true").option("inferSchema", "true")
                .csv(putanjaResults)
                .withColumn("date", to_date(col("date")));

        // --- Samo regularni golovi (bez autogolova) ---
        Column notOwnGoal = col("own_goal_str").isNull()
                .or(col("own_goal_str").equalTo("false"))
                .or(col("own_goal_str").equalTo("0"));

        Dataset<Row> cistiGolovi = scorers.filter(notOwnGoal)
                .select("date", "home_team", "away_team", "team", "scorer");

        // --- Golovi strelca na pojedinacnoj utakmici ---
        Dataset<Row> goloviPoUtakmici = cistiGolovi.groupBy(
                        col("date"), col("home_team"), col("away_team"),
                        col("team"), col("scorer"))
                .agg(count(lit(1)).alias("golova_na_utakmici"));

        // --- Join prego Using kljuceva (nema dvosmislenih kolona) ---
        Dataset<Row> saTurnirom = goloviPoUtakmici.join(
                results.select("date", "home_team", "away_team", "tournament", "country"),
                new String[]{"date", "home_team", "away_team"},
                "inner"
        );

        // --- FILTAR: FIFA WORLD CUP, GODINA >= 1990 ---
        Column jeMundijal = lower(col("tournament")).like("%fifa world cup%")
        		.and(not(lower(col("tournament")).like("%qualification%")));
        Dataset<Row> wcOd1990 = saTurnirom
                .withColumn("year", year(col("date")))
                .filter(jeMundijal.and(col("year").geq(1990)));

        // --- Suma golova po (tournament, year, scorer, team) ---
        Dataset<Row> zbirPoTurniru = wcOd1990.groupBy(
                        col("tournament"), col("year"), col("country"),
                        col("scorer"), col("team"))
                .agg(sum(col("golova_na_utakmici")).alias("totalGoals"));

        // --- Rangiranje: prvo/drugo mesto (dozvoli izjednačenja) ---
        WindowSpec w = partitionBy(col("tournament"), col("year"))
                .orderBy(col("totalGoals").desc(), col("scorer").asc());
        Dataset<Row> saRangom = zbirPoTurniru.withColumn("rang", dense_rank().over(w))
                .filter(col("rang").leq(2));

        // --- Izlaz: tražene kolone + sortiranje Godina↑, Golova↓ ---
        Dataset<Row> izlaz = saRangom.select(
                        col("scorer").alias("Strelac"),
                        col("team").alias("Reprezentacija"),
                        col("totalGoals").alias("Golova"),
                        col("rang").alias("Rang"),
                        col("year").alias("Godina"),
                        col("tournament").alias("Turnir"),
                        col("country").alias("Zemlja")
                )
                .orderBy(col("Godina").asc(), col("Golova").desc(), col("Strelac").asc());

        // Lep prikaz tabele
        izlaz.show(200, false);

        // (opciono) snimanje
        // izlaz.coalesce(1).write().mode(SaveMode.Overwrite).option("header","true").csv("out/najbolji_strelci_wc_od_1990");

        spark.stop();
    }
}
