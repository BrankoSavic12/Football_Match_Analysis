package rs.raf.pds.spark;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class HetTrikEuro {

    public static void main(String[] args) {
        String putanjaScorers = args.length > 0 ? args[0] : "dataset/goalscorers.csv";
        String putanjaResults = args.length > 1 ? args[1] : "dataset/results.csv";

        SparkSession spark = SparkSession.builder()
                .appName("Het-trikovi na UEFA EURO (DF)")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        
        Dataset<Row> scorers = spark.read()
                .option("header", "true").option("inferSchema", "true")
                .csv(putanjaScorers)
                .withColumn("date", to_date(col("date")))
                .withColumn("own_goal_str", lower(col("own_goal").cast("string")));
     

        Dataset<Row> results = spark.read()
                .option("header", "true").option("inferSchema", "true")
                .csv(putanjaResults)
                .withColumn("date", to_date(col("date")));

        
        Column notOwnGoal = col("own_goal_str").isNull()
                .or(col("own_goal_str").equalTo("false"))
                .or(col("own_goal_str").equalTo("0"));
          
        

        Dataset<Row> cistiGolovi = scorers.filter(notOwnGoal)
                .select("date", "home_team", "away_team", "team", "scorer");

        // --- Broj golova strelca na istoj utakmici ---
        Dataset<Row> zbirPoUtakmici = cistiGolovi.groupBy(
                        col("date"), col("home_team"), col("away_team"),
                        col("team"), col("scorer"))
                .agg(count(lit(1)).alias("total_scorer_goals"));
        
       
        // --- JOIN preko USING kolona: uklanja duplikate 
        Dataset<Row> saTurnirom = zbirPoUtakmici.join(
                results.select("date", "home_team", "away_team", "tournament", "country"),
                new String[]{"date", "home_team", "away_team"},
                "inner"
        );

    
        Column jeEuro = lower(col("tournament")).like("%uefa euro%")
                .and(not(lower(col("tournament")).like("%qualification%")));

        Dataset<Row> hetTrikovi = saTurnirom
                .filter(jeEuro.and(col("total_scorer_goals").geq(3)));

        // --- Tražene kolone i redosled + uredan prikaz ---
        Dataset<Row> izlaz = hetTrikovi.select(
                        col("date").alias("Datum"),
                        col("home_team").alias("Domaci"),
                        col("away_team").alias("Gosti"),
                        col("team").alias("Igra_za"),
                        col("scorer").alias("Strelac"),
                        col("total_scorer_goals").alias("Golova_na_utakmici"),
                        col("tournament").alias("Turnir"),
                        col("country").alias("Zemlja")
                )
                .orderBy(col("Datum").asc(), col("Strelac"));

        izlaz.show(200, false); 

        spark.stop();
    }
}
