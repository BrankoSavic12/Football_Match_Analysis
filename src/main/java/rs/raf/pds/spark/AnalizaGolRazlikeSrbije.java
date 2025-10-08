package rs.raf.pds.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
//import static org.apache.spark.sql.functions.*;

import java.util.List;

public class AnalizaGolRazlikeSrbije {

    public static void main(String[] args) {
        // Putanja do results.csv
        String putanjaDoCsv = args.length > 0 ? args[0] : "dataset/results.csv";

        // Spark sesija 
        SparkSession sesija = SparkSession.builder()
                .appName("Analiza gol razlike Srbije (RDD)")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        sesija.sparkContext().setLogLevel("ERROR");

        // Učitaj CSV kao DF (radi pouzdanog parsiranja), pa prebaci u RDD
        Dataset<Row> df = sesija.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(putanjaDoCsv)
                .select("home_team", "away_team", "home_score", "away_score");

        JavaRDD<Row> rdd = df.javaRDD();

        // (protivnik, gol-razlika iz perspektive Srbije u toj utakmici)
        JavaPairRDD<String, Integer> poProtivniku = rdd
                .filter(red -> {
                    String domacin = red.getAs("home_team");
                    String gost = red.getAs("away_team");
                    return "Serbia".equals(domacin) || "Serbia".equals(gost);
                })
                .mapToPair(red -> {
                    String domacin = red.getAs("home_team");
                    String gost = red.getAs("away_team");
                    Integer gDom = red.getAs("home_score");
                    Integer gGos = red.getAs("away_score");

                    String protivnik;
                    int razlika;
                    if ("Serbia".equals(domacin)) {           
                        protivnik = gost;
                        razlika = (gDom == null ? 0 : gDom) - (gGos == null ? 0 : gGos);
                    } else {                                  
                        protivnik = domacin;
                        razlika = (gGos == null ? 0 : gGos) - (gDom == null ? 0 : gDom);
                    }
                    return new Tuple2<>(protivnik, razlika);
                })
                .reduceByKey(Integer::sum); // map-reduce: zbir gol-razlika po protivniku

        // Najbolji i najgori (max/min) protivnik po ukupnoj gol-razlici
        Tuple2<String, Integer> najbolji = poProtivniku.reduce((a, b) -> a._2() >= b._2() ? a : b);
        Tuple2<String, Integer> najgori  = poProtivniku.reduce((a, b) -> a._2() <= b._2() ? a : b);

        Integer maxVrednost = najbolji._2();
        Integer minVrednost = najgori._2();

        // Ako postoji deljenje rekorda, prikaži sve sa istim maksimumom/minimumom
        List<Tuple2<String, Integer>> sviNajbolji = poProtivniku.filter(t -> t._2().equals(maxVrednost)).collect();
        List<Tuple2<String, Integer>> sviNajgori  = poProtivniku.filter(t -> t._2().equals(minVrednost)).collect();

        System.out.println("Najbolja ukupna gol-razlika Srbije je " + maxVrednost + " protiv:");
        for (Tuple2<String, Integer> t : sviNajbolji) System.out.println(" - " + t._1());

        System.out.println("Najgora ukupna gol-razlika Srbije je " + minVrednost + " protiv:");
        for (Tuple2<String, Integer> t : sviNajgori) System.out.println(" - " + t._1());

        sesija.stop();
    }
}
