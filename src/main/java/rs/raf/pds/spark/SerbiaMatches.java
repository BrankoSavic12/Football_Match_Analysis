package rs.raf.pds.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class SerbiaMatches {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Serbia Goal Difference RDD")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Učitaj CSV kao plain tekst (linija po linija)
        JavaRDD<String> lines = sc.textFile("dataset/results.csv");

        // Prva linija je header → preskoči je
        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

        // Filtriramo samo mečeve gde igra Srbija
        JavaRDD<String> serbiaMatches = data.filter(line -> line.contains("Serbia"));

        // Parsiramo svaku liniju → (opponent, diff)
        JavaPairRDD<String, Integer> diffs = serbiaMatches.mapToPair(line -> {
            String[] parts = line.split(",");

            String home = parts[1];
            String away = parts[2];
            int hs = Integer.parseInt(parts[3]);
            int as = Integer.parseInt(parts[4]);

            String opponent;
            int diff;
            if (home.equals("Serbia")) {
                opponent = away;
                diff = hs - as;
            } else {
                opponent = home;
                diff = as - hs;
            }
            return new Tuple2<>(opponent, diff);
        });

        // Saberi sve gol-razlike po protivniku
        JavaPairRDD<String, Integer> totalDiffs = diffs.reduceByKey(Integer::sum);

        // Nađi najbolju i najgoru gol-razliku
        Tuple2<String, Integer> best = totalDiffs.max(new TupleComparator());
        Tuple2<String, Integer> worst = totalDiffs.min(new TupleComparator());

        System.out.println("=== Najbolja gol-razlika (RDD) ===");
        System.out.println(best._1 + " : " + best._2);

        System.out.println("=== Najgora gol-razlika (RDD) ===");
        System.out.println(worst._1 + " : " + worst._2);

        sc.close();
    }

    // Potreban comparator za min/max
    public static class TupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return Integer.compare(t1._2, t2._2);
        }
    }
}
