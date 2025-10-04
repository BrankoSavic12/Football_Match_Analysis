package rs.raf.pds.spark;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class HatTricksEuro {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HatTricksEuro").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // === Učitavanje goalscorers.csv ===
        JavaRDD<String> goalsData = sc.textFile("dataset/goalscorers.csv");
        String header1 = goalsData.first();
        goalsData = goalsData.filter(line -> !line.equals(header1));

        // (date,home_team,away_team,scorer) -> broj golova
        JavaPairRDD<String, Integer> scorerGoals = goalsData
                .mapToPair(line -> {
                    String[] parts = line.split(",", -1);
                    String date = parts[0];
                    String home = parts[1];
                    String away = parts[2];
                    String scorer = parts[4];
                    String key = date + "," + home + "," + away + "," + scorer;
                    return new Tuple2<>(key, 1);
                })
                .reduceByKey(Integer::sum)
                .filter(x -> x._2 >= 3);

        // (date,home,away) -> scorer (broj golova)
        JavaPairRDD<String, String> hatTricks = scorerGoals
                .mapToPair(x -> {
                    String[] keyParts = x._1.split(",");
                    String date = keyParts[0];
                    String home = keyParts[1];
                    String away = keyParts[2];
                    String scorer = keyParts[3];
                    String newKey = date + "," + home + "," + away;
                    String newValue = scorer + " (" + x._2 + ")";
                    return new Tuple2<>(newKey, newValue);
                });

        // === Učitavanje results.csv ===
        JavaRDD<String> resultsData = sc.textFile("dataset/results.csv");
        String header2 = resultsData.first();
        resultsData = resultsData.filter(line -> !line.equals(header2));

        // (date,home,away) -> tournament,country
        JavaPairRDD<String, String> results = resultsData
                .mapToPair(line -> {
                    String[] parts = line.split(",", -1);
                    String date = parts[0];
                    String home = parts[1];
                    String away = parts[2];
                    String tournament = parts[5];
                    String country = parts[7];
                    String key = date + "," + home + "," + away;
                    String value = tournament + "," + country;
                    return new Tuple2<>(key, value);
                });

        // === Join ===
        JavaPairRDD<String, Tuple2<String, String>> joined = hatTricks.join(results);

        // filtriraj samo UEFA Euro
        JavaRDD<String[]> hetTrikoviEuro = joined
                .filter(x -> x._2._2.startsWith("UEFA Euro"))
                .map(x -> {
                    String[] matchKey = x._1.split(",", -1); // [date, home, away]
                    String scorer = x._2._1;
                    String[] parts = x._2._2.split(",", -1); // [tournament, country]
                    return new String[]{matchKey[0], matchKey[1], matchKey[2], scorer, parts[0], parts[1]};
                });

        // Ispis u formi tabele
        System.out.println("+------------+----------------+----------------+----------------------+----------------+----------------+");
        System.out.println("|    Datum   |     Domaci     |     Gosti      |       Strelac        |     Turnir     |     Zemlja     |");
        System.out.println("+------------+----------------+----------------+----------------------+----------------+----------------+");

        hetTrikoviEuro.collect().forEach(row -> {
            System.out.printf("| %-10s | %-14s | %-14s | %-20s | %-14s | %-14s |%n",
                    row[0], row[1], row[2], row[3], row[4], row[5]);
        });

        System.out.println("+------------+----------------+----------------+----------------------+----------------+----------------+");

        sc.stop();
    }
}
