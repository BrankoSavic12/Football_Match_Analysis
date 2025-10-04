package rs.raf.pds.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import java.time.Year;

public class TeslaCar {
	/* 
	Dataset - Podaci o registrovanim automobilima u američkoj državi Vašington 
	https://www.kaggle.com/datasets/adityakishor1/electric-vehicle-population-data-2024
	Fajl sa podacima se nalazi u dataset folderu unutar projekta!

	Kolone:
	|VIN (1-10)|County|City|State|Postal Code|Model Year|Make|Model|Electric Vehicle Type|Clean Alternative Fuel Vehicle (CAFV) Eligibility|Electric Range|Base MSRP|Legislative District|DOL Vehicle ID|Vehicle Location|Electric Utility|2020 Census Tract|
	
	Deo dataset-a:
	+----------+---------+-----------------+-----+-----------+----------+----------+-------+---------------------+-------------------------------------------------+--------------+---------+--------------------+--------------+--------------------+--------------------+-----------------+
	|VIN (1-10)|   County|             City|State|Postal Code|Model Year|      Make|  Model|Electric Vehicle Type|Clean Alternative Fuel Vehicle (CAFV) Eligibility|Electric Range|Base MSRP|Legislative District|DOL Vehicle ID|    Vehicle Location|    Electric Utility|2020 Census Tract|
	+----------+---------+-----------------+-----+-----------+----------+----------+-------+---------------------+-------------------------------------------------+--------------+---------+--------------------+--------------+--------------------+--------------------+-----------------+
	|WBY8P6C58K|     King|          Seattle|   WA|      98115|      2019|       BMW|     I3| Battery Electric ...|                             Clean Alternative...|           153|        0|                  43|     259254397|POINT (-122.30082...|CITY OF SEATTLE -...|      53033003601|
	|5YJSA1DN4D|   Kitsap|        Bremerton|   WA|      98312|      2013|     TESLA|MODEL S| Battery Electric ...|                             Clean Alternative...|           208|    69900|                  35|     127420940|POINT (-122.69612...|PUGET SOUND ENERG...|      53035080700|
	|5YJSA1E26J|     King|             Kent|   WA|      98042|      2018|     TESLA|MODEL S| Battery Electric ...|                             Clean Alternative...|           249|        0|                  47|     170287183|POINT (-122.11451...|PUGET SOUND ENERG...|      53033031708|
	|WBY2Z2C54E|     King|         Bellevue|   WA|      98004|      2014|       BMW|     I8| Plug-in Hybrid El...|                             Not eligible due ...|            14|        0|                  41|     205545868|POINT (-122.20239...|PUGET SOUND ENERG...|      53033024002|
	|5YJXCDE23J|     King|         Bellevue|   WA|      98004|      2018|     TESLA|MODEL X| Battery Electric ...|                             Clean Alternative...|           238|        0|                  41|     237977386|POINT (-122.20239...|PUGET SOUND ENERG...|      53033023601|
	|WBY33AW0XP|     King|          Seattle|   WA|      98109|      2023|       BMW|     I4| Battery Electric ...|                             Eligibility unkno...|             0|        0|                  36|     238283545|POINT (-122.34415...|CITY OF SEATTLE -...|      53033007002|
	...
	*/
	
	public static void zad1_RDD(JavaRDD<String> autoRDD) {
		//  Zadatak br. 1 - U kom gradu se najviše vozi BMW ?
		
		JavaPairRDD<String, Integer> bmwRows = autoRDD.filter(line -> line.contains("BMW"))
							.map(line -> line.split(","))
							.mapToPair(attributes -> new Tuple2<>(attributes[2], 1));
							
		
		
		JavaPairRDD<Integer, String> bmwCitiesRDD = bmwRows.reduceByKey((a,b) -> a + b)
							.mapToPair(Tuple2::swap)	// Zamena mesta u paru kljuc-vrednost, 
							.sortByKey(false);			// jer moze samo da se sortira po kljucu!
		
		System.out.println("\nZADATAK 1 - RDD - MAP_REDUCE REZULTAT");
		bmwCitiesRDD.take(5).forEach(System.out::println);
		
		/* ZADATAK 1 - RDD - MAP_REDUCE REZULTAT
			(1671,Seattle)
			(517,Bellevue)
			(319,Redmond)
			(315,Kirkland)
			(305,Sammamish)
		 */
				
	}
	private static void zad1_DF(Dataset<Row> autoDF) {
		//  Zadatak br. 1 - U kom gradu se najviše vozi BMW ?
		// Dataframe resenje
		
		Dataset<Row> bmwCities = autoDF.filter(col("Make").equalTo("BMW"));
		
		Dataset<Row> result = bmwCities.groupBy("City")
										.agg(count("*").as("Count"))
										.orderBy(col("Count").desc());
		
		System.out.println("\nZADATAK 1 - Dataframe REZULTAT");
		result.show();
		
		/* ZADATAK 1 - Rezultat: 
		+-----------------+-----+
		|             City|Count|
		+-----------------+-----+
		|          Seattle| 1671|
		|         Bellevue|  517|
		|          Redmond|  319|
		|         Kirkland|  315|
		|        Sammamish|  305|
			....
		*/
	}
	private static void zad2(Dataset<Row> autoDF) {
		// Zadatak br. 2  
	    // U kojoj oblasti se voze najmladji automobili u proseku, a u kojoj se voze najstariji automobili
		
		Dataset<Row> countiesYear = autoDF.select(col("County"), col("Model Year"))
				.withColumn("Age", col("Model Year"));

		Dataset<Row> countiesCarAvgYear = countiesYear.groupBy("County").
							agg(avg(col("Model Year")).as("AvgAge"))
							.orderBy(col("AvgAge").desc());

		System.out.println("ZADATAK 2 - REZULTAT");
		System.out.println("		 Okrug, godiste");
						
		System.out.println("Najmladji	:" + countiesCarAvgYear.head().mkString(",")); 
		System.out.println("Najstariji	:" + ((Row[])countiesCarAvgYear.tail(1))[0].mkString(","));
		
		/* ZADATAK 2 - REZULTAT
		Najmladji  :Lake,2023.1666666666667
		Najstariji :Charles,2012.0
		*/
			
	}
	
	private static void zad3(Dataset<Row> autoDF) {
		// Zadatak br. 3 - U kojem okrugu Tesla Model 3 je najpopularniji ?
		
		// Grupisanje po okrugu i računanje ukupnog broja i broja Tesla Model 3
		    
		Dataset<Row> groupedDF = autoDF.groupBy("County")
		            .agg(count("*").alias("Total Cars"),
		                 sum(when(col("Model").equalTo("MODEL 3"), 1).otherwise(0)).as("Tesla Model 3 Total"))
		            .filter(col("Total Cars").$greater(10)); 

	    // Racunanje procenta Tesla Model 3 od svih automobila
	    Dataset<Row> percentageDF = groupedDF.withColumn("Procenat Tesla Model 3", col("Tesla Model 3 Total").divide(col("Total Cars")).multiply(100))
	    		.orderBy(col("Procenat Tesla Model 3").desc());

	    // County sa najvećim procentom Tesla Modela 3 
	    	    
	    System.out.println("\nZADATAK 3 - REZULTAT");
	    
	    percentageDF.show(50, false); 
	    
	    Row highestPercentageCounty = percentageDF.first();

	    // Print the result
	    System.out.println("County sa najvecim procentom Tesla Model 3: " + highestPercentageCounty.getAs("County"));
	    System.out.println("Procenat Tesla Model 3 automobila: " + highestPercentageCounty.getAs("Procenat Tesla Model 3"));
	    
	    /* ZADATAK 3 - REZULTAT 
		+------------+----------+-------------------+----------------------+
		|      County|Total Cars|Tesla Model 3 total|Procenat Tesla Model 3|
		+------------+----------+-------------------+----------------------+
		|   San Diego|        24|                  7|    29.166666666666668|
		|    Franklin|       617|                138|    22.366288492706644|
		|       Ferry|        32|                  7|                21.875|
		|      Yakima|      1070|                212|    19.813084112149532|
		|      Benton|      2282|                406|    17.791411042944784|
		|   Snohomish|     22086|               3928|    17.785022186000184|
		...
		*/
	    
	}
	private static void zad4(Dataset<Row> autoDF) {
		// ZADATAK 4 
		// Koji su najmladji automobili koji se voze?
		
		Dataset<Row> carModelYearDS = autoDF.select("Make", "Model", "Model Year");
		
		Dataset<Row> carModelAvgYear = carModelYearDS.groupBy("Make", "Model")
											.agg(count("*").as("Total"), 
												avg(negate(col("Model Year")).plus(Year.now().getValue())).as("Avg Age"))
											.orderBy(col("Avg Age").asc());
		
		System.out.println("ZADATAK 4 - REZULTAT");
		carModelAvgYear.show(50);
		
		/* ZADATAK 4 - REZULTAT 
		+-------------+----------------+-----+------------------+
		|         Make|           Model|Total|           Avg Age|
		+-------------+----------------+-----+------------------+
		|          BMW|              I5|   79|               1.0|
		|    CHEVROLET|       BLAZER EV|   34|               1.0|
		|        MAZDA|           CX-90|  545|               1.0|
		|         AUDI|              Q8|  338|               1.0|
		|        TESLA|      CYBERTRUCK|   65|               1.0|
		|          BMW|              XM|   10|               1.0|
		|        DODGE|          HORNET|  614|               1.0|
		|   ALFA ROMEO|          TONALE|   47|               1.0|
		|        LEXUS|              TX|    4|               1.0|
		|         AUDI|             SQ8|    9|               1.0|
		|          KIA|             EV9|  320|               1.0|
		|    CHEVROLET|    SILVERADO EV|   52|               1.0|
		|          GMC|HUMMER EV PICKUP|    4|               1.0|
		|        LEXUS|              RX|   65|               1.0|
		|          BMW|            750E|    1|               1.0|
		|  ROLLS ROYCE|         SPECTRE|    2|               1.0|
		|     CADILLAC|           LYRIQ|  405|1.1407407407407408|
		|      GENESIS|            GV70|   38|1.4210526315789473|
		|          BMW|              I7|   39|1.5128205128205128|
		|        LEXUS|              NX|  223|1.5650224215246638|
		...
		*/
		
		
	}
	public static void main(String[] args) {
        	    
		

		// Create SparkSession
		SparkSession spark = SparkSession.builder()
                .appName("RegistracijeAutomobila")
                .master("local[*]")
                .getOrCreate();
	    spark.sparkContext().setLogLevel("OFF"); 
	   	
	    // Kreiranje RDD-a od txt fajla!
	    JavaRDD<String> autoRDD = spark.sparkContext()
	    		.textFile("dataset/Electric_Vehicle_Population_Data.csv", 0).toJavaRDD();
	    
	    // Kreiranje Dataframe-a, tj. Dataset<Row> od csv fajla!
	    Dataset<Row> autoDF = spark.read().option("header", "true")
	    		.csv("dataset/Electric_Vehicle_Population_Data.csv");
	    
	    

	    //  Zadatak br. 1 
	    // U kojem gradu se najviše vozi BMW ?
	    zad1_RDD(autoRDD);
	    zad1_DF(autoDF);
	    
	    
	    // Zadatak br. 2  
	    // U kojoj oblasti se voze najmladji automobili u proseku, a u kojoj se voze najstariji automobili
	    zad2(autoDF);
	    
	    // Zadatak br. 3 
	    // U kojem okrugu Tesla Model 3 je najpopularniji ?
        zad3(autoDF);
        
        // ZADATAK 4 
		// Koji su najmladji automobili koji se voze?
        zad4(autoDF);
    	
        	
        // Stop SparkSession
        //spark.stop();
    }

	
}
