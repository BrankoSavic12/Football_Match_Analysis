package rs.raf.pds.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import scala.collection.Seq;

import static org.apache.spark.sql.functions.*;
import scala.collection.JavaConverters;
import scala.jdk.CollectionConverters; 

import java.util.Arrays;

public class Wallmart {
	/* 
    Dataset - Podaci o prodaji u maloprodajnim objektima kompanije Walmart u SAD
    https://www.kaggle.com/datasets/devarajv88/walmart-sales-dataset
    Fajl sa podacima se nalazi u dataset folderu unutar projekta!

    Kolone:
   |User_ID|Product_ID|Gender|Age  |Occupation|City_Category|Stay_In_Current_City_Years|Marital_Status|Product_Category|Purchase|
    
    Deo dataset-a:
   +-------+----------+------+-----+----------+-------------+--------------------------+--------------+----------------+--------+
   |User_ID|Product_ID|Gender|  Age|Occupation|City_Category|Stay_In_Current_City_Years|Marital_Status|Product_Category|Purchase|
   +-------+----------+------+-----+----------+-------------+--------------------------+--------------+----------------+--------+
   |1000001| P00069042|     F| 0-17|        10|            A|                         2|             0|               3|    8370|
   |1000001| P00248942|     F| 0-17|        10|            A|                         2|             0|               1|   15200|
   |1000001| P00087842|     F| 0-17|        10|            A|                         2|             0|              12|    1422|
   |1000001| P00085442|     F| 0-17|        10|            A|                         2|             0|              12|    1057|
   |1000002| P00285442|     M|  55+|        16|            C|                        4+|             0|               8|    7969|
   |1000003| P00193542|     M|26-35|        15|            A|                         3|             0|               1|   15227|
   ...
   */	
	
	private static void zad1(Dataset<Row> wallDF) {
		// Zadatak 1. 
		// Kojeg su zanimanja samci koji najvise prosecno trose novca po jednoj kupovini 
		// u odredjenoj kategoriji grada i koji su to iznosi?
		
		Dataset<Row> samciTrosenjeZanimanja = 
				wallDF.filter(col("Marital_Status").equalTo(0))
                	.groupBy("User_ID", "Occupation", "City_Category")
                	.agg(avg(col("Purchase")).as("Prosecna Potrosnja"))
                	.orderBy(col("Prosecna Potrosnja").desc());

        System.out.println("ZADATAK 1 - REZULTAT:");
        samciTrosenjeZanimanja.show(20);
        
        /* ZADATAK 1 - REZULTAT: 
        |User_ID|Occupation|City_Category|Prosecna Potrosnja|
        +-------+----------+-------------+------------------+
        |1003902|         5|            C|18577.893617021276|
        |1003461|         0|            C|           17508.7|
        |1000101|         3|            A|17246.439393939392|
        |1002983|         4|            C|16790.612903225807|
        |1004474|         7|            C|16639.941176470587|
        .....
        */
	}
	
	private static void zad2(Dataset<Row> wallDF) {
		// Zadatak 2. 
		// Dati listu proizvoda sa informacijom u kojem zivotnom dobu
		// se najvise kupuje neki proizvod i u kojoj kategoriji grada 
		
		Dataset<Row> proizvodProdaja = wallDF.groupBy("Product_ID", "Age", "City_Category")
                						.agg(count("*").as("Prodaja"));
		

		Dataset<Row> proizvodProdajaMaxSale = 
				proizvodProdaja.withColumn("Max Prodaja", max("Prodaja").over(Window.partitionBy("Product_ID")))
                                .filter(col("Prodaja").equalTo(col("Max Prodaja")))
                                .orderBy(col("Max Prodaja").desc())
                                .drop("Prodaja");
		

		System.out.println("ZADATAK 2 - REZULTAT:");
		proizvodProdajaMaxSale.show(20);
		
		Dataset<Row> proizvodProdajaMax2 = 
				proizvodProdaja.groupBy("Product_ID")
                	.agg(max("Prodaja").as("Max Prodaja"))
                	.join(proizvodProdaja, "Product_ID")
                	.where(col("Prodaja").equalTo(col("Max Prodaja")))
                	.drop("Prodaja");

        proizvodProdajaMax2.select("Product_ID", "Age", "City_Category", "Max Prodaja")
        			.orderBy(col("Max Prodaja").desc())
        			.show();
        
        /* ZADATAK 2 - REZULTAT:

        +----------+-----+-------------+-----------+
        |Product_ID|  Age|City_Category|Max Prodaja|
        +----------+-----+-------------+-----------+
        | P00265242|26-35|            C|        290|
        | P00025442|26-35|            C|        247|
        | P00112142|26-35|            C|        246|
        | P00110742|26-35|            B|        243|
        | P00184942|26-35|            B|        230|
        | P00057642|26-35|            B|        229|
        | P00058042|26-35|            B|        225|
        ...
        */
		
	}
	
	private static void zad3(Dataset<Row> wallDF) {
		// ZADATAK 3. 
		// Navesti koje proizvode i koje kategorije proizvoda najcesce kupuju
		// zene i muskarci nekog zanimanja.
		
		Dataset<Row> occupationProizvodDS = 
        		wallDF.groupBy("Occupation", "Product_ID", "Gender")
                .agg(count("*").as("Amount"))
                .orderBy("Occupation", "Product_ID", "Gender");
		occupationProizvodDS.show();
		
        Dataset<Row> topOccupationKupovina = 
        		occupationProizvodDS.withColumn("Max Kupovina", max("Amount").over(Window.partitionBy("Occupation", "Gender"))) //"rn", functions.row_number().over(Window.partitionBy("Occupation", "Gender").orderBy(col("Amount").desc()))) //max("Amount").over(Window.partitionBy("Occupation", "Gender")))
        							.filter(col("Amount").equalTo(col("Max Kupovina"))) // .filter(col("rn").equalTo(1)) 
        							//.withColumnRenamed("Amount", "Max Kupovina")
        							.drop("Amount")// .drop("rn") // .drop("Amount", "rn");
        							.orderBy("Occupation", "Product_ID", "Gender");
        System.out.println("ZADATAK 3 - REZULTAT:");
        topOccupationKupovina.show();
        
        // Ako hocemo da piše, Žene/Muškarci:
        topOccupationKupovina.withColumn("Gender", when(col("Gender").equalTo("F"), "Zene").otherwise("Muskarci"))
        					 .show();

        Dataset<Row> occupationProductCategory = 
        			wallDF.groupBy("Occupation", "Gender", "Product_Category")
        			.agg(count("*").as("Category_Amount"))
        			.orderBy(col("Occupation"))
        			.withColumn("Max Category Kupovina", max("Category_Amount").over(Window.partitionBy("Occupation", "Gender")))
        			.filter(col("Category_Amount").equalTo(col("Max Category Kupovina")))
        			.drop("Category_Amount");

        Dataset<Row> zad3fullResult = 
    			occupationProductCategory.join(topOccupationKupovina, new String[] {"Occupation", "Gender"});
        
        System.out.println("ZADATAK 3 - FULL REZULTAT:");
        zad3fullResult.show();		
        
        /* ZADATAK 3 - REZULTAT:
        +----------+------+----------+------------+
        |Occupation|Gender|Product_ID|Max Kupovina|
        +----------+------+----------+------------+
        |         0|     F| P00265242|          62|
        |         0|     M| P00265242|         144|
        |         1|     F| P00265242|          59|
        |         1|     F| P00220442|          59|
        |         1|     M| P00265242|         100|
        |         2|     F| P00059442|          28|
        ...
        +----------+------+----------------+---------------------+
        |Occupation|Gender|Product_Category|Max Category Kupovina|
        +----------+------+----------------+---------------------+
        |         0|     F|               5|                 5657|
        |         0|     M|               1|                14532|
        |         1|     F|               5|                 5536|
        |         1|     M|               5|                 7633|
        |         2|     F|               5|                 2646|
        |         2|     M|               5|                 4967|
        |         3|     F|               5|                 2380|
        ...
        +----------+------+----------------+---------------------+----------+------------+
        |Occupation|Gender|Product_Category|Max Category Kupovina|Product_ID|Max Kupovina|
        +----------+------+----------------+---------------------+----------+------------+
        |         0|     F|               5|                 5657| P00265242|          62|
        |         0|     M|               1|                14532| P00265242|         144|
        |         1|     F|               5|                 5536| P00265242|          59|
        |         1|     F|               5|                 5536| P00220442|          59|
        |         1|     M|               5|                 7633| P00265242|         100|
        |         2|     F|               5|                 2646| P00059442|          28|
        |         2|     M|               5|                 4967| P00265242|          63|
        |         3|     F|               5|                 2380| P00117942|          28|
        |         3|     M|               5|                 2906| P00059442|          28|
        |         3|     M|               5|                 2906| P00025442|          28|
        ...
        */
	}
	
	private static void zad4(Dataset<Row> wallDF) {
		// ZADATAK 4. 
		// Da li u nekom zivotnom dobu, samci muskarci vise trose nego zene (same) u nekoj kategoriji grada? 
		// Dati primer takvih kategorija gradova i zivotnog doba. Ako ne postoji takva kombinacija zivotnog doba i kategorije grada, 
		// onda navesti zivotno doba gde je ta razlika najmanja u korist zena.
		
		Dataset<Row> zivotnaDobSamci = wallDF.filter(col("Marital_Status").equalTo(0))
                .groupBy("Age","City_Category")
                .agg(avg(when(col("Gender").equalTo("M"), col("Purchase"))).as("Muskarci Potrosnja"), 
                     avg(when(col("Gender").equalTo("F"), col("Purchase"))).as("Zene Potrosnja"))
                .withColumn("Razlika Potrosnje", col("Muskarci Potrosnja").minus(col("Zene Potrosnja")));

        Dataset<Row> zad4res = zivotnaDobSamci.orderBy("Age", "City_Category");

        System.out.println("ZADATAK 4 - REZULTAT:");
        zad4res.show();

        // Varijanta 2

        Dataset<Row> muskarciTrosenje = 
        		wallDF.filter(col("Marital_Status").equalTo(0).and(col("Gender").equalTo("M")))
        				.groupBy("Age","City_Category")
        				.agg(avg("Purchase").as("Muskarci Potrosnja"));

        Dataset<Row> zeneTrosenje = 
        		wallDF.filter(col("Marital_Status").equalTo(0).and(col("Gender").equalTo("F")))
                	.groupBy("Age","City_Category")
                	.agg(avg("Purchase").as("Zene Potrosnja"));

        
        
        Dataset<Row> samciDobTrosenje = 
        		muskarciTrosenje.join(zeneTrosenje, new String[] {"Age", "City_Category"})
                			.withColumn("Razlika Potrosnje", col("Muskarci Potrosnja").minus(col("Zene Potrosnja")))
                			.orderBy("Age", "City_Category");
        
        samciDobTrosenje.show();
        
        
        /* REZULTAT:

        +-----+-------------+------------------+-----------------+-------------------+
        |  Age|City_Category|Muskarci Potrosnja|   Zene Potrosnja|  Razlika Potrosnje|
        +-----+-------------+------------------+-----------------+-------------------+
        | 0-17|            A| 9655.655423883318|7826.252246026262|  1829.403177857056|
        | 0-17|            B|  8946.03023255814|8846.238338658148|  99.79189389999192|
        | 0-17|            C| 9365.363024544735|8313.388701110574| 1051.9743234341604|
        |18-25|            A| 9044.066666666668|8558.911988180667|  485.1546784860002|
        |18-25|            B| 9401.546168254095|8065.421820303384| 1336.1243479507111|
        |18-25|            C| 9873.842563334996|9105.567637155873|  768.2749261791232|
        |26-35|            A| 9080.252034084662|8590.171803081848|  490.0802310028139|
        |26-35|            B| 9429.548563838618|8463.337348538846|  966.2112152997724|
        |26-35|            C|10005.376382866636|8909.458704988438| 1095.9176778781984|
        |36-45|            A| 9397.794833433074|8778.596976793699|  619.1978566393755|
        ...
        */
     

	}
	
	public static void main(String[] args) {
	    
	
		// Create SparkSession
		SparkSession spark = SparkSession.builder()
						.appName("Wallmart")
						.config("spark.cleaner.ttl", "60") // clean objects after 60s
					    .config("spark.files.overwrite", "true")
					    .master("local[*]")
		                .getOrCreate();
			    spark.sparkContext().setLogLevel("OFF"); 
			   	
		 // Kreiranje Dataframe-a, tj. Dataset<Row> od csv fajla!
		Dataset<Row> wallmartDF = spark.read().option("header", "true")
		    		.csv("dataset/walmart.csv");
		
		// Zadatak 1. 
		// Kojeg su zanimanja samci koji najvise prosecno trose novca po jednoj kupovini 
		// u odredjenoj kategoriji grada i koji su to iznosi?
		zad1(wallmartDF);
		
		// Zadatak 2. 
		// Dati listu proizvoda sa informacijom u kojoj zivotnoj dobi se
		// se najvise kupuje dati proizvod i u kojoj kategoriji grada 
		zad2(wallmartDF);
		
		
		// ZADATAK 3. 
		// Navesti koje proizvode i koje kategorije proizvoda najcesce kupuju
		// zene i muskarci nekog zanimanja.
		zad3(wallmartDF);
		
		// ZADATAK 4. 
		// Da li u nekoj zivotnoj dobi, samci muskarci vise trose nego zene u nekoj kategoriji grada? 
	    // Dati primer takvih kategorija gradova i zivotnog doba. Ako ne postoji takva kombinacija zivotnog doba i kategorije grada, 
	    // onda navesti zivotno doba gde je ta razlika najmanja u korist zena.
		zad4(wallmartDF);
	
	}
}
