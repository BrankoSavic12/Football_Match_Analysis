package rs.raf.pds.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.api.java.function.FilterFunction;

public class SimpleSpark {
	
	public static void main(String[] args) {
		
		// NAVESTI TACNU PUTANJU DO FAJLA NOTES, ili nekog drugog tekstualnog fajla
		String testFile = "D:/Football_Match_Analysis/dataset/notes.txt";
		
	    SparkSession spark = SparkSession.builder().appName("Simple Spark App").master("local[*]").getOrCreate();
	    spark.sparkContext().setLogLevel("OFF"); 
	    Dataset<String> testData = spark.read().textFile(testFile).cache();
	
	    long numAs = testData.filter((FilterFunction<String>)s -> s.contains("Java")).count();
	    long numBs = testData.filter((FilterFunction<String>)s -> s.contains("the")).count();
	
	    System.out.println("\n-----------------------\n");
	    System.out.println("Lines with 'Java': " + numAs + ", lines with 'the': " + numBs);
	
	    spark.close();
	    //spark.sparkContext().stop();
	}
}