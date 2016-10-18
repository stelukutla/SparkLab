import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


/**
 * Created by Neel on 10/18/16.
 *
 *  Self-Contained Applications
 *   Step 1) Download Spark from Spark Portal and extract to some folder.
 *          http://spark.apache.org/downloads.html
 *   Step 2) Prepare jar file by running maven package for his project
 *
 *   Step 3) Get the jar file location then open the command prompt at spark location and run the below command
 *      bin/spark-submit --class HelloSpark  --master local[4] /Users/Neel/MasterWorkspace/SparkLab/target/SparkLab-1.0.jar
 *
 *   Step 4) Verify the results.
 *
 *   Step 5) In order change the log level go the folder spark-2.0.1-bin-hadoop2.7/conf then copy log4j.properties.template as log4j.properties
 *
 *   Step 6) Open the file log4j.properties and change the log level as needed.
 *
 */


public class HelloSpark {
     public static void main(String[] args) {
            System.out.println("******* Hello Spark **************");
            String logFile = "/Users/Neel/MasterWorkspace/SparkLab/src/main/java/HelloWorld.java"; // Should be some file on your system
            SparkConf conf = new SparkConf().setAppName("Simple Application");
            JavaSparkContext sc = new JavaSparkContext(conf);
            JavaRDD<String> logData = sc.textFile(logFile).cache();

            long numAs = logData.filter(new Function<String, Boolean>() {
                public Boolean call(String s) { return s.contains("a"); }
            }).count();

            long numBs = logData.filter(new Function<String, Boolean>() {
                public Boolean call(String s) { return s.contains("b"); }
            }).count();

            System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
        }

    }

