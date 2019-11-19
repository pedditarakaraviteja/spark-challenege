package edu.nwmissouri.Tarakapeddi;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import java.util.Comparator;

/**
 * Hello world!
 */
public final class App {
  private App() {
  }

  private static void process(String fileName) {

    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Challenge");

    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    
    JavaRDD<String> inputFile = sparkContext.textFile(fileName);

    
    JavaRDD<String> wordsFromFile = inputFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator());


    JavaPairRDD<String, Integer> countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1))
        .reduceByKey((x, y) -> (int) x + (int) y);



    JavaPairRDD<Integer, String> output = countData.mapToPair(p -> new Tuple2(p._2, p._1))
        .sortByKey(Comparator.reverseOrder());


    String outputFolder = "countdata";
    Path path = FileSystems.getDefault().getPath(outputFolder);
    FileUtils.deleteQuietly(path.toFile());
    output.saveAsTextFile(outputFolder);

    sparkContext.close();
  }

  /**
   * Says hello to the world.
   * 
   * @param args The arguments of the program.
   */
  public static void main(String[] args) {
    // System.out.println("Hello World!");

    if (args.length != 1) {
      System.out.println("Please provide a single argument (text file name).");
      System.exit(0);
    }
    // call wordCount with the first argument
    process(args[0]);

  }
}

