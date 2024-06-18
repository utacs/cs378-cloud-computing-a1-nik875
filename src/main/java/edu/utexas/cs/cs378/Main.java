package edu.utexas.cs.cs378;

import java.util.Map;

public class Main {

  /**
   * A main method to run examples.
   *
   * @param args not used
   */
  public static void main(String[] args) {

    // pass the file name as the first argument.
    // We can also accept a .bz2 file

    // This line is just for Kia :)
    // You should pass the file name and path as first argument of this main
    // method.
    String file =
        "/Users/ayaannazir/Desktop/CC/taxi-data-sorted-small.csv.bz2";
    String outputFile = "SORTED-FILE-RESULT.txt";

    if (args.length > 0)
      file = args[0];

    int batchSize = 1_000_000;

    if (args.length > 1)
      batchSize = Integer.parseInt(args[1]);

    String outputDir = "processedBatches";

    boolean success = MapToDataFile.mapIt(file, batchSize, outputDir, 1);
    if (!success)
      System.out.println("Unhandled exception in MapToDataFile!");

    //		MapToDataFile.sortData(file, outputFile);
    //        MapToDataFile.displayFirst10Lines(outputFile);
  }
}
