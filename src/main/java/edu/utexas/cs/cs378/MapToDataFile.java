package edu.utexas.cs.cs378;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Runnable;
import java.lang.Thread;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

class MyRunnable implements Runnable {
  private String file;
  private String outputDir;
  private int batchSize;
  private long[] startpos;
  private Lock lock;
  public MyRunnable(String file, String outputDir, int batchSize,
                    long[] startpos, Lock lock) {
    super();
    this.file = file;
    this.outputDir = outputDir;
    this.batchSize = batchSize;
    this.startpos = startpos;
    this.lock = lock;
  }
  @Override
  public void run() {
    MapToDataFile.createThread(file, outputDir, batchSize, startpos, lock);
  }
}

public class MapToDataFile {

  static private Pattern pattern = Pattern.compile("[^a-zA-Z]");

  /**
   *
   * @param file
   * @param batchSize
   * @param outputDir
   */
  static boolean mapIt(String file, int batchSize, String outputDir,
                       int threads) {
    try {
      initDir(outputDir);
    } catch (FileNotFoundException e) {
      return false;
    }
    Lock lock = new ReentrantLock();
    long[] startpos = {0};
    Thread[] all_threads = new Thread[threads];
    try {
      for (int i = 0; i < threads; i++) {
        Runnable task =
            new MyRunnable(file, outputDir, batchSize, startpos, lock);
        all_threads[i] = new Thread(task);
        all_threads[i].start();
      }
      for (Thread t : all_threads)
        t.join();
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  public static void initDir(String outputDir) throws FileNotFoundException {
    // Check if the directory exists
    File directory = new File(outputDir);
    if (!directory.exists()) {
      // Create the directory if it doesn't exist
      boolean created = directory.mkdirs();
      if (created) {
        System.out.println("outputDir created");
      } else {
        System.out.println("Failed to create outputDir!");
        throw new FileNotFoundException();
      }
    } else if (!directory.isDirectory()) {
      boolean deleted = directory.delete();
      if (!deleted) {
        System.out.println("Failed to delete file: " +
                           directory.getAbsolutePath());
        throw new FileNotFoundException();
      }
      boolean created = directory.mkdirs();
      if (created) {
        System.out.println("outputDir created");
      } else {
        System.out.println("Failed to create outputDir!");
        throw new FileNotFoundException();
      }
    }

    // Delete all contents of the directory
    File[] files = directory.listFiles();
    if (files != null) {
      for (File f : files) {
        boolean deleted = f.delete();
        if (!deleted) {
          System.out.println("Failed to delete file: " + f.getAbsolutePath());
          throw new FileNotFoundException();
        }
      }
    }
  }

  public static void createThread(String file, String outputDir, int batchSize,
                                  long[] startpos, Lock lock) {
    try {
      FileInputStream fin = new FileInputStream(file);
      BufferedInputStream bis = new BufferedInputStream(fin);

      // Here we uncompress .bz2 file
      BZip2CompressorInputStream input = new BZip2CompressorInputStream(bis);

      long myLastPos = 0;
      while (true) {
        lock.lock(); // Thread synchronization for finding start positions
        if (startpos[0] == -1) // If we're done
          break;
        input.skip(startpos[0] - myLastPos); // Seek to the new startpos
        myLastPos = startpos[0]; // Set myLastPos since skip() is cumulative
        // New bufferedreader every time with a custom start position
        BufferedReader br = new BufferedReader(new InputStreamReader(input));
        // Read without error checking
        ArrayList<String> data = readData(br, batchSize);

        long totalBytes = 0;
        if (data.size() < batchSize) // If we're done
          startpos[0] = -1;
        else { // If not done, count bytes and update startpos
          for (String s : data)
            totalBytes += s.getBytes().length;

          startpos[0] += totalBytes;
        }
        lock.unlock(); // Free the lock so another thread can read

        // Check errors, sort data, output partial sort to new file
        mapToFile(data, outputDir, String.valueOf(myLastPos));
        // Consider that the cursor has also moved during read
        myLastPos += totalBytes;
      }
      fin.close(); // Once we're done, close file
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static ArrayList<String> readData(BufferedReader br, int batchSize)
      throws IOException {
    ArrayList<String> result = new ArrayList<>(batchSize);
    String line;
    for (int lineCounter = 0;
         (line = br.readLine()) != null && lineCounter < batchSize;
         lineCounter++) {
      result.add(line);
    }
    return result;
  }

  /**
   *
   * @param batchSize
   * @param outputTempFile
   * @param br
   * @param batch
   * @param lineCounter
   * @throws IOException
   */
  static void mapToFile(ArrayList<String> data, String outputDir,
                        String outfileHead) throws IOException {
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    Map<String, Float> lineToAmountMap = new HashMap<>();
    PriorityQueue<Map.Entry<String, Float>> priorityQueue = new PriorityQueue<>(
        Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()));

    for (String line : data) {
      try {
        lineToAmountMap.put(line, processLine(line, formatter));
      } catch (                      // Retain the original code to catch errors
          NumberFormatException e) { // check if expected fields are floats.
        System.out.println("Float error");
        System.out.println(line);
      } catch (
          DateTimeParseException e) { // check if expected fields are DateTime.
        System.out.println("DateTime error");
        System.out.println(line);
      } catch (IllegalArgumentException e) {
        System.out.println(line);
      }
    }

    priorityQueue.addAll(lineToAmountMap.entrySet());

    writeBatch(priorityQueue, outputDir, outfileHead);
  }

  /**
   *
   * @param line
   * @param formatter
   * @return
   */
  public static float processLine(String line, DateTimeFormatter formatter) {
    // Retain original parsing code to catch improperly formatted lines
    String[] fields = line.split(",");
    if (fields.length != 17) {
      System.out.println("Incorrect number of fields");
      throw new IllegalArgumentException();
    }
    LocalDateTime pickup_datetime = LocalDateTime.parse(fields[2], formatter);
    LocalDateTime dropoff_datetime = LocalDateTime.parse(fields[3], formatter);

    Long trip_time_in_secs = Long.parseLong(fields[4]);
    long durationInSeconds =
        Duration.between(pickup_datetime, dropoff_datetime).getSeconds();
    if (durationInSeconds + 1 < trip_time_in_secs &&
        durationInSeconds - 1 > trip_time_in_secs) {
      System.out.println("Time inconsistencies detected");
      throw new IllegalArgumentException();
    }

    Float trip_distance = Float.parseFloat(fields[5]);

    Float pickup_longitude = Float.parseFloat(fields[6]);
    Float pickup_latitude = Float.parseFloat(fields[7]);
    Float dropoff_longitude = Float.parseFloat(fields[8]);
    Float dropoff_latitude = Float.parseFloat(fields[9]);
    //    if (pickup_latitude == 0 || pickup_longitude == 0 ||
    //        dropoff_latitude == 0 || dropoff_longitude == 0) {
    //      System.out.println("Invalid coordinates");
    //      throw new IllegalArgumentException();
    //    }

    if (!fields[10].equals("CSH") && !fields[10].equals("CRD") &&
        !fields[10].equals("UNK")) {
      System.out.println("Invalid payment type");
      throw new IllegalArgumentException();
    }

    Float fair_amount = Float.parseFloat(fields[11]);
    Float surcharge = Float.parseFloat(fields[12]);
    Float mta_tax = Float.parseFloat(fields[13]);
    Float tip_amount = Float.parseFloat(fields[14]);
    Float tolls_amount = Float.parseFloat(fields[15]);
    float total_amount = Float.parseFloat(fields[16]);
    return total_amount;
  }

  public static void
  writeBatch(PriorityQueue<Map.Entry<String, Float>> priorityQueue,
             String outputDir, String outfileHead) {
    String outputFile = outputDir + "/" + outfileHead + ".txt";
    File file = new File(outputFile);
    BufferedWriter bf = null;

    try {
      // create new BufferedWriter for the output file, true means append
      bf = new BufferedWriter(new FileWriter(file, false));
      while (!priorityQueue.isEmpty()) {
        Map.Entry<String, Float> entry = priorityQueue.poll();
        bf.write(entry.getKey());
        bf.newLine();
      }
      bf.flush();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {

      try {
        // always close the writer
        bf.close();
      } catch (Exception e) {
      }
    }
  }

  public static void appendToTempFile(Map<String, Long> data,
                                      String outputFile) {

    // new file object
    File file = new File(outputFile);
    BufferedWriter bf = null;

    try {

      // create new BufferedWriter for the output file
      // true means append
      bf = new BufferedWriter(new FileWriter(file, true));

      // iterate map entries
      for (Map.Entry<String, Long> entry : data.entrySet()) {

        // put key and value separated by a comma
        // better use a string builder.
        // better use a data serializiation library

        bf.write(entry.getKey() + "," + entry.getValue());

        // new line
        bf.newLine();
      }

      bf.flush();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {

      try {
        // always close the writer
        bf.close();
      } catch (Exception e) {
      }
    }
  }

  public static void displayFirst10Lines(String outputFile) {
    try (BufferedReader br = new BufferedReader(
             new InputStreamReader(new FileInputStream(outputFile)))) {
      for (int i = 0; i < 10; i++) {
        String line = br.readLine();
        System.out.println(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
