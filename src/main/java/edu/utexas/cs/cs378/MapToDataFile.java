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
import java.util.Collections;
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

class RunBatchSort implements Runnable {
  private String file;
  private String outputDir;
  private int batchSize;
  private BufferedReader br;
  private Lock lock;
  private int[] batchesWritten;
  public RunBatchSort(String file, String outputDir, int batchSize,
                      BufferedReader br, Lock lock, int[] batchesWritten) {
    super();
    this.file = file;
    this.outputDir = outputDir;
    this.batchSize = batchSize;
    this.br = br;
    this.lock = lock;
    this.batchesWritten = batchesWritten;
  }
  @Override
  public void run() {
    // Philosophy: single, serial decompression, multithreaded sorting/diskwrite
    try {

      while (true) {
        lock.lock(); // Thread synchronization for finding start positions
        long startTime = System.currentTimeMillis();
        // Read without error checking
        ArrayList<String> data = MapToDataFile.readData(br, batchSize);

        if (data.size() == 0) { // If we're done
          lock.unlock();        // Free lock so other threads can also finish
          break;                // Exit loop and end thread
        } else if (data.size() < batchSize) { // If we're almost done
          System.out.println("Last batch with " + data.size() + " lines");
        }
        int thisBatch = ++batchesWritten[0];
        lock.unlock(); // Free the lock so another thread can read

        // Check errors, sort data, output partial sort to new file
        MapToDataFile.sortData(data, outputDir, String.valueOf(thisBatch));
        double duration = (System.currentTimeMillis() - startTime) / 1000.0;
        System.out.println("Wrote batch " + thisBatch + " in " + duration +
                           "s");
      }
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}

class RunMergeSort implements Runnable {
  private ArrayList<File[]> inputs;
  private File outputDir;
  int batchSize;
  private BufferedWriter outbuf = null;
  public RunMergeSort(ArrayList<File[]> inputs, File outputDir, int batchSize) {
    super();
    this.inputs = inputs;
    this.outputDir = outputDir;
    this.batchSize = batchSize;
  }

  @Override
  public void run() {
    // Copy over anything we don't need to merge
    for (int i = inputs.size() - 1; i >= 0; i--) {
      if (inputs.get(i).length == 1) {
        File f = inputs.get(i)[0];
        String newPath =
            f.getName().substring(0, f.getName().length() - 4) + "_.txt";
        inputs.get(i)[0].renameTo(new File(outputDir, newPath));
        System.out.println("Moved " + inputs.get(i)[0].getPath());
        inputs.remove(i);
      }
    }
    if (inputs.size() == 0 ||
        inputs.get(0).length == 0) // If there's no files to merge
      return;
    ArrayList<ArrayList<String>> batches = // Initialize batches container
        new ArrayList<>(inputs.get(0).length);
    // Traverse each batch in reverse order without actually reversing batch
    int[] posAlongBatch = new int[inputs.get(0).length];
    // Store the FileInputStreams so they can be closed later
    FileInputStream[] fins = new FileInputStream[inputs.get(0).length];
    // Store the bufferedreaders so we can read from each file
    BufferedReader[] brs = new BufferedReader[inputs.get(0).length];
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    try {
      for (File[] toMerge : inputs) { // Iterate over file sets
        batches.clear();
        boolean[] bufferCompleted = new boolean[toMerge.length];
        // Create the InputStreams for these specific files
        for (int i = 0; i < toMerge.length; i++) {
          fins[i] = new FileInputStream(toMerge[i]);
          BufferedInputStream bis = new BufferedInputStream(fins[i]);
          brs[i] = new BufferedReader(new InputStreamReader(bis));
          batches.add(i, new ArrayList<String>());
          posAlongBatch[i] = 0;
        }

        // Small buffer to compare values in
        String[] buffer = new String[toMerge.length];
        // Values to compare
        float[] scores = new float[toMerge.length];
        // Buffer slots to refill with new data
        ArrayList<Integer> toReplace = new ArrayList<>(toMerge.length);
        // Iterate while any buffer slot has contents
        for (int hasContents = toMerge.length; hasContents > 0;) {

          // Find highest score and write that line to outfile
          int selectedIdx = -1;
          for (int i = 0; i < buffer.length; i++)
            if (buffer[i] == null)
              toReplace.add(i);
            else if (scores[i] > (selectedIdx >= 0 ? scores[selectedIdx] : -1))
              selectedIdx = i;
          if (selectedIdx >= 0) {
            toReplace.add(selectedIdx); // Pop off buffer
            String newPath = "";
            for (File i : toMerge)
              newPath +=
                  i.getName().substring(0, i.getName().length() - 4) + "_";
            newPath += ".txt";
            appendToOutfile(buffer[selectedIdx], new File(outputDir, newPath));
          }

          // For everything that was removed from the buffer
          for (int i : toReplace) {
            if (!bufferCompleted[i] &&
                posAlongBatch[i] ==
                    batches.get(i)
                        .size()) { // If we need to load more from disk
              batches.get(i).clear();
              ArrayList<String> batch = // Read the data from the file
                  MapToDataFile.readData(brs[i], batchSize);
              if (batch.size() == 0) {     // If there's nothing more to be read
                bufferCompleted[i] = true; // Don't return to this block
                buffer[i] = null;
                hasContents--; // Subtract hasContents (only runs once)
              } else {         // If we read in new data
                               // Add all to batches buffer and update pos
                batches.get(i).addAll(batch);
                posAlongBatch[i] = 0;
              }
            }
            if (!bufferCompleted[i]) { // If this file isn't completed
              // Add the string line to buffer and parse to get score
              buffer[i] = batches.get(i).get(posAlongBatch[i]++);
              scores[i] = MapToDataFile.processLine(buffer[i], formatter);
            }
          }
          toReplace.clear(); // Reset clear array
        }

        for (int i = 0; i < fins.length; i++) // Close all files
          fins[i].close();
        finishWrite();

        String toPrint = "Merged ";
        for (File f : toMerge) {
          toPrint += f.getPath() + ", ";
        }
        System.out.println(toPrint);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void appendToOutfile(String s, File fname) {
    try {
      if (outbuf == null)
        outbuf = new BufferedWriter(new FileWriter(fname, true));
      outbuf.write(s);
      outbuf.newLine();

      outbuf.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void finishWrite() {
    try {
      outbuf.flush();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        // always close the writer
        outbuf.close();
        outbuf = null;
      } catch (Exception e) {
      }
    }
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
  static int mapIt(String file, int batchSize, String outputDir, int threads) {
    try {
      initDir(outputDir);
    } catch (FileNotFoundException e) {
      return 0;
    }
    Lock lock = new ReentrantLock();
    Thread[] all_threads = new Thread[threads];
    try {
      FileInputStream fin = new FileInputStream(file);
      BufferedInputStream bis = new BufferedInputStream(fin);
      // Here we uncompress .bz2 file
      BZip2CompressorInputStream input = new BZip2CompressorInputStream(bis);
      BufferedReader br = new BufferedReader(new InputStreamReader(input));
      int[] batchesWritten = {0};
      for (int i = 0; i < threads; i++) {
        Runnable task = new RunBatchSort(file, outputDir, batchSize, br, lock,
                                         batchesWritten);
        all_threads[i] = new Thread(task);
        all_threads[i].start();
      }
      for (Thread t : all_threads)
        t.join();
      fin.close();
      return 1;
    } catch (Exception e) {
      return 0;
    }
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

    clearDir(outputDir); // Delete all contents of the directory
  }

  public static void clearDir(String dir) {
    File directory = new File(dir);
    File[] files = directory.listFiles();
    if (files != null) {
      for (File f : files) {
        boolean deleted = f.delete();
        if (!deleted)
          System.out.println("Failed to delete file: " + f.getAbsolutePath());
      }
    }
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

  public static ArrayList<String> readData(BufferedReader br, int batchSize)
      throws IOException {
    ArrayList<String> result = new ArrayList<>(batchSize);
    String line;
    for (int lineCounter = 0;
         lineCounter < batchSize && (line = br.readLine()) != null;
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
  public static void sortData(ArrayList<String> data, String outputDir,
                              String outfileHead) throws IOException {
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    Map<String, Float> lineToAmountMap = new HashMap<>();
    PriorityQueue<Map.Entry<String, Float>> priorityQueue = new PriorityQueue<>(
        Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()));

    for (String line : data) {
      try {
        lineToAmountMap.put(line, MapToDataFile.processLine(line, formatter));
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

  public static boolean externalMergeSort(String dirA, String dirB,
                                          String outfile, int batchSize,
                                          int jobs, int batchesPerJob) {
    int origBatchesPerJob = batchesPerJob;
    try {
      // First dir has initial sort output, second dir must be empty
      initDir(dirB);
      File[] dirs = {new File(dirA), new File(dirB)}; // Flip flop between dirs
      String[] fps = {dirA, dirB};
      int current = 0;
      int remainingFiles = dirs[current].listFiles().length;
      while (batchesPerJob > 2 && remainingFiles < jobs * batchesPerJob)
        System.out.println("Shrank batchesPerJob to " + --batchesPerJob);
      while (jobs > 1 && remainingFiles < jobs * batchesPerJob)
        System.out.println("Shrank jobs to " + --jobs);

      // Iterate over all files in current dir, flipping dir each time
      for (File[] files = dirs[current].listFiles();
           files.length >= jobs * batchesPerJob;
           files = dirs[current].listFiles()) {
        // Condition: there are enough files to submit batchesPerJob to jobs
        // Break the files into chunks for each job to tackle
        ArrayList<ArrayList<File[]>> workPerJob = new ArrayList<>(jobs);
        for (int i = 0; i < jobs; i++) {
          workPerJob.add(new ArrayList<File[]>());
        }
        // For each set of batchesPerJob files, append to a job's workload
        int currentJob = 0;
        for (int i = 0; i < files.length; i += batchesPerJob) {
          int endpoint = i + batchesPerJob < files.length ? i + batchesPerJob
                                                          : files.length;
          workPerJob.get(currentJob)
              .add(Arrays.copyOfRange(files, i, endpoint));
          currentJob = ++currentJob % jobs;
        }
        Thread[] threads = new Thread[jobs];
        for (int i = 0; i < jobs; i++) {
          Runnable task = new RunMergeSort(workPerJob.get(i),
                                           dirs[(current + 1) % 2], batchSize);
          threads[i] = new Thread(task);
          threads[i].start();
        }
        for (int i = 0; i < threads.length; i++)
          threads[i].join(); // Wait for all to finish
        clearDir(fps[current]);
        current = (current + 1) % 2;

        File[] origFiles = dirs[current].listFiles();
        remainingFiles = origFiles.length;
        // Rename the files nicely
        for (int i = 0; i < remainingFiles; i++) {
          origFiles[i].renameTo(
              new File(dirs[current], String.valueOf(i) + ".txt"));
        }
        // Dynamically shrink jobs, batchesPerJob
        if (remainingFiles == 1)
          break;
        while (batchesPerJob > 2 && remainingFiles < jobs * batchesPerJob)
          System.out.println("Shrank batchesPerJob to " + --batchesPerJob);
        while (jobs > 1 && remainingFiles < jobs * batchesPerJob) {
          System.out.println("Shrank jobs to " + --jobs + " and reset batchesPerJob");
	  batchesPerJob = origBatchesPerJob;
          while (batchesPerJob > 2 && remainingFiles < jobs * batchesPerJob)
            System.out.println("Shrank batchesPerJob to " + --batchesPerJob);
	}
      }
      File[] leftoverFiles = dirs[current].listFiles();
      leftoverFiles[0].renameTo(new File(outfile));
      displayFirst10Lines(outfile);
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
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
