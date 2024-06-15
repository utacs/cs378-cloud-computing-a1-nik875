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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.time.LocalDateTime;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class MapToDataFile {
	
	
	static private Pattern pattern = Pattern.compile("[^a-zA-Z]");

	/**
	 * 
	 * @param file
	 * @param batchSize
	 * @param outputTempFile
	 */
	static void mapIt(String file, int batchSize, String outputTempFile) {

		try {
			FileInputStream fin = new FileInputStream(file);
			BufferedInputStream bis = new BufferedInputStream(fin);

			// Here we uncompress .bz2 file
			CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
			BufferedReader br = new BufferedReader(new InputStreamReader(input));

			// Initialize a bunch of variables

			StringBuilder batch = new StringBuilder("");

		

			mapToFile(batchSize, outputTempFile, br, batch);



			fin.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CompressorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	static void mapToFile(int batchSize, String outputTempFile, BufferedReader br, StringBuilder batch)
			throws IOException {
		String line;
		Map<String, Long> wordCountTmp = new HashMap<String, Long>(batchSize);
		
		Long lineCounter = 0l;
		int limit = 4000; // setting a temporary limit
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");



		// Start reading the file line by line.
		while ((line = br.readLine()) != null && lineCounter < limit) {

			lineCounter += 1;

			// add the current text line to the data batch that we want to process.
			batch.append(line);

			String[] fields = line.split(",");
			Float total_amount = 0.0f;
			
			try {
				LocalDateTime pickup_datetime = LocalDateTime.parse(fields[2], formatter);
				LocalDateTime dropoff_datetime  = LocalDateTime.parse(fields[3], formatter);

				Long trip_time_in_secs = Long.parseLong(fields[4]);
				long durationInSeconds = Duration.between(pickup_datetime, dropoff_datetime).getSeconds();
				if (durationInSeconds + 1 < trip_time_in_secs && durationInSeconds - 1 > trip_time_in_secs) {
				 System.out.println("Time inconsistencies detected");
					throw new IllegalArgumentException();
				}

				Float trip_distance = Float.parseFloat(fields[5]);

				Float pickup_longitude = Float.parseFloat(fields[6]);
				Float pickup_latitude = Float.parseFloat(fields[7]);
				Float dropoff_longitude = Float.parseFloat(fields[8]);
				Float dropoff_latitude = Float.parseFloat(fields[9]);
				if(pickup_latitude == 0 || pickup_longitude == 0 || dropoff_latitude == 0 || dropoff_longitude == 0) {
					System.out.println("Invalid coordinates");
					throw new IllegalArgumentException();
				}

				if (!fields[10].equals("CSH") && !fields[10].equals("CRD")) {
					System.out.println("Invalid payment type");
					throw new IllegalArgumentException();
				}

				Float fair_amount = Float.parseFloat(fields[11]);
				Float surcharge = Float.parseFloat(fields[12]);
				Float mta_tax = Float.parseFloat(fields[13]);
				Float tip_amount = Float.parseFloat(fields[14]);
				Float tolls_amount = Float.parseFloat(fields[15]);
				total_amount = Float.parseFloat(fields[16]);
			} catch (NumberFormatException e) { // check if expected fields are floats.
				System.out.println("Float error");
				System.out.println(lineCounter);
				System.out.println(line);
			} catch (DateTimeParseException e) { // check if expected fields are DateTime.
				System.out.println("DateTime error");
				System.out.println(lineCounter);
				System.out.println(line);
			} catch (IllegalArgumentException e) {
				System.out.println(lineCounter);
				System.out.println(line);
			}
			

			if (lineCounter % batchSize == 0) {
				wordCountTmp = MapToDataFile.processLine(batch.toString());
				// System.out.println(lineCounter + "  Pages processed!");

				// We can write the map into disk and read it back if it is too big.
				// System.out.println(lineCounter + " Pages processed! ");
				MapToDataFile.appendToTempFile(wordCountTmp, outputTempFile);

				// reset the batch to empty string and restart.
				batch = new StringBuilder("");

			}
		}
	}

	/**
	 * 
	 * @param input
	 * @return
	 */

	public static Map<String, Long> processLine(String input) {

		String[] lines = input.split("\\R");
		
		Map<String, Long> wordCount = Arrays.stream(lines).flatMap(line -> Arrays.stream(line.trim().split(" "))) // split by space
				.filter(word -> !pattern.matcher(word).find()) //
				.map(word -> word.toLowerCase().trim()) // Drop all words with special chars and convert it to lower case.
				.filter(word -> !word.isEmpty()) // Drop all empty words
				.map(word -> new SimpleEntry<>(word, 1))
				.collect(Collectors.groupingBy(SimpleEntry::getKey, Collectors.counting()));

		return wordCount;

	}

	public static void appendToTempFile(Map<String, Long> data, String outputFile) {

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


	public static void sortData(String inputFile, String outputFile) {
        Map<String, Float> lineToAmountMap = new HashMap<>();
        PriorityQueue<Map.Entry<String, Float>> priorityQueue = new PriorityQueue<>(Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()));

        try {
            FileInputStream fin = new FileInputStream(inputFile);
            BufferedInputStream bis = new BufferedInputStream(fin);
            CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
            BufferedReader br = new BufferedReader(new InputStreamReader(input));

            String line;
            while ((line = br.readLine()) != null) {
                try {
                    Float totalAmount = Float.parseFloat(line.split(",")[16]);
                    lineToAmountMap.put(line, totalAmount);
                } catch (NumberFormatException e) {
                    System.err.println("Skipping line due to parse error: " + line);
                }
            }

            priorityQueue.addAll(lineToAmountMap.entrySet());

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
                while (!priorityQueue.isEmpty()) {
                    Map.Entry<String, Float> entry = priorityQueue.poll();
                    bw.write(entry.getKey());
                    bw.newLine();
                }
            }

            fin.close();
            br.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CompressorException e) {
            e.printStackTrace();
        }
    }

    public static void displayFirst10Lines(String outputFile) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(outputFile)))) {
            for (int i = 0; i < 10; i++) {
                String line = br.readLine();
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	

}
