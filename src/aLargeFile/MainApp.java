package aLargeFile;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.SystemUtils;

public class MainApp {

	static String FILE_NAME;
	static String WORKING_DIRECTORY;
	static String SPLIT_PREFIX;
	static int SPLIT_PERLINE;
	static boolean TIME_TAKEN;

	public static void main(String[] args) {
		loadConfig();
		long startTime = 0;
		if (TIME_TAKEN) {
			startTime = System.nanoTime();
		}

		// split book into multiple files and get total count of files created
		int fc = splitBook();
		if (fc == -1) {
			// failed to split into files so end program
			System.exit(1);
		}

		BlockingQueue<File> bq = new ArrayBlockingQueue<File>(fc);

		Producer producer = new Producer(bq);
		// start producer process
		producer.start();

		// create thread safe file count and set the amount of files that will
		// be
		// read and analysed
		AtomicInteger fileCount = new AtomicInteger();
		fileCount.set(fc);

		ConcurrentHashMap<String, AtomicInteger> words = new ConcurrentHashMap<String, AtomicInteger>();
		ExecutorService executorService = Executors.newFixedThreadPool(4);

		// create 4 consumer threads
		for (int i = 0; i < 4; i++) {
			executorService.execute(new Consumer(bq, fileCount, words));
		}

		executorService.shutdown();
		// wait until consumer threads terminate or timeout in 5 minutes
		try {
			if (!executorService.awaitTermination(5, TimeUnit.MINUTES)) {
				System.out.println("Threads didn't finish in 5 minutes!");
				System.exit(1);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		printTopCommonWords(words);
		cleanUp();
		if (TIME_TAKEN) {
			long endTime = System.nanoTime();
			long duration = (endTime - startTime);

			System.out.println("Time taken: " + (duration / 1000000000.0) + " seconds");
		}
	}

	private static void loadConfig() {
		Configurations configs = new Configurations();
		try {
			Configuration config = configs.properties(new File("resources/config.properties"));
			// access configuration properties
			String workingDirectory = config.getString("workingDirectory", "/home/tanzim/Documents/tanzim_tmp/");
			String fileName = config.getString("fileName", "aLargeFile");
			boolean timeTaken = config.getBoolean("timeTaken", false);
			String splitPrefix = config.getString("splitPrefix");
			int splitPerLine = config.getInteger("splitPerLine", 521370);

			FILE_NAME = fileName;
			WORKING_DIRECTORY = workingDirectory;
			SPLIT_PREFIX = splitPrefix;
			TIME_TAKEN = timeTaken;
			SPLIT_PERLINE = splitPerLine;

		} catch (ConfigurationException e) {
			e.printStackTrace();
		}

	}

	public static int splitBook() {
		if (SystemUtils.IS_OS_LINUX) {
			// run bash command split
			try {
				ProcessBuilder pbSplit = new ProcessBuilder("/bin/bash", "-c",
						"/usr/bin/split -dl " + SPLIT_PERLINE + " --additional-suffix= " + FILE_NAME + " " + SPLIT_PREFIX);
				pbSplit.inheritIO();
				pbSplit.directory(new File(WORKING_DIRECTORY));
				Process pbSplitProcess = pbSplit.start();
				pbSplitProcess.waitFor();
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
			File dir = new File(WORKING_DIRECTORY);

			// return total count of files created by split
			return dir.listFiles(new FilenameFilter() {
				public boolean accept(File dir, String name) {
					return name.startsWith(SPLIT_PREFIX);
				}
			}).length;
		} else {
			try {
				File dir = new File(WORKING_DIRECTORY);
				File file = new File(WORKING_DIRECTORY + FILE_NAME);
				splitFile(file, 30);
				// return total count of files created by split
				return dir.listFiles(new FilenameFilter() {
					public boolean accept(File dir, String name) {
						return name.startsWith(SPLIT_PREFIX);
					}
				}).length;
			} catch (IOException e) {

			}
			return -1;
		}

	}

	public static ArrayList<File> splitFile(File file, int sizeOfFileInMB) throws IOException {
		int counter = 1;
		ArrayList<File> files = new ArrayList<File>();
		int sizeOfChunk = 1024 * 1024 * sizeOfFileInMB;
		String eof = System.lineSeparator();
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String line = br.readLine();
			while (line != null) {
				File newFile = new File(file.getParent(), SPLIT_PREFIX + String.format("%03d", counter++));
				try (OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile))) {
					int fileSize = 0;
					while (line != null) {
						byte[] bytes = (line + eof).getBytes(Charset.defaultCharset());
						if (fileSize + bytes.length > sizeOfChunk)
							break;
						out.write(bytes);
						fileSize += bytes.length;
						line = br.readLine();
					}
				}
				files.add(newFile);
			}
		}
		return files;
	}

	public static void printTopCommonWords(ConcurrentHashMap<String, AtomicInteger> words) {
		Integer mostFrequency = null;
		String mostFrequent = null;

		Integer secondFrequency = null;
		String secondFrequent = null;

		Integer thirdFrequency = null;
		String thirdFrequent = null;

		for (String s : words.keySet()) {
			Integer i = words.get(s).get();

			if (mostFrequency == null) {
				mostFrequency = i;
				mostFrequent = s;
			}

			if (i > mostFrequency) {
				Integer tmp_secondFrequency = mostFrequency;
				String tmp_secondFrequent = mostFrequent;

				Integer tmp_thirdFrequency = secondFrequency;
				String tmp_thirdFrequent = secondFrequent;

				mostFrequency = i;
				mostFrequent = s;

				secondFrequency = tmp_secondFrequency;
				secondFrequent = tmp_secondFrequent;

				thirdFrequency = tmp_thirdFrequency;
				thirdFrequent = tmp_thirdFrequent;
			} else if (secondFrequency == null || i > secondFrequency) {
				Integer tmp_thirdFrequency = secondFrequency;
				String tmp_thirdFrequent = secondFrequent;

				secondFrequency = i;
				secondFrequent = s;

				thirdFrequency = tmp_thirdFrequency;
				thirdFrequent = tmp_thirdFrequent;
			} else if (thirdFrequency == null || i > thirdFrequency) {
				thirdFrequency = i;
				thirdFrequent = s;
			}
		}
		StringBuilder stringBuilderFirst = new StringBuilder();
		stringBuilderFirst.append("The most frequent word is \"");
		stringBuilderFirst.append(mostFrequent);
		stringBuilderFirst.append("\" which occurred ");
		stringBuilderFirst.append(mostFrequency);
		stringBuilderFirst.append(" times\n");

		StringBuilder stringBuilderSecond = new StringBuilder();
		stringBuilderFirst.append("The second frequent word is \"");
		stringBuilderFirst.append(secondFrequent);
		stringBuilderFirst.append("\" which occurred ");
		stringBuilderFirst.append(secondFrequency);
		stringBuilderFirst.append(" times\n");

		StringBuilder stringBuilderThird = new StringBuilder();
		stringBuilderFirst.append("The third frequent word is \"");
		stringBuilderFirst.append(thirdFrequent);
		stringBuilderFirst.append("\" which occurred ");
		stringBuilderFirst.append(thirdFrequency);
		stringBuilderFirst.append(" times");

		System.out.println(stringBuilderFirst);
		System.out.println(stringBuilderSecond);
		System.out.println(stringBuilderThird);
	}

	public static void cleanUp() {
		File dir = new File(WORKING_DIRECTORY);
		File[] foundFiles = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith(SPLIT_PREFIX);
			}
		});

		for (File file : foundFiles) {
			file.delete();
		}
	}

}