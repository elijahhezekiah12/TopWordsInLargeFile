package aLargeFile;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

class Consumer extends Thread {
	BlockingQueue<File> bq = null;
	AtomicInteger fileCount;
	ConcurrentHashMap<String, AtomicInteger> words;

	public Consumer(BlockingQueue<File> bq, AtomicInteger fileCount, ConcurrentHashMap<String, AtomicInteger> words) {
		this.bq = bq;
		this.fileCount = fileCount;
		this.words = words;
	}

	public void run() {
		while (fileCount.get() > 0) {
			try {
				File file = bq.take();
				if (file != null) {
					// take the file from the BlockingQueue and decrement our file count
					fileCount.decrementAndGet();
					LineIterator lineIterator = FileUtils.lineIterator(file, "UTF-8");
					try {						
						// read the file line by line
						while (lineIterator.hasNext()) {
							String[] line_words = lineIterator.nextLine().split("\\s+");
							for (int i = 0; i < line_words.length; i++) {
								String s = line_words[i].toLowerCase();
								if (words.keySet().contains(s)) {
									words.get(s).incrementAndGet();
								} else {
									AtomicInteger newInteger = new AtomicInteger();
									newInteger.set(1);
									words.put(s, newInteger);
								}
							}
						}	
					} finally {
						LineIterator.closeQuietly(lineIterator);
					}
				}
			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}

		}
	}
}