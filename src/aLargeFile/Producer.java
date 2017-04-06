package aLargeFile;

import java.io.File;
import java.io.FilenameFilter;
import java.util.concurrent.BlockingQueue;

class Producer extends Thread {
	BlockingQueue<File> bq = null;

	public Producer(BlockingQueue<File> bq) {
		this.bq = bq;

	}

	public void run() {
		File dir = new File(MainApp.WORKING_DIRECTORY);
		File[] foundFiles = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith(MainApp.SPLIT_PREFIX);
			}
		});

		//put all the smaller files into the BlockingQueue
		for (File file : foundFiles) {
			try {
				bq.put(file);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}