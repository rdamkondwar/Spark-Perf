package edu.wisc.streaming.util;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class RandomWordReceiver extends Receiver<String> {

	private RandomWordGenerator rwg;
	// private int MAX_WORD_COUNT = 5000;

	public RandomWordReceiver() {
		super(StorageLevel.MEMORY_AND_DISK_2());
		rwg = new RandomWordGenerator();
	}

	private static final long serialVersionUID = 1L;

	@Override
	public void onStart() {
		new Thread() {
			@Override
			public void run() {
				try {
					receive();
				}
				catch(Exception e) {
					System.out.println("Caught exception in onStart()");
				}
			}
		}.start();
	}

	@Override
	public void onStop() {
		// Do nothing.
	}

	private void receive() throws InterruptedException {
		// int count = 0;
		try {
			while (!isStopped()) {
				String randomStr = this.rwg.generateString();
				// System.out.println("Received data '" + randomStr + "'");
				store(randomStr);
				// count++;
			}
		} 
		catch (Exception e) {
			// Try sleeping and then retrying...
			Thread.sleep(100);
		}
		
		restart("Restarting after a small sleep");
	}
}
