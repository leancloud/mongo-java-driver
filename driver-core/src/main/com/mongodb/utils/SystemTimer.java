package com.mongodb.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SystemTimer {
	private final static ScheduledExecutorService executor = Executors
			.newSingleThreadScheduledExecutor();

	private static final long tickUnit = Long.parseLong(System.getProperty(
			"mongodb.systimer.tick.mills", "5"));

	static {
		executor.scheduleAtFixedRate(new TimerTicker(), tickUnit, tickUnit,
				TimeUnit.MILLISECONDS);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				executor.shutdown();
			}
		});
	}

	private static volatile long time = System.currentTimeMillis();

	private static class TimerTicker implements Runnable {
		public void run() {
			time = System.currentTimeMillis();
		}
	}

	public static long currentTimeMillis() {
		return time;
	}

}