package com.kii.test.util;

import java.util.List;

public class LogUtil {

	public static void logMaxTime(List<Long> singleTimes) {
		long max = singleTimes.stream().max((o1, o2) -> o1.compareTo(o2)).get();
		System.out.println("Max: " + max + " ms");
	}

	public static void logMinTime(List<Long> singleTimes) {
		long min = singleTimes.stream().min((o1, o2) -> o1.compareTo(o2)).get();
		System.out.println("Min: " + min + " ms");
	}

	public static void logAvgTime(List<Long> singleTimes) {
		double total = 0;

		for (long singleTime : singleTimes) {
			total += singleTime;
		}

		double avg = total / singleTimes.size();
		System.out.println("Avg: " + avg + " ms");
	}

	public static void logMaxDelay(List<Long> delayTimes) {
		long max = delayTimes.stream().max((o1, o2) -> o1.compareTo(o2)).get();
		System.out.println("Delay Max: " + max + " ms");
	}

	public static void logMinDelay(List<Long> delayTimes) {
		long min = delayTimes.stream().min((o1, o2) -> o1.compareTo(o2)).get();
		System.out.println("Delay Min: " + min + " ms");
	}

	public static void logAvgDelay(List<Long> delayTimes) {
		double total = 0;

		for (long singleTime : delayTimes) {
			total += singleTime;
		}

		double avg = total / delayTimes.size();
		System.out.println("Delay Avg: " + avg + " ms");
	}
}
