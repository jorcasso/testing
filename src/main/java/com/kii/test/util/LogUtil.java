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
}
