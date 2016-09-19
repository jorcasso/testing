package com.kii.test;

import com.kii.test.util.Site;

public class TestStart {

	public static void main(String[] args) throws Exception {
		Site site = Site.valueOf(args[1]);
		int threads = args.length >= 3 ? Integer.valueOf(args[2]) : 1;
		String bucketType = args.length >= 4 ? args[3] : "rw";
		String bucketID = args.length >= 5 ? args[4] : "perfBucketTest";

		if (args.length == 0 || args[0].equals("createObjects")) {
			TestCreateObjects.run(site, threads, bucketType, bucketID);
		} else if (args[0].equals("updateObjects")) {
			TestUpdateObjects.run(site, threads, bucketType, bucketID);
		} else if (args[0].equals("updateStateREST")) {
			TestUpdateStateREST.run(site, threads);
		} else if (args[0].equals("updateStateMQTT")) {
			TestUpdateStateMQTT.run(site, threads);
		} else {
			System.out.println("Unknown argument " + args[0]);
		}
	}

}
