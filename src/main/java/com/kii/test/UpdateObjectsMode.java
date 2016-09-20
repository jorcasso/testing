package com.kii.test;

public enum UpdateObjectsMode {
	SAME_TOKEN(false, true), SAME_BUCKET(true, false), SAME_BUCKET_AND_TOKEN(true, true), ALL_DIFFERENT(false, false);

	private boolean sameBucket;
	private boolean sameToken;

	private UpdateObjectsMode(boolean sameBucket, boolean sameToken) {
		this.sameBucket = sameBucket;
		this.sameToken = sameToken;
	}

	public boolean sameBucket() {
		return sameBucket;
	}

	public boolean sameToken() {
		return sameToken;
	}
}
