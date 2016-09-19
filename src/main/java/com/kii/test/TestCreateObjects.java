package com.kii.test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import com.kii.test.util.Site;
import com.kii.test.util.SiteUtil;
import com.kii.test.util.TokenUtil;

public class TestCreateObjects {

	private static final int AMOUNT = 200;
	private static final String OBJECTS_PATH_TEMPLATE = "/apps/%s/buckets/%s:%s/objects";

	private final RestTemplate restTemplate;
	private final String objectsURI;
	private final Site site;

	public static void run(Site site, int threads, String bucketType, String bucketID) throws Exception {
		new TestCreateObjects(site, threads, bucketType, bucketID);
	}

	private TestCreateObjects(Site site, int threads, String bucketType, String bucketID) throws Exception {
		this.objectsURI = SiteUtil.getUfeURI(site,
				String.format(OBJECTS_PATH_TEMPLATE, SiteUtil.getApp(site), bucketType, bucketID));
		this.site = site;

		System.setProperty("http.maxConnections", "" + threads);
		restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());

		System.out.println("Going to create " + AMOUNT + " objects in " + bucketType + ":" + bucketID + " bucket");

		if (threads > 1) {
			createObjectsMultithread(threads);
		} else {
			createObjectsSingleThread();
		}
	}

	private void createObjectsMultithread(int threads) throws Exception {
		System.out.println("Multithread (" + threads + ")");
		HttpEntity<String> requestEntity = createObjectRequestEntity(TokenUtil.getToken(site));

		ExecutorService executor = Executors.newFixedThreadPool(threads);

		try {
			List<Future<?>> futures = new LinkedList<>();

			long time1 = System.currentTimeMillis();

			for (int i = 0; i < AMOUNT; i++) {
				futures.add(executor.submit(() -> createObject(requestEntity)));
			}

			for (Future<?> f : futures) {
				f.get();
			}

			long time2 = System.currentTimeMillis();

			System.out.println("Elapsed time: " + (time2 - time1) + " ms");
		} finally {
			executor.shutdown();
		}
	}

	private void createObjectsSingleThread() {
		System.out.println("Single thread");
		HttpEntity<String> requestEntity = createObjectRequestEntity(TokenUtil.getToken(site));

		long time1 = System.currentTimeMillis();

		for (int i = 0; i < AMOUNT; i++) {
			createObject(requestEntity);
		}
		long time2 = System.currentTimeMillis();

		System.out.println("Elapsed time: " + (time2 - time1) + " ms");
	}

	private static HttpEntity<String> createObjectRequestEntity(String accessToken) {
		// Data
		String object = "{'field1': 'value1', 'field2': 'value2', 'subjson': {'subfield1': 234, 'subfield2': true}}"
				.replace("'", "\"");

		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		return new HttpEntity<String>(object, headers);
	}

	private void createObject(HttpEntity<String> requestEntity) {
		// Request / Response
		restTemplate.exchange(objectsURI, HttpMethod.POST, requestEntity, String.class);
	}

}
