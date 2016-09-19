package com.kii.test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import com.kii.test.util.Site;
import com.kii.test.util.SiteUtil;
import com.kii.test.util.TokenUtil;

public class TestUpdateObjects {

	private static final int AMOUNT = 200;
	private static final String OBJECTS_PATH_TEMPLATE = "/apps/%s/buckets/%s:%s/objects";

	private final RestTemplate restTemplate;
	private final String objectsURI;
	private final String accessToken;

	public static void run(Site site, int threads, String bucketType, String bucketID) throws Exception {
		new TestUpdateObjects(site, threads, bucketType, bucketID);
	}

	private TestUpdateObjects(Site site, int threads, String bucketType, String bucketID) throws Exception {
		this.objectsURI = String.format(OBJECTS_PATH_TEMPLATE, SiteUtil.getApp(site), bucketType, bucketID);
		this.accessToken = TokenUtil.getToken(site);

		System.setProperty("http.maxConnections", "" + threads);
		restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());

		System.out.println("Going to update " + AMOUNT + " objects in " + bucketType + ":" + bucketID + " bucket");

		if (threads > 1) {
			updateObjectMultiThread(threads);
		} else {
			updateObjectSingleThread();
		}
	}

	private void updateObjectSingleThread() throws JSONException {
		System.out.println("Single thread");

		List<String> objects = new LinkedList<>();
		List<Long> singleTimes = new LinkedList<>();

		for (int i = 0; i < AMOUNT; i++) {
			String objectID = createObject();
			objects.add(objectID);
		}

		HttpEntity<String> requestEntity = updateObjectRequestEntity("jiji");

		long time1 = System.currentTimeMillis();

		for (String objectID : objects) {

			long t1 = System.currentTimeMillis();
			updateObject(objectID, requestEntity);
			singleTimes.add(System.currentTimeMillis() - t1);
		}
		long time2 = System.currentTimeMillis();

		System.out.println("Elapsed time: " + (time2 - time1) + " ms");

		for (int i = 0; i < singleTimes.size(); i++) {
			System.out.println(String.format("Single time #%d: %d ms", i + 1, singleTimes.get(i)));
		}
	}

	private void updateObjectMultiThread(int threads) throws Exception {
		System.out.println("Multithread (" + threads + ")");

		HttpEntity<String> requestEntity = updateObjectRequestEntity("jiji");

		ExecutorService executor = Executors.newFixedThreadPool(threads);

		try {
			List<String> objects = new CopyOnWriteArrayList<>();
			List<Long> singleTimes = new CopyOnWriteArrayList<>();
			List<Future<?>> futures = new LinkedList<>();

			for (int i = 0; i < AMOUNT; i++) {
				futures.add(executor.submit(() -> {
					String objectID = createObject();
					objects.add(objectID);
					return null;
				}));
			}

			for (Future<?> f : futures) {
				f.get();
			}
			futures.clear();

			// restTemplate.setInterceptors(Arrays.asList((request, body,
			// execution) -> {
			// long time = System.currentTimeMillis();
			// System.out.println(time + " - " + (time / 1000));
			// return execution.execute(request, body);
			// }));

			long time1 = System.currentTimeMillis();

			for (String objectID : objects) {
				futures.add(executor.submit(() -> {
					long t1 = System.currentTimeMillis();
					updateObject(objectID, requestEntity);
					singleTimes.add(System.currentTimeMillis() - t1);
				}));
			}

			for (Future<?> f : futures) {
				f.get();
			}

			long time2 = System.currentTimeMillis();

			System.out.println("Elapsed time: " + (time2 - time1) + " ms");

			for (int i = 0; i < singleTimes.size(); i++) {
				System.out.println(String.format("Single time #%d: %d ms", i + 1, singleTimes.get(i)));
			}
		} finally {
			executor.shutdown();
		}
	}

	private String createObject() throws JSONException {
		// Data
		String object = "{'field1': 'value1', 'field2': 'value2', 'subjson': {'subfield1': 234, 'subfield2': true}}"
				.replace("'", "\"");

		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		HttpEntity<String> requestEntity = new HttpEntity<String>(object, headers);

		// Request / Response
		String response = restTemplate.exchange(objectsURI, HttpMethod.POST, requestEntity, String.class).getBody();

		return new JSONObject(response).getString("objectID");
	}

	private HttpEntity<String> updateObjectRequestEntity(String fieldValue) {
		// Data
		String object = ("{'field1': '" + fieldValue
				+ "', 'field2': 'value2', 'subjson': {'subfield1': 234, 'subfield2': true}}").replace("'", "\"");

		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		return new HttpEntity<String>(object, headers);
	}

	private void updateObject(String objectID, HttpEntity<String> requestEntity) {
		// Request / Response
		restTemplate.exchange(objectsURI + "/" + objectID, HttpMethod.PUT, requestEntity, String.class);
	}

}
