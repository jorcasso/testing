package com.kii.test;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
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

public class TestUpdateObjectsDifferentBuckets {

	private static final int AMOUNT = 200;
	private static final String OBJECTS_URI_TEMPLATE = "/apps/%s/buckets/%s:%s/objects";

	private final RestTemplate restTemplate;
	private final String appID;
	private final Site site;
	private final String bucketType;

	public static void run(Site site, int threads, String bucketType) throws Exception {
		new TestUpdateObjectsDifferentBuckets(site, threads, bucketType);
	}

	private TestUpdateObjectsDifferentBuckets(Site site, int threads, String bucketType) throws Exception {
		this.appID = SiteUtil.getApp(site);
		this.site = site;
		this.bucketType = bucketType;

		System.setProperty("http.maxConnections", "" + threads);
		restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());

		System.out.println("Going to update " + AMOUNT + " objects in " + bucketType + " buckets");

		if (threads > 1) {
			updateObjectMultiThread(threads);
		} else {
			updateObjectSingleThread();
		}
	}

	private void updateObjectSingleThread() throws JSONException {
		System.out.println("Single thread");

		List<String> objectPaths = new LinkedList<>();
		List<Long> singleTimes = new LinkedList<>();

		for (int i = 0; i < AMOUNT; i++) {
			String bucketID = UUID.randomUUID().toString();
			String path = SiteUtil.getUfeURI(site, String.format(OBJECTS_URI_TEMPLATE, appID, bucketType, bucketID));
			String objectID = createObject(path, TokenUtil.getToken(site, i));
			objectPaths.add(path + "/" + objectID);
		}

		long time1 = System.currentTimeMillis();

		for (int i = 0; i < objectPaths.size(); i++) {
			String objectPath = objectPaths.get(i);
			HttpEntity<String> requestEntity = updateObjectRequestEntity("jiji", TokenUtil.getToken(site, i));

			long t1 = System.currentTimeMillis();
			updateObject(objectPath, requestEntity);
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
		ExecutorService executor = Executors.newFixedThreadPool(threads);

		try {
			List<String> objectPaths = new CopyOnWriteArrayList<>();
			List<Long> singleTimes = new CopyOnWriteArrayList<>();
			List<Future<?>> futures = new LinkedList<>();

			for (int i = 0; i < AMOUNT; i++) {
				String accessToken = TokenUtil.getToken(site, i);

				futures.add(executor.submit(() -> {
					String bucketID = UUID.randomUUID().toString();
					String path = SiteUtil.getUfeURI(site,
							String.format(OBJECTS_URI_TEMPLATE, appID, bucketType, bucketID));
					String objectID = createObject(path, accessToken);
					objectPaths.add(path + "/" + objectID);
					return null;
				}));
			}

			for (Future<?> f : futures) {
				f.get();
			}
			futures.clear();

			long time1 = System.currentTimeMillis();

			for (int i = 0; i < objectPaths.size(); i++) {
				String objectPath = objectPaths.get(i);
				HttpEntity<String> requestEntity = updateObjectRequestEntity("jiji", TokenUtil.getToken(site, i));

				futures.add(executor.submit(() -> {
					long t1 = System.currentTimeMillis();
					updateObject(objectPath, requestEntity);
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

	private String createObject(String path, String accessToken) throws JSONException {
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
		String response = restTemplate.exchange(path, HttpMethod.POST, requestEntity, String.class).getBody();

		return new JSONObject(response).getString("objectID");
	}

	private static HttpEntity<String> updateObjectRequestEntity(String fieldValue, String accessToken) {
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

	private void updateObject(String path, HttpEntity<String> requestEntity) {
		// Request / Response
		restTemplate.exchange(path, HttpMethod.PUT, requestEntity, String.class);
	}

	public static void main(String[] args) throws JSONException {
		String object = ("{'client_id':'7311508b7dd2aaa4b06886f67038ca93', 'client_secret':'2b06ee27e5d9ce3bd36133b6627ffa26ef5a270a29dea625974560d2407d596f'}")
				.replace("'", "\"");

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.set("Connection", "keep-alive");
		headers.set("X-Kii-AppID", "691f1d8d");
		headers.set("X-Kii-AppKey", "d785caef215571b3f8439eff3a4c088b");

		RestTemplate restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());

		for (int i = 0; i < 200; i++) {
			String response = restTemplate.exchange("https://api-jp.kii.com/api/oauth2/token", HttpMethod.POST,
					new HttpEntity<String>(object, headers), String.class).getBody();
			JSONObject o = new JSONObject(response);
			System.out.println(o.get("access_token"));
		}

	}

}
