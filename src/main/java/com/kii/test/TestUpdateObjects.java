package com.kii.test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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

import com.kii.test.util.LogUtil;
import com.kii.test.util.Site;
import com.kii.test.util.SiteUtil;
import com.kii.test.util.TokenUtil;

public class TestUpdateObjects {

	private static final int AMOUNT = 200;
	private static final String OBJECTS_PATH_TEMPLATE = "/apps/%s/buckets/%s:%s/objects";

	private final RestTemplate restTemplate;
	private final Site site;
	private final String appID;
	private final String bucketType;
	private final Optional<String> accessToken;
	private final Optional<String> bucketID;

	public static void run(Site site, int threads, String bucketType, UpdateObjectsMode mode, String bucketID)
			throws Exception {
		new TestUpdateObjects(site, threads, bucketType, mode, bucketID);
	}

	private TestUpdateObjects(Site site, int threads, String bucketType, UpdateObjectsMode mode, String bucketID)
			throws Exception {
		this.site = site;
		this.appID = SiteUtil.getApp(site);
		this.bucketType = bucketType;
		this.accessToken = mode.sameToken() ? Optional.of(TokenUtil.getToken(site)) : Optional.empty();
		this.bucketID = mode.sameBucket() ? Optional.of(bucketID) : Optional.empty();

		System.setProperty("http.maxConnections", "" + threads);
		restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());

		System.out.println(String.format("Going to update %d objects. bucketType=%s, mode=%s, bucketID=%s", AMOUNT,
				bucketType, mode, this.bucketID.orElse("")));

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
			final int pos = i;
			String bucketID = this.bucketID.orElseGet(() -> UUID.randomUUID().toString());
			String path = SiteUtil.getUfeURI(site, String.format(OBJECTS_PATH_TEMPLATE, appID, bucketType, bucketID));
			String objectID = createObject(path, accessToken.orElseGet(() -> TokenUtil.getToken(site, pos)));
			objectPaths.add(path + "/" + objectID);
		}

		long time1 = System.currentTimeMillis();

		for (int i = 0; i < objectPaths.size(); i++) {
			final int pos = i;
			String objectPath = objectPaths.get(pos);
			String accessToken = this.accessToken.orElseGet(() -> TokenUtil.getToken(site, pos));

			HttpEntity<String> requestEntity = updateObjectRequestEntity("jiji", accessToken);

			long t1 = System.currentTimeMillis();
			updateObject(objectPath, requestEntity);
			singleTimes.add(System.currentTimeMillis() - t1);
		}
		long time2 = System.currentTimeMillis();

		for (int i = 0; i < singleTimes.size(); i++) {
			System.out.println(String.format("Single time #%d: %d ms", i + 1, singleTimes.get(i)));
		}

		System.out.println("Elapsed time: " + (time2 - time1) + " ms");
		LogUtil.logMinTime(singleTimes);
		LogUtil.logMaxTime(singleTimes);
		LogUtil.logAvgTime(singleTimes);
	}

	private void updateObjectMultiThread(int threads) throws Exception {
		System.out.println("Multithread (" + threads + ")");

		ExecutorService executor = Executors.newFixedThreadPool(threads);

		try {
			List<String> objectPaths = Collections.synchronizedList(new LinkedList<>());
			List<Long> singleTimes = Collections.synchronizedList(new LinkedList<>());
			List<Long> delayTimes = Collections.synchronizedList(new LinkedList<>());
			List<Future<?>> futures = new LinkedList<>();

			for (int i = 0; i < AMOUNT; i++) {
				final int pos = i;
				futures.add(executor.submit(() -> {
					String bucketID = this.bucketID.orElseGet(() -> UUID.randomUUID().toString());
					String path = SiteUtil.getUfeURI(site,
							String.format(OBJECTS_PATH_TEMPLATE, appID, bucketType, bucketID));
					String objectID = createObject(path, accessToken.orElseGet(() -> TokenUtil.getToken(site, pos)));
					objectPaths.add(path + "/" + objectID);
					return null;
				}));
			}

			for (Future<?> f : futures) {
				f.get();
			}
			futures.clear();

			long startTime = System.currentTimeMillis() + 2000;

			for (int i = 0; i < objectPaths.size(); i++) {
				final int pos = i;

				futures.add(executor.submit(() -> {
					String objectPath = objectPaths.get(pos);
					String accessToken = this.accessToken.orElseGet(() -> TokenUtil.getToken(site, pos));

					HttpEntity<String> requestEntity = updateObjectRequestEntity("jiji", accessToken);

					long delayStart = startTime - System.currentTimeMillis();
					if (delayStart > 0)
						Thread.sleep(delayStart);

					long t1 = System.currentTimeMillis();
					updateObject(objectPath, requestEntity);
					singleTimes.add(System.currentTimeMillis() - t1);
					delayTimes.add(t1 - startTime);
					return null;
				}));
			}

			for (Future<?> f : futures) {
				f.get();
			}

			long time2 = System.currentTimeMillis();

			for (int i = 0; i < singleTimes.size(); i++) {
				System.out.println(String.format("Single time #%d: %d ms, delay %d", i + 1, singleTimes.get(i),
						delayTimes.get(i)));
			}

			System.out.println("Elapsed time: " + (time2 - startTime) + " ms");
			LogUtil.logMinTime(singleTimes);
			LogUtil.logMaxTime(singleTimes);
			LogUtil.logAvgTime(singleTimes);
			LogUtil.logMinDelay(delayTimes);
			LogUtil.logMaxDelay(delayTimes);
			LogUtil.logAvgDelay(delayTimes);
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
		HttpEntity<String> requestEntity = new HttpEntity<>(object, headers);

		// Request / Response
		String response = restTemplate.exchange(path, HttpMethod.POST, requestEntity, String.class).getBody();

		return new JSONObject(response).getString("objectID");
	}

	private HttpEntity<String> updateObjectRequestEntity(String fieldValue, String accessToken) {
		// Data
		String object = ("{'field1': '" + fieldValue
				+ "', 'field2': 'value2', 'subjson': {'subfield1': 234, 'subfield2': true}}").replace("'", "\"");

		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		return new HttpEntity<>(object, headers);
	}

	private void updateObject(String path, HttpEntity<String> requestEntity) {
		// Request / Response
		restTemplate.exchange(path, HttpMethod.PUT, requestEntity, String.class);
	}

}
