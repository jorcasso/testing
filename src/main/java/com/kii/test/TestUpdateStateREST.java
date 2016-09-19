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
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import com.kii.test.util.Site;
import com.kii.test.util.SiteUtil;
import com.kii.test.util.TokenUtil;

public class TestUpdateStateREST {

	private static final int AMOUNT = 200;
	private static final String ONBOARDING_PATH_TEMPLATE = "/apps/%s/onboardings";
	private static final String STATE_PATH_TEMPLATE = "/apps/%s/targets/THING:%s/states";
	private static final String THING_PATH_TEMPLATE = "/apps/%s/things/VENDOR_THING_ID:%s";
	private static final String VENDOR_THING_ID_PREFIX = "testUpdateState";

	private final RestTemplate restTemplate;
	private final String accessToken;
	private final Site site;
	private final String appID;

	public static void run(Site site, int threads) throws Exception {
		new TestUpdateStateREST(site, threads);
	}

	private TestUpdateStateREST(Site site, int threads) throws Exception {
		this.site = site;
		accessToken = TokenUtil.getToken(site);
		appID = SiteUtil.getApp(site);

		System.setProperty("http.maxConnections", "" + threads);
		restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());

		System.out.println("Going to update " + AMOUNT + " states by REST");

		if (threads > 1) {
			updateStateMultiThread(threads);
		} else {
			updateStateSingleThread();
		}
	}

	private void updateStateSingleThread() throws JSONException {
		System.out.println("Single thread");

		List<String> things = new LinkedList<>();

		for (int i = 0; i < AMOUNT; i++) {
			things.add(onboardThing(VENDOR_THING_ID_PREFIX + i));
		}

		HttpEntity<String> requestEntity = updateStateRequestEntity("jiji");

		long time1 = System.currentTimeMillis();

		for (String thingID : things) {
			updateState(thingID, requestEntity);
		}
		long time2 = System.currentTimeMillis();

		System.out.println("Elapsed time: " + (time2 - time1) + " ms");
	}

	private void updateStateMultiThread(int threads) throws Exception {
		System.out.println("Multithread (" + threads + ")");

		HttpEntity<String> requestEntity = updateStateRequestEntity("jiji");

		ExecutorService executor = Executors.newFixedThreadPool(threads);

		try {
			List<Future<?>> futures = new LinkedList<>();

			List<String> things = new LinkedList<>();

			for (int i = 0; i < AMOUNT; i++) {
				String vendorThingID = VENDOR_THING_ID_PREFIX + i;
				futures.add(executor.submit(() -> things.add(onboardThing(vendorThingID))));

				if (i % 20 == 0) {
					for (Future<?> f : futures) {
						f.get();
					}
				}
			}

			for (Future<?> f : futures) {
				f.get();
			}
			futures.clear();

			List<Long> singleTimes = new CopyOnWriteArrayList<>();
			long time1 = System.currentTimeMillis();

			for (String thingID : things) {
				futures.add(executor.submit(() -> {
					long t1 = System.currentTimeMillis();
					updateState(thingID, requestEntity);
					singleTimes.add(System.currentTimeMillis() - t1);
				}));
			}

			for (Future<?> f : futures) {
				f.get();
			}

			long time2 = System.currentTimeMillis();

			System.out.println("Elapsed time: " + (time2 - time1) + " ms");
			logMin(singleTimes);
			logMax(singleTimes);
			logAvg(singleTimes);
		} finally {
			executor.shutdown();
		}
	}

	private void logMax(List<Long> singleTimes) {
		long max = singleTimes.stream().max((o1, o2) -> o1.compareTo(o2)).get();
		System.out.println("Max: " + max + " ms");
	}

	private void logMin(List<Long> singleTimes) {
		long min = singleTimes.stream().min((o1, o2) -> o1.compareTo(o2)).get();
		System.out.println("Min: " + min + " ms");
	}

	private void logAvg(List<Long> singleTimes) {
		double total = 0;

		for (long singleTime : singleTimes) {
			total += singleTime;
		}

		double avg = total / singleTimes.size();
		System.out.println("Avg: " + avg + " ms");
	}

	private String onboardThing(String vendorThingID) throws JSONException {
		String thingID = getThingID(vendorThingID);
		if (thingID != null) {
			return thingID;
		} else {
			String body = ("{'vendorThingID': '" + vendorThingID + "', 'thingPassword': '123456'}").replace("'", "\"");

			// Headers
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(
					MediaType.parseMediaType("application/vnd.kii.OnboardingWithVendorThingIDByThing+json"));
			headers.set("Authorization", "Bearer " + accessToken);
			headers.set("Connection", "keep-alive");

			// Entity
			HttpEntity<String> requestEntity = new HttpEntity<String>(body, headers);

			// Request / Response
			String response = restTemplate
					.exchange(SiteUtil.getThingIFURI(site, String.format(ONBOARDING_PATH_TEMPLATE, appID)),
							HttpMethod.POST, requestEntity, String.class)
					.getBody();

			JSONObject responseJSON = new JSONObject(response);
			return responseJSON.getString("thingID");
		}
	}

	private String getThingID(String vendorThingID) throws JSONException {
		HttpHeaders headers = new HttpHeaders();
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		HttpEntity<Void> requestEntity = new HttpEntity<Void>(headers);

		// Request / Response
		try {
			String response = restTemplate
					.exchange(SiteUtil.getUfeURI(site, String.format(THING_PATH_TEMPLATE, appID, vendorThingID)),
							HttpMethod.GET, requestEntity, String.class)
					.getBody();

			JSONObject responseJSON = new JSONObject(response);
			return responseJSON.getString("_thingID");
		} catch (HttpStatusCodeException e) {
			if (e.getStatusCode().value() == 404) {
				return null;
			}
			throw e;
		}
	}

	private HttpEntity<String> updateStateRequestEntity(String fieldValue) {
		// Data
		String object = ("{'field1': '" + fieldValue + "', 'field2': 'value2'}").replace("'", "\"");

		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		return new HttpEntity<String>(object, headers);
	}

	private void updateState(String thingID, HttpEntity<String> requestEntity) {
		// Request / Response
		restTemplate.exchange(SiteUtil.getThingIFURI(site, String.format(STATE_PATH_TEMPLATE, appID, thingID)),
				HttpMethod.PUT, requestEntity, String.class);
	}

}
