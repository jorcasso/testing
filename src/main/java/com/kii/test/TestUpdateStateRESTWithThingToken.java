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

import com.kii.test.util.LogUtil;
import com.kii.test.util.Site;
import com.kii.test.util.SiteUtil;
import com.kii.test.util.TokenUtil;

public class TestUpdateStateRESTWithThingToken {

	private static final int AMOUNT = 200;
	private static final String ONBOARDING_PATH_TEMPLATE = "/apps/%s/onboardings";
	private static final String STATE_PATH_TEMPLATE = "/apps/%s/targets/THING:%s/states";
	private static final String THING_PATH_TEMPLATE = "/apps/%s/things/VENDOR_THING_ID:%s";
	private static final String VENDOR_THING_ID_PREFIX = "testUpdateState";

	private final RestTemplate restTemplate;
	private final String accessToken;
	private final Site site;
	private final String appID;

	private static final boolean THING_TOKEN_FLAG = true;

	public static void run(Site site, int threads) throws Exception {
		new TestUpdateStateRESTWithThingToken(site, threads);
	}

	private TestUpdateStateRESTWithThingToken(Site site, int threads) throws Exception {
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

		List<ThingInfo> things = new LinkedList<>();
		String fieldValue = "jiji";

		for (int i = 0; i < AMOUNT; i++) {
			things.add(onboardThing(VENDOR_THING_ID_PREFIX + i));
		}

		long time1 = System.currentTimeMillis();

		for (ThingInfo thingInfo : things) {
			HttpEntity<String> requestEntity = updateStateRequestEntity(
					THING_TOKEN_FLAG ? thingInfo.token : accessToken, fieldValue);

			updateState(thingInfo, requestEntity);
		}
		long time2 = System.currentTimeMillis();

		System.out.println("Elapsed time: " + (time2 - time1) + " ms");
	}

	private void updateStateMultiThread(int threads) throws Exception {
		System.out.println("Multithread (" + threads + ")");

		String fieldValue = "jiji";

		ExecutorService executor = Executors.newFixedThreadPool(threads);

		try {
			List<Future<?>> futures = new LinkedList<>();

			List<ThingInfo> things = new CopyOnWriteArrayList<>();

			for (int i = 0; i < AMOUNT; i++) {
				String vendorThingID = VENDOR_THING_ID_PREFIX + i; // + "-" + System.currentTimeMillis();
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

			for (ThingInfo thingInfo : things) {
				futures.add(executor.submit(() -> {
					HttpEntity<String> requestEntity = updateStateRequestEntity(
							THING_TOKEN_FLAG ? thingInfo.token : accessToken, fieldValue);

					long t1 = System.currentTimeMillis();
					updateState(thingInfo, requestEntity);
					singleTimes.add(System.currentTimeMillis() - t1);
				}));
			}

			for (Future<?> f : futures) {
				f.get();
			}

			long time2 = System.currentTimeMillis();

			System.out.println("Elapsed time: " + (time2 - time1) + " ms");
			LogUtil.logMinTime(singleTimes);
			LogUtil.logMaxTime(singleTimes);
			LogUtil.logAvgTime(singleTimes);
		} finally {
			executor.shutdown();
		}
	}

	private ThingInfo onboardThing(String vendorThingID) throws JSONException {
		String body = ("{'vendorThingID': '" + vendorThingID + "', 'thingPassword': '123456'}").replace("'", "\"");

		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.parseMediaType("application/vnd.kii.OnboardingWithVendorThingIDByThing+json"));
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		HttpEntity<String> requestEntity = new HttpEntity<String>(body, headers);

		// Request / Response
		String uri = SiteUtil.getThingIFURI(site, String.format(ONBOARDING_PATH_TEMPLATE, appID));
		String response = "";
		response = restTemplate.exchange(uri, HttpMethod.POST, requestEntity, String.class).getBody();

		JSONObject responseJSON = new JSONObject(response);
		return new ThingInfo(responseJSON.optString("accessToken"), responseJSON.optString("thingID"));

	}

	private ThingInfo getThingInfo(String vendorThingID) throws JSONException {
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

			return new ThingInfo(responseJSON.optString("_accessToken"), responseJSON.optString("_thingID"));
		} catch (HttpStatusCodeException e) {
			if (e.getStatusCode().value() == 404) {
				return null;
			}
			throw e;
		}
	}

	private HttpEntity<String> updateStateRequestEntity(String token, String fieldValue) {
		// Data
		String object = ("{'field1': '" + fieldValue + "', 'field2': 'value2'}").replace("'", "\"");

		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.set("Authorization", "Bearer " + token);
		headers.set("Connection", "keep-alive");

		// Entity
		return new HttpEntity<String>(object, headers);
	}

	private void updateState(ThingInfo thingInfo, HttpEntity<String> requestEntity) {
		// Request / Response
		restTemplate.exchange(
				SiteUtil.getThingIFURI(site, String.format(STATE_PATH_TEMPLATE, appID, thingInfo.thingID)),
				HttpMethod.PUT, requestEntity, String.class);
	}

	static class ThingInfo {
		String token;
		String thingID;

		public ThingInfo(String token, String thingID) {
			this.token = token;
			this.thingID = thingID;
		}
	}
}
