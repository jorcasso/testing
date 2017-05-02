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
import org.springframework.web.client.RestClientException;
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
	private static final String THING_TYPE_PATH_TEMPLATE = "/apps/%s/configuration/thing-types/%s";
	private static final String FIRMWARE_VERSION_PATH_TEMPLATE = "/apps/%s/configuration/thing-types/%s/firmware-versions/%s";
	private static final String TRAIT_PATH_TEMPLATE = "/apps/%s/traits/%s/versions";
	private static final String ALIAS_PATH_TEMPLATE = "/apps/%s/configuration/thing-types/%s/firmware-versions/%s/aliases/%s";

	private static final String VENDOR_THING_ID_PREFIX = "testUpdateState";

	private final RestTemplate restTemplate;
	private final String accessToken;
	private final Site site;
	private final String appID;

	private String trait;
	private String alias;
	private String thingType;
	private String firmwareVersion;
	private int traitVersion;

	private final boolean hasTraits;

	private static final boolean THING_TOKEN_FLAG = true;

	public static void run(Site site, int threads, boolean hasTraits) throws Exception {
		new TestUpdateStateRESTWithThingToken(site, threads, hasTraits);
	}

	private TestUpdateStateRESTWithThingToken(Site site, int threads, boolean hasTraits) throws Exception {
		this.site = site;
		this.accessToken = TokenUtil.getToken(site);
		this.appID = SiteUtil.getApp(site);
		this.hasTraits = hasTraits;

		System.setProperty("http.maxConnections", "" + threads);

		restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());

		if (hasTraits) {
			prepareRequiredDataForTraits();
		}

		LogUtil.log("Going to update " + AMOUNT + " states" + (hasTraits ? " with traits " : " ") + "by REST");

		if (threads > 1) {
			updateStateMultiThread(threads);
		} else {
			updateStateSingleThread();
		}
	}

	private void updateStateSingleThread() throws JSONException {
		LogUtil.log("Single thread");

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

		LogUtil.log("Elapsed time: " + (time2 - time1) + " ms");
	}

	private void updateStateMultiThread(int threads) throws Exception {
		LogUtil.log("Multithread (" + threads + ")");

		String fieldValue = "jiji";

		ExecutorService executor = Executors.newFixedThreadPool(threads);

		try {
			List<Future<?>> futures = new LinkedList<>();

			List<ThingInfo> things = new CopyOnWriteArrayList<>();

			for (int i = 0; i < AMOUNT; i++) {
				String vendorThingID = VENDOR_THING_ID_PREFIX + System.currentTimeMillis() + i;
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
			List<Long> delayTimes = new CopyOnWriteArrayList<>();
			long startTime = System.currentTimeMillis() + 2000;

			for (ThingInfo thingInfo : things) {
				futures.add(executor.submit(() -> {
					HttpEntity<String> requestEntity = instanceRequestEntity(
							THING_TOKEN_FLAG ? thingInfo.token : accessToken, fieldValue);

					long delayStart = startTime - System.currentTimeMillis();
					if (delayStart > 0)
						Thread.sleep(delayStart);

					long t1 = System.currentTimeMillis();
					updateState(thingInfo, requestEntity);
					singleTimes.add(System.currentTimeMillis() - t1);
					delayTimes.add(t1 - startTime);
					return null;
				}));
			}

			for (Future<?> f : futures) {
				f.get();
			}

			long time2 = System.currentTimeMillis();

			LogUtil.log("Elapsed time: " + (time2 - startTime) + " ms");
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

	private void prepareRequiredDataForTraits() throws JSONException {
		LogUtil.log("* Preparing required data for traits...");

		trait = "testPerfTrait1";
		alias = "testPerfAlias1";
		thingType = "testPerfThingType";
		firmwareVersion = "testPerfFirmwareVersion";
		traitVersion = 1;

		registerThingType();
		registerFirmwareVersion(thingType);
		registerTrait();
		registerAlias();
	}

	private HttpEntity<String> instanceRequestEntity(String token, String fieldValue) {
		if (hasTraits) {
			return updateStateWithTraitsRequestEntity(token, alias, fieldValue);
		} else {
			return updateStateRequestEntity(token, fieldValue);
		}
	}

	private ThingInfo onboardThing(String vendorThingID) throws JSONException {
		String body = hasTraits
				? ("{'vendorThingID': '" + vendorThingID + "', 'thingPassword': '123456', 'thingType' : '" + thingType
						+ "', 'firmwareVersion' : '" + firmwareVersion + "'}").replace("'", "\"")
				: ("{'vendorThingID': '" + vendorThingID + "', 'thingPassword': '123456'}").replace("'", "\"");

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

	private void registerThingType() {
		String body = "{'simpleFlow':true, 'verificationCodeFlowStartedByUser':true, 'verificationCodeFlowStartedByThing':true}"
				.replace("'", "\"");

		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.parseMediaType("application/vnd.kii.ThingTypeConfigurationRequest+json"));
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		HttpEntity<String> requestEntity = new HttpEntity<>(body, headers);

		// Request / Response
		String uri = SiteUtil.getUfeURI(site, String.format(THING_TYPE_PATH_TEMPLATE, appID, thingType));

		restTemplate.exchange(uri, HttpMethod.PUT, requestEntity, String.class);
	}

	private void registerFirmwareVersion(String thingType) {
		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		HttpEntity<Void> requestEntity = new HttpEntity<>(headers);

		// Request
		String uri = SiteUtil.getUfeURI(site,
				String.format(FIRMWARE_VERSION_PATH_TEMPLATE, appID, thingType, firmwareVersion));

		restTemplate.exchange(uri, HttpMethod.PUT, requestEntity, String.class);
	}

	private void registerTrait() throws JSONException {
		String body = ("{ 'actions' : [], "
				+ "'states' : [ { 'power' : {'description':'the power','payloadSchema':{'type':'boolean'}}}," + //
				"{'customField' : {'description':'custom field','payloadSchema':{'type':'string'}}} ]}") //
						.replace("'", "\"");

		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.parseMediaType("application/vnd.kii.TraitCreationRequest+json"));
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		HttpEntity<String> requestEntity = new HttpEntity<>(body, headers);

		// Request
		String uri = SiteUtil.getThingIFURI(site, String.format(TRAIT_PATH_TEMPLATE, appID, trait));
		String response;
		try {
			response = restTemplate.exchange(uri, HttpMethod.POST, requestEntity, String.class).getBody();
			JSONObject responseJSON = new JSONObject(response);
			traitVersion = responseJSON.getInt("traitVersion");
			LogUtil.log("Created trait " + trait + " with version " + traitVersion);
		} catch (HttpStatusCodeException e) {
			if (e.getStatusCode().value() != 409) {
				throw e;
			}
			LogUtil.log("Trait already exists (" + trait + ")");
		}
	}

	private void registerAlias() {
		String body = ("{'trait' : '" + trait + "', 'traitVersion' : " + traitVersion + "}").replace("'", "\"");

		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.parseMediaType("application/vnd.kii.TraitAliasCreationRequest+json"));
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		HttpEntity<String> requestEntity = new HttpEntity<>(body, headers);

		// Request
		String uri = SiteUtil.getThingIFURI(site,
				String.format(ALIAS_PATH_TEMPLATE, appID, thingType, firmwareVersion, alias));
		try {
			restTemplate.exchange(uri, HttpMethod.PUT, requestEntity, String.class);
			LogUtil.log("Created alias " + alias);
		} catch (HttpStatusCodeException e) {
			if (e.getStatusCode().value() != 409) {
				throw e;
			}
			LogUtil.log("Alias already exists (" + alias + ")");
		}
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

	private HttpEntity<String> updateStateWithTraitsRequestEntity(String token, String alias, String fieldValue) {
		// Data
		String object = ("{'" + alias + "' : " + //
				"{ 'customField':'" + fieldValue + "', 'power':true }}").replace("'", "\"");

		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.parseMediaType("application/vnd.kii.MultipleTraitState+json"));
		headers.set("Authorization", "Bearer " + token);
		headers.set("Connection", "keep-alive");

		// Entity
		return new HttpEntity<String>(object, headers);
	}

	private void updateState(ThingInfo thingInfo, HttpEntity<String> requestEntity) {
		// Request / Response
		try {
			restTemplate.exchange(
					SiteUtil.getThingIFURI(site, String.format(STATE_PATH_TEMPLATE, appID, thingInfo.thingID)),
					HttpMethod.PUT, requestEntity, String.class);
		} catch (RestClientException e) {
			LogUtil.log("-ERR=" + e.getMessage());
		}
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
