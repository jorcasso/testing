package com.kii.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
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

public class TestUpdateStateMQTT {

	private static final int AMOUNT = 200;
	private static final String ONBOARDING_PATH_TEMPLATE = "/apps/%s/onboardings";
	private static final String MQTT_URI_TEMPLATE = "tcp://%s:%d";
	private static final String MQTT_TOPIC_TEMPLATE = "p/%s/thing-if/apps/%s/targets/THING:%s/states";
	private static final String VENDOR_THING_ID_PREFIX = "testUpdateState";
	private static final byte[] CRLF = new byte[] { 13, 10 };

	private static final boolean THING_TOKEN_FLAG = true;

	private final List<MqttAsyncClient> clients = new LinkedList<>();
	private final RestTemplate restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());
	private final String appID;
	private final String onboardingURI;
	private final String accessToken;

	public static void main(String[] args) throws Exception {
		new TestUpdateStateMQTT(Site.DEV_JP, 10);
	}

	public static void run(Site site, int threads) throws Exception {
		new TestUpdateStateMQTT(site, threads);
	}

	private TestUpdateStateMQTT(Site site, int threads) throws Exception {
		this.appID = SiteUtil.getApp(site);
		this.onboardingURI = SiteUtil.getThingIFURI(site, String.format(ONBOARDING_PATH_TEMPLATE, appID));
		this.accessToken = TokenUtil.getToken(site);

		System.out.println("Going to update " + AMOUNT + " states by MQTT");
		try {
			if (threads > 1) {
				updateStateMultiThread(threads);
			} else {
				updateStateSingleThread();
			}
		} finally {
			for (MqttAsyncClient client : clients) {
				try {
					client.disconnect();
				} catch (Exception e) {
				}
			}
		}
	}

	private void updateStateSingleThread() throws Exception {
		System.out.println("Single thread");

		Map<ThingInfo, MqttAsyncClient> things = new HashMap<>();

		for (int i = 0; i < AMOUNT; i++) {
			JSONObject response = onboardThing(VENDOR_THING_ID_PREFIX + i);
			MqttAsyncClient client = getClient(response.getJSONObject("mqttEndpoint"));
			things.put(new ThingInfo(response.getString("accessToken"), response.getString("thingID")), client);
		}

		List<AtomicLong> singleTimes = new CopyOnWriteArrayList<>();
		List<CountDownLatch> latches = new LinkedList<>();
		long time1 = System.currentTimeMillis();

		for (Entry<ThingInfo, MqttAsyncClient> entry : things.entrySet()) {
			AtomicLong t1 = new AtomicLong(0);
			latches.add(updateState(entry.getValue(), entry.getKey(), t1));
			singleTimes.add(t1);
		}

		long time2 = System.currentTimeMillis();
		System.out.println("Request elapsed time: " + (time2 - time1) + " ms");

		time1 = System.currentTimeMillis();
		for (CountDownLatch latch : latches) {
			if (!latch.await(5000, TimeUnit.SECONDS)) {
				throw new RuntimeException("Timeout waiting response from the broker");
			}
		}
		time2 = System.currentTimeMillis();
		System.out.println("Response elapsed time: " + (time2 - time1) + " ms");
		LogUtil.logMinTime(singleTimes.stream().map(AtomicLong::get).collect(Collectors.toList()));
		LogUtil.logMaxTime(singleTimes.stream().map(AtomicLong::get).collect(Collectors.toList()));
		LogUtil.logAvgTime(singleTimes.stream().map(AtomicLong::get).collect(Collectors.toList()));
	}

	private void updateStateMultiThread(int threads) throws Exception {
		System.out.println("Multithread (" + threads + ")");

		ExecutorService executor = Executors.newFixedThreadPool(threads);

		try {
			Map<ThingInfo, MqttAsyncClient> things = new ConcurrentHashMap<>();
			List<Future<?>> futures = new LinkedList<>();

			for (int i = 0; i < AMOUNT; i++) {
				String vendorThingID = VENDOR_THING_ID_PREFIX + i;
				futures.add(executor.submit(() -> {
					JSONObject response = onboardThing(vendorThingID);
					MqttAsyncClient client = getClient(response.getJSONObject("mqttEndpoint"));
					things.put(new ThingInfo(response.getString("accessToken"), response.getString("thingID")), client);
					return null;
				}));
			}

			for (Future<?> f : futures) {
				f.get();
			}
			futures.clear();

			List<AtomicLong> singleTimes = new CopyOnWriteArrayList<>();
			List<CountDownLatch> latches = new CopyOnWriteArrayList<>();
			long time1 = System.currentTimeMillis();

			for (Entry<ThingInfo, MqttAsyncClient> entry : things.entrySet()) {
				futures.add(executor.submit(() -> {
					AtomicLong t1 = new AtomicLong(0);
					latches.add(updateState(entry.getValue(), entry.getKey(), t1));
					singleTimes.add(t1);
					return null;
				}));
			}

			for (Future<?> f : futures) {
				f.get();
			}

			long time2 = System.currentTimeMillis();
			System.out.println("Request elapsed time: " + (time2 - time1) + " ms");

			for (CountDownLatch latch : latches) {
				if (!latch.await(5000, TimeUnit.SECONDS)) {
					throw new RuntimeException("Timeout waiting response from the broker");
				}
			}
			time2 = System.currentTimeMillis();

			System.out.println("Response elapsed time: " + (time2 - time1) + " ms");
			LogUtil.logMinTime(singleTimes.stream().map(AtomicLong::get).collect(Collectors.toList()));
			LogUtil.logMaxTime(singleTimes.stream().map(AtomicLong::get).collect(Collectors.toList()));
			LogUtil.logAvgTime(singleTimes.stream().map(AtomicLong::get).collect(Collectors.toList()));
		} finally {
			executor.shutdown();
		}
	}

	private JSONObject onboardThing(String vendorThingID) throws JSONException {
		String body = ("{'vendorThingID': '" + vendorThingID + "', 'thingPassword': '123456'}").replace("'", "\"");

		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.parseMediaType("application/vnd.kii.OnboardingWithVendorThingIDByThing+json"));
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		HttpEntity<String> requestEntity = new HttpEntity<String>(body, headers);

		// Request / Response
		String response = restTemplate.exchange(onboardingURI, HttpMethod.POST, requestEntity, String.class).getBody();

		return new JSONObject(response);
	}

	private MqttAsyncClient getClient(JSONObject mqttData) throws Exception {
		String clientID = mqttData.getString("mqttTopic");
		String username = mqttData.getString("username");
		String password = mqttData.getString("password");
		String host = mqttData.getString("host");
		int port = mqttData.getInt("portTCP");

		MqttConnectOptions connOpts = new MqttConnectOptions();
		connOpts.setCleanSession(true);
		connOpts.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
		connOpts.setUserName(username);
		connOpts.setPassword(password.toCharArray());

		MqttAsyncClient client = new MqttAsyncClient(getServerURI(host, port), clientID, new MemoryPersistence());
		IMqttToken token = client.connect(connOpts);
		token.waitForCompletion();

		clients.add(client);

		return client;
	}

	private CountDownLatch updateState(MqttAsyncClient client, ThingInfo thingInfo, AtomicLong singleTime) throws Exception {
		String sendTopic = String.format(MQTT_TOPIC_TEMPLATE, client.getClientId(), appID, thingInfo.thingID);
		AtomicLong t1 = new AtomicLong(0);

		CountDownLatch latch = new CountDownLatch(1);
		client.setCallback(new MqttCallback() {

			@Override
			public void messageArrived(String topic, MqttMessage message) throws Exception {
				if (topic.equals(sendTopic)) {
					singleTime.set(System.currentTimeMillis() - t1.get());
					latch.countDown();
					// System.out.println(new String(message.getPayload()));
				}
			}

			@Override
			public void deliveryComplete(IMqttDeliveryToken token) {

			}

			@Override
			public void connectionLost(Throwable cause) {

			}
		});

		JSONObject body = new JSONObject().put("field1", "something").put("field2", "value2");
		byte[] requestPayload = buildPublishRequestPayload(body, THING_TOKEN_FLAG ? thingInfo.token : accessToken);

		MqttMessage message = new MqttMessage();
		message.setPayload(requestPayload);
		message.setQos(0);

		t1.set(System.currentTimeMillis());
		client.publish(sendTopic, message);

		return latch;
	}

	private static String getServerURI(String host, int port) {
		return String.format(MQTT_URI_TEMPLATE, host, port);
	}

	protected byte[] buildPublishRequestPayload(JSONObject body, String token) throws IOException {
		ByteArrayOutputStream bArray = new ByteArrayOutputStream();

		try {
			// Write method
			bArray.write("PUT".getBytes());
			bArray.write(CRLF);

			// Write headers
			bArray.write("Content-Type:application/json".getBytes());
			bArray.write(CRLF);
			bArray.write(("Authorization:Bearer " + token).getBytes());
			bArray.write(CRLF);

			// Write body
			bArray.write(CRLF);
			bArray.write(body.toString().getBytes());

			return bArray.toByteArray();
		} finally {
			bArray.close();
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
