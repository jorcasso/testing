package com.kii.test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

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

public class TestOneTimeTask {

	private static final int AMOUNT = 50;
	private static final String OBJECT_PATH_TEMPLATE = "/apps/%s/buckets/taskBucketTest/objects/%s";
	private static final String TASKS_PATH_TEMPLATE = "/apps/%s/tasks";

	private final RestTemplate restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());

	private final String batchID = "b" + new Random().nextInt(1000000);
	private final Site site;
	private final String appID;
	private final String accessToken;

	public static void main(String[] args) throws JSONException, InterruptedException {
		new TestOneTimeTask(Site.DEV_JP);
	}

	private TestOneTimeTask(Site site) throws JSONException, InterruptedException {
		this.site = site;
		appID = SiteUtil.getApp(site);
		accessToken = TokenUtil.getToken(site);

		System.out.println("BatchID: " + batchID);
		long date = System.currentTimeMillis() + 90000;
		System.out.println("Scheduled date: " + date);

		List<String> objects = scheduleTasks(date);
		System.out.println("All tasks scheduled");

		Thread.sleep(100000);

		List<Long> createdDates = new LinkedList<>();
		for (String objectID : objects) {
			long created = getCreatedAt(objectID);
			System.out.println(objectID + " - " + created);
			createdDates.add(created);
		}

		System.out.println("-------------------------------");

		createdDates.sort((o1, o2) -> o1.compareTo(o2));

		for (Long created : createdDates) {
			System.out.println(created + " - " + (created / 1000));
		}
	}

	private List<String> scheduleTasks(long time) throws JSONException {
		List<String> objects = new LinkedList<>();

		for (int i = 0; i < AMOUNT; i++) {
			String objectID = i + "-" + System.currentTimeMillis();
			scheduleTask(time, objectID);
			objects.add(objectID);
		}

		return objects;
	}

	private void scheduleTask(long date, String objectID) throws JSONException {
		// Data
		JSONObject params = new JSONObject();
		params.put("uri", "/api" + String.format(OBJECT_PATH_TEMPLATE, appID, objectID));
		params.put("method", "PUT");
		params.put("headers", new JSONObject().put("Content-Type", "application/json").put("If-None-Match", "*"));
		params.put("body", new JSONObject().put("something", "ops"));

		JSONObject body = new JSONObject();
		body.put("description", "Create object " + batchID + " " + objectID);
		body.put("what", "REST_API_CALL");
		body.put("when", date);
		body.put("params", params);

		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.parseMediaType("application/vnd.kii.TaskCreationRequest+json"));
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		HttpEntity<String> requestEntity = new HttpEntity<String>(body.toString(), headers);

		// Request / Response
		restTemplate.exchange(SiteUtil.getUfeURI(site, String.format(TASKS_PATH_TEMPLATE, appID)), HttpMethod.POST,
				requestEntity, String.class);
	}

	private long getCreatedAt(String objectID) throws JSONException {
		// Headers
		HttpHeaders headers = new HttpHeaders();
		headers.set("Authorization", "Bearer " + accessToken);
		headers.set("Connection", "keep-alive");

		// Entity
		HttpEntity<Void> requestEntity = new HttpEntity<Void>(headers);

		// Request / Response
		String object = restTemplate
				.exchange(SiteUtil.getUfeURI(site, String.format(OBJECT_PATH_TEMPLATE, appID, objectID)),
						HttpMethod.GET, requestEntity, String.class)
				.getBody();

		return new JSONObject(object).getLong("_created");
	}
}
