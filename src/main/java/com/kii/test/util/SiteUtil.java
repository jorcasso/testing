package com.kii.test.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SiteUtil {

	private static String UFE_DEV_JP;
	private static String UFE_DEV_JP_01;
	private static String UFE_DEV_JP_02;
	private static String UFE_STG_JP;
	private static String UFE_PROD_JP;
	private static String UFE_LOCAL;

	private static String THING_IF_DEV_JP;
	private static String THING_IF_DEV_JP_01;
	private static String THING_IF_DEV_JP_02;
	private static String THING_IF_STG_JP;
	private static String THING_IF_PROD_JP;
	private static String THING_IF_LOCAL;

	private static String APP_DEV_JP;
	private static String APP_STG_JP;
	private static String APP_PROD_JP;

	static {
		loadApps();
		loadSites();
	}

	private static void loadSites() {
		Properties sites = new Properties();

		InputStream stream = TokenUtil.class.getResourceAsStream("/com/kii/test/sites");

		try {
			sites.load(stream);
			stream.close();

			UFE_DEV_JP = sites.getProperty("ufe-dev-jp");
			UFE_DEV_JP_01 = sites.getProperty("ufe-dev-jp-01");
			UFE_DEV_JP_02 = sites.getProperty("ufe-dev-jp-02");
			UFE_STG_JP = sites.getProperty("ufe-stg-jp");
			UFE_PROD_JP = sites.getProperty("ufe-prod-jp");
			UFE_LOCAL = sites.getProperty("ufe-local");
			THING_IF_DEV_JP = sites.getProperty("thing-if-dev-jp");
			THING_IF_DEV_JP_01 = sites.getProperty("thing-if-dev-jp-01");
			THING_IF_DEV_JP_02 = sites.getProperty("thing-if-dev-jp-02");
			THING_IF_STG_JP = sites.getProperty("thing-if-stg-jp");
			THING_IF_PROD_JP = sites.getProperty("thing-if-prod-jp");
			THING_IF_LOCAL = sites.getProperty("thing-if-local");
		} catch (IOException e) {
			System.out.println("failed to read sites");
			System.exit(1);
		}
	}

	private static void loadApps() {
		Properties apps = new Properties();

		InputStream stream = TokenUtil.class.getResourceAsStream("/com/kii/test/apps");

		try {
			apps.load(stream);
			stream.close();

			APP_DEV_JP = apps.getProperty("dev-jp");
			APP_STG_JP = apps.getProperty("stg-jp");
			APP_PROD_JP = apps.getProperty("prod-jp");
		} catch (IOException e) {
			System.out.println("failed to read sites");
			System.exit(1);
		}
	}

	public static String getUfeURI(Site site, String path) {
		String separator = path.startsWith("/") ? "" : "/";

		switch (site) {
		case DEV_JP:
			return UFE_DEV_JP + separator + path;
		case DEV_JP_01:
			return UFE_DEV_JP_01 + separator + path;
		case DEV_JP_02:
			return UFE_DEV_JP_02 + separator + path;
		case STG_JP:
			return UFE_STG_JP + separator + path;
		case PROD_JP:
			return UFE_PROD_JP + separator + path;
		case DEV_JP_LOCAL:
		case STG_JP_LOCAL:
		case PROD_JP_LOCAL:
			return UFE_LOCAL + separator + path;
		default:
			throw new RuntimeException("Unknown site " + site);
		}
	}

	public static String getThingIFURI(Site site, String path) {
		String separator = path.startsWith("/") ? "" : "/";

		switch (site) {
		case DEV_JP:
			return THING_IF_DEV_JP + separator + path;
		case DEV_JP_01:
			return THING_IF_DEV_JP_01 + separator + path;
		case DEV_JP_02:
			return THING_IF_DEV_JP_02 + separator + path;
		case STG_JP:
			return THING_IF_STG_JP + separator + path;
		case PROD_JP:
			return THING_IF_PROD_JP + separator + path;
		case DEV_JP_LOCAL:
		case STG_JP_LOCAL:
		case PROD_JP_LOCAL:
			return THING_IF_LOCAL + separator + path;
		default:
			throw new RuntimeException("Unknown site " + site);
		}
	}

	public static String getApp(Site site) {
		switch (site) {
		case DEV_JP:
		case DEV_JP_01:
		case DEV_JP_02:
		case DEV_JP_LOCAL:
			return APP_DEV_JP;
		case STG_JP:
		case STG_JP_LOCAL:
			return APP_STG_JP;
		case PROD_JP:
		case PROD_JP_LOCAL:
			return APP_PROD_JP;
		default:
			throw new RuntimeException("Unknown site " + site);
		}
	}
}
