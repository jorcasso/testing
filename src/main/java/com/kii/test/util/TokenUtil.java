package com.kii.test.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

public class TokenUtil {

	private static List<String> devTokens = new LinkedList<>();
	private static List<String> stgTokens = new LinkedList<>();
	private static List<String> prodTokens = new LinkedList<>();

	static {
		loadTokens("dev-tokens", devTokens);
		loadTokens("stg-tokens", stgTokens);
		loadTokens("prod-tokens", prodTokens);
	}

	private static void loadTokens(String file, List<String> list) {
		InputStream stream = TokenUtil.class.getResourceAsStream("/com/kii/test/" + file);

		BufferedReader in = new BufferedReader(new InputStreamReader(stream));

		String line = null;
		try {
			while ((line = in.readLine()) != null) {
				list.add(line);
			}

			stream.close();
		} catch (IOException e) {
			System.out.println("failed to read tokens. File " + file);
			System.exit(1);
		}
	}

	public static String getToken(Site site) {
		return getToken(site, 0);
	}

	public static String getToken(Site site, int position) {
		switch (site) {
		case DEV_JP:
			return devTokens.get(position);
		case STG_JP:
			return stgTokens.get(position);
		case PROD_JP:
			return prodTokens.get(position);
		default:
			throw new RuntimeException("Unknown site " + site);
		}
	}
}
