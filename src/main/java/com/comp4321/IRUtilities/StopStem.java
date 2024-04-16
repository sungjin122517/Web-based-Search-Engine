package com.comp4321.IRUtilities;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;

public class StopStem {
	private static final String STOPWORDS_FILE = "stopwords.txt";

	private Porter porter;
	private HashSet<String> stopWords;

	public boolean isStopWord(String str) {
		return stopWords.contains(str);
	}

	public StopStem(String str) {
		super();
		porter = new Porter();
		stopWords = new HashSet<>();

		BufferedReader br = new BufferedReader(
				new InputStreamReader(getClass().getClassLoader().getResourceAsStream(str)));
		br.lines().forEach(stopWords::add);
	}

	public StopStem() {
		this(STOPWORDS_FILE);
	}

	public String stem(String str) {
		return porter.stripAffixes(str);
	}

}
