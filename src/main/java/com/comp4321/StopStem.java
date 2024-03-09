package com.comp4321;

import java.io.*;
import java.util.HashSet;

import com.comp4321.IRUtilities.Porter;

public class StopStem {
	private Porter porter;
	private HashSet<String> stopWords;

	public boolean isStopWord(String str) {
		return stopWords.contains(str);
	}

	public StopStem(String str) {
		super();
		porter = new Porter();
		stopWords = new HashSet<String>();

		BufferedReader br = new BufferedReader(
				new InputStreamReader(getClass().getClassLoader().getResourceAsStream(str)));
		br.lines().forEach(stopWords::add);
	}

	public String stem(String str) {
		return porter.stripAffixes(str);
	}

}
