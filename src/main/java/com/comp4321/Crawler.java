package com.comp4321;

import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

import org.htmlparser.beans.LinkBean;
import org.htmlparser.beans.StringBean;
import org.htmlparser.util.ParserException;

public class Crawler {
    public final String url;

    Crawler(String _url) {
        url = _url;
    }

    public List<String> extractWords() throws ParserException {
        final var sb = new StringBean();
        sb.setURL(url);
        sb.setLinks(false);

        final var words = new ArrayList<String>();
        final var tokenizer = new StringTokenizer(sb.getStrings());
        tokenizer.asIterator().forEachRemaining(t -> words.add((String) t));

        return words;
    }

    public List<String> extractLinks() throws ParserException {
        final var lb = new LinkBean();
        lb.setURL(url);

        return Arrays.stream(lb.getLinks()).map(URL::toString).toList();
    }

    public List<String> bfs(int maxPages) throws ParserException {
        final var queue = new ArrayDeque<String>();
        final var visited = new HashSet<String>();
        final var result = new ArrayList<String>();

        queue.add(url);
        visited.add(url);
        result.add(url);

        final var lb = new LinkBean();
        while (!queue.isEmpty() && result.size() < maxPages) {
            final var curURL = queue.remove();
            lb.setURL(curURL);

            Arrays.stream(lb.getLinks())
                    .map(URL::toString)
                    .filter(link -> !visited.contains(link))
                    .forEach(link -> {
                        queue.add(link);
                        visited.add(link);
                        result.add(link);
                    });
        }

        return result;
    }
}
