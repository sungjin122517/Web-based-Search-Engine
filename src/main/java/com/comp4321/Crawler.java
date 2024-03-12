package com.comp4321;

import java.net.URL;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.function.Consumer;

import org.htmlparser.beans.LinkBean;
import org.htmlparser.beans.StringBean;
import org.htmlparser.util.ParserException;

public class Crawler {
    public final String url;

    public Crawler(String _url) {
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

    public ZonedDateTime getLastModified() throws ParserException {
        final var lb = new LinkBean();
        lb.setURL(url);

        // HTTP header "Last-Modified" is in GMT
        final var instant = Instant.ofEpochMilli(lb.getConnection().getLastModified());
        return ZonedDateTime.ofInstant(instant, ZoneId.of("GMT"));
    }

    public void bfs(int maxPages, Consumer<String> indexer) throws ParserException {
        final var queue = new ArrayDeque<String>();
        final var visited = new HashSet<String>();

        queue.add(url);
        visited.add(url);

        final var lb = new LinkBean();
        while (!queue.isEmpty() && visited.size() < maxPages) {
            final var curURL = queue.remove();
            lb.setURL(curURL);

            Arrays.stream(lb.getLinks())
                    .map(URL::toString)
                    .forEach(link -> {
                        if (!visited.contains(link)) {
                            queue.add(link);
                            visited.add(link);
                            indexer.accept(link);
                        }
                    });
        }
    }
}
