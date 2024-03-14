package com.comp4321;

import java.net.URL;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

import org.htmlparser.Parser;
import org.htmlparser.Tag;
import org.htmlparser.beans.LinkBean;
import org.htmlparser.beans.StringBean;
import org.htmlparser.util.ParserException;

public class Crawler {
    public final String url;

    public Crawler(String _url) {
        url = _url;
    }

    public List<String> extractWords() throws ParserException {
        final var parser = new Parser();
        parser.setURL(url);
        final var body = parser.parse(node -> {
            if (!(node instanceof Tag))
                return false;

            final var tag = (Tag) node;
            return tag.getTagName().equalsIgnoreCase("BODY");
        }).toNodeArray();

        final var sb = new StringBean();
        for (final var b : body)
            b.accept(sb);

        // Split by non-alphanumeric characters
        return Arrays.asList(sb.getStrings().split("[\\W_]+"));
    }

    public List<String> extractTitle() throws ParserException {
        final var parser = new Parser();
        parser.setURL(url);
        final var titles = parser.parse(node -> {
            if (!(node instanceof Tag))
                return false;

            final var tag = (Tag) node;
            return tag.getTagName().equalsIgnoreCase("TITLE");
        }).toNodeArray();

        final var sb = new StringBean();
        for (final var title : titles)
            title.accept(sb);

        // Split by non-alphanumeric characters
        return Arrays.asList(sb.getStrings().split("[\\W_]+"));
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
        indexer.accept(url);

        final var lb = new LinkBean();
        while (!queue.isEmpty() && visited.size() < maxPages) {
            final var curURL = queue.remove();
            lb.setURL(curURL);

            Arrays.stream(lb.getLinks())
                    .map(URL::toString)
                    .forEach(link -> {
                        if (!visited.contains(link) && visited.size() < maxPages) {
                            queue.add(link);
                            visited.add(link);
                            indexer.accept(link);
                        }
                    });
        }
    }
}
