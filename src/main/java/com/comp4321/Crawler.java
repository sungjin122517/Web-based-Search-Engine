package com.comp4321;

import java.net.URL;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.Vector;

import org.htmlparser.beans.LinkBean;
import org.htmlparser.beans.StringBean;
import org.htmlparser.util.ParserException;

public class Crawler {
    public final String url;

    Crawler(String _url) {
        url = _url;
    }

    public Vector<String> extractWords() throws ParserException {
        final var sb = new StringBean();
        sb.setURL(url);
        sb.setLinks(false);

        final var words = new Vector<String>();
        final var tokenizer = new StringTokenizer(sb.getStrings());
        tokenizer.asIterator().forEachRemaining(t -> words.add((String) t));

        return words;
    }

    public Vector<String> extractLinks() throws ParserException {
        final var lb = new LinkBean();
        lb.setURL(url);

        return Arrays.stream(lb.getLinks()).map(URL::toString).collect(Vector::new, Vector::add, Vector::addAll);
    }
}
