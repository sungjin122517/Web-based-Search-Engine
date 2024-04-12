package com.comp4321;

import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public record SearchResult(Double score, String title, String url, ZonedDateTime lastModified, Long pageSize,
        Map<String, Integer> keywords, Set<String> parentLinks, Set<String> childLinks) {

    public static final Integer MAX_KEYWORD_COUNT = 5;

    public SearchResult {
        Objects.requireNonNull(score);
        Objects.requireNonNull(title);
        Objects.requireNonNull(url);
        Objects.requireNonNull(lastModified);
        Objects.requireNonNull(pageSize);
        Objects.requireNonNull(keywords);
        Objects.requireNonNull(parentLinks);
        Objects.requireNonNull(childLinks);

        if (score < 0)
            throw new IllegalArgumentException("Score must be non-negative");
    }

    /**
     * Converts the search result object to a formatted string representation.
     *
     * @return The formatted string representation of the search result.
     */
    public String toResultFormat() {
        /*
         * score title
         * url
         * lastModified, pageSize
         * keyword1 count1, keyword2 count2, ...
         * parentLink1
         * parentLink2
         * ...
         * childLink1
         * childLink2
         * ...
         */

        final var sb = new StringBuilder();

        sb.append(String.format("%.4f", score));
        sb.append('\t');

        sb.append(title);
        sb.append('\n');

        sb.append('\t');
        sb.append(url);
        sb.append('\n');

        sb.append('\t');
        sb.append(lastModified);
        sb.append(", ");
        sb.append(pageSize);
        sb.append('\n');

        sb.append('\t');
        keywords().entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()))
                .limit(MAX_KEYWORD_COUNT)
                .forEach(e -> {
                    sb.append(e.getKey());
                    sb.append(' ');
                    sb.append(e.getValue());
                    sb.append("; ");
                });
        sb.append('\n');

        sb.append("\tParent Links:\n");
        parentLinks().forEach(link -> {
            sb.append('\t');
            sb.append(link);
            sb.append('\n');
        });

        sb.append("\tChild Links:\n");
        childLinks().forEach(link -> {
            sb.append('\t');
            sb.append(link);
            sb.append('\n');
        });

        return sb.toString();
    }

    private String toHTMLLink(String link, String text) {
        return "<a href=\"" + link + "\">" + text + "</a>";
    }

    /**
     * Converts the SearchResult object to an HTML representation.
     * The HTML representation includes the score, title, URL, last modified date,
     * page size,
     * keywords and their counts, parent links, and child links.
     *
     * @return The HTML representation of the SearchResult object.
     */
    public String toHTML() {
        /*
         * score title
         * url
         * lastModified, pageSize
         * keyword1 count1, keyword2 count2, ...
         * parentLink1
         * parentLink2
         * ...
         * childLink1
         * childLink2
         * ...
         */

        final var sb = new StringBuilder();

        sb.append("<li>");

        sb.append("<h3>");
        sb.append(String.format("%.4f", score));
        sb.append(" ");
        sb.append(toHTMLLink(url, title));
        sb.append("</h3>");

        sb.append(toHTMLLink(url, url));

        sb.append("<p>");
        sb.append(lastModified);
        sb.append(", ");
        sb.append(pageSize);
        sb.append("</p>");

        sb.append("<p>");
        sb.append("Keywords: ");
        keywords().entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()))
                .limit(MAX_KEYWORD_COUNT)
                .forEach(e -> {
                    sb.append(e.getKey());
                    sb.append(' ');
                    sb.append(e.getValue());
                    sb.append("; ");
                });
        sb.append("</p>");

        sb.append("<p>Parent Links:</p>");
        sb.append("<ul>");
        parentLinks().forEach(link -> {
            sb.append("<li>");
            sb.append(toHTMLLink(link, link));
            sb.append("</li>");
        });
        sb.append("</ul>");

        sb.append("<p>Child Links:</p>");
        sb.append("<ul>");
        childLinks().forEach(link -> {
            sb.append("<li>");
            sb.append(toHTMLLink(link, link));
            sb.append("</li>");
        });
        sb.append("</ul>");

        sb.append("</li>");

        return sb.toString();
    }
}
