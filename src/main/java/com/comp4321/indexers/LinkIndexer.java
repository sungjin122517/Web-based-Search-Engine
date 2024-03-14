package com.comp4321.indexers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.comp4321.jdbm.SafeHTree;

public class LinkIndexer {
    private final SafeHTree<Integer, Set<Integer>> parentToChild;
    private final SafeHTree<Integer, Set<Integer>> childToParent;
    private final int maxPages;

    public LinkIndexer(SafeHTree<Integer, Set<Integer>> parentToChild, SafeHTree<Integer, Set<Integer>> childToParent,
            int maxPages) {
        this.parentToChild = parentToChild;
        this.childToParent = childToParent;
        this.maxPages = maxPages;
    }

    private void addChildLink(int parent, int child) {
        if (parent == child)
            return;
        if (parent > maxPages || child > maxPages)
            return;

        final var childKey = Integer.valueOf(child);

        try {
            var parentsValue = childToParent.get(childKey);
            if (parentsValue == null)
                parentsValue = new HashSet<Integer>();

            parentsValue.add(parent);
            childToParent.put(childKey, parentsValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void removeChildLink(int parent, int child) {
        if (parent == child)
            return;
        if (parent > maxPages || child > maxPages)
            return;

        final var childKey = Integer.valueOf(child);

        try {
            final var parentsValue = childToParent.get(childKey);
            if (parentsValue == null)
                return;

            parentsValue.remove(parent);
            childToParent.put(childKey, parentsValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setLinks(int docId, Set<Integer> links) {
        if (docId > maxPages)
            return;

        try {
            // Get and remove old links
            final var oldLinks = parentToChild.get(Integer.valueOf(docId));
            if (oldLinks != null)
                oldLinks.stream().forEach(child -> removeChildLink(docId, child));

            // Overwrite with new links
            parentToChild.put(Integer.valueOf(docId), links);
            links.stream().forEach(child -> addChildLink(docId, child));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void printAll() throws IOException {
        System.out.println("PARENT_TO_CHILD:");
        for (final var entry : parentToChild) {
            System.out.println(entry.getKey() + " -> " + entry.getValue().toString());
        }
        System.out.println();

        System.out.println("CHILD_TO_PARENT:");
        for (final var entry : childToParent) {
            System.out.println(entry.getKey() + " -> " + entry.getValue().toString());
        }
        System.out.println();
    }
}
