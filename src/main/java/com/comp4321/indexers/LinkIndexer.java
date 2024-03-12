package com.comp4321.indexers;

import java.io.IOException;
import java.util.HashSet;

import jdbm.htree.HTree;

public class LinkIndexer {
    private final HTree parentToChild;
    private final HTree childToParent;
    private final int maxPages;

    public LinkIndexer(HTree parentToChild, HTree childToParent, int maxPages) {
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
            @SuppressWarnings("unchecked")
            var parentsValue = (HashSet<Integer>) childToParent.get(childKey);
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
            @SuppressWarnings("unchecked")
            final var parentsValue = (HashSet<Integer>) childToParent.get(childKey);
            if (parentsValue == null)
                return;

            parentsValue.remove(parent);
            childToParent.put(childKey, parentsValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setLinks(int docId, HashSet<Integer> links) {
        if (docId > maxPages)
            return;

        try {
            // Get and remove old links
            @SuppressWarnings("unchecked")
            final var oldLinks = (HashSet<Integer>) parentToChild.get(Integer.valueOf(docId));
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
        final var parentKeys = parentToChild.keys();
        var parentKey = (Integer) parentKeys.next();
        while (parentKey != null) {
            @SuppressWarnings("unchecked")
            final var childEntry = (HashSet<Integer>) parentToChild.get(parentKey);
            System.out.println(parentKey + " -> " + childEntry.toString());

            parentKey = (Integer) parentKeys.next();
        }
        System.out.println();

        System.out.println("CHILD_TO_PARENT:");
        final var childKeys = childToParent.keys();
        var childKey = (Integer) childKeys.next();
        while (childKey != null) {
            @SuppressWarnings("unchecked")
            final var parentEntry = (HashSet<Integer>) childToParent.get(childKey);
            System.out.println(childKey + " -> " + parentEntry.toString());

            childKey = (Integer) childKeys.next();
        }
        System.out.println();
    }
}
