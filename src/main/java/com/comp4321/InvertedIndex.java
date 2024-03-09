package com.comp4321;

import java.io.IOException;
import java.util.ArrayList;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.htree.HTree;

public class InvertedIndex {
    private RecordManager recman;
    private HTree hashtable;

    InvertedIndex(String recordmanager, String objectname) throws IOException {
        recman = RecordManagerFactory.createRecordManager(recordmanager);
        long recid = recman.getNamedObject(objectname);

        if (recid != 0) {
            hashtable = HTree.load(recman, recid);
        } else {
            hashtable = HTree.createInstance(recman);
            recman.setNamedObject("ht1", hashtable.getRecid());
        }
    }

    @Override
    public void finalize() throws IOException {
        recman.commit();
        recman.close();
    }

    public void addEntry(String word, int x, int y) throws IOException {
        @SuppressWarnings("unchecked")
        var entry = (ArrayList<Posting>) hashtable.get(word);
        if (entry == null)
            entry = new ArrayList<>();

        final var to_add = new Posting("doc" + x, y);
        entry.add(to_add);

        hashtable.put(word, entry);
    }

    public void delEntry(String word) throws IOException {
        hashtable.remove(word);
    }

    public void printAll() throws IOException {
        final var keys = hashtable.keys();
        var key = (String) keys.next();
        while (key != null) {
            System.out.print(key + " = ");

            @SuppressWarnings("unchecked")
            final var entry = (ArrayList<Posting>) hashtable.get(key);
            System.out.println(String.join(" ", entry.stream().map(Posting::toString).toList()));

            key = (String) keys.next();
        }
    }

}
