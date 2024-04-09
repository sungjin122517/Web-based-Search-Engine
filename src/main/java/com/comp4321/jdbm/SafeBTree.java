package com.comp4321.jdbm;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;

import jdbm.RecordManager;
import jdbm.btree.BTree;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

public class SafeBTree<K, V> implements Iterable<Entry<K, V>> {
    private final BTree btree;

    public SafeBTree(RecordManager recman, String name, Comparator<K> comparator) throws IOException {
        long recid = recman.getNamedObject(name);
        if (recid != 0) {
            btree = BTree.load(recman, recid);
        } else {
            btree = BTree.createInstance(recman, comparator);
            recman.setNamedObject(name, btree.getRecid());
        }
    }

    public V find(K key) throws IOException {
        @SuppressWarnings("unchecked")
        final var value = (V) btree.find(key);
        return value;
    }

    public void insert(K key, V value) throws IOException {
        btree.insert(key, value, true);
    }

    public void remove(K key) throws IOException {
        if (btree.find(key) != null)
            btree.remove(key);
    }

    public int size() {
        return btree.size();
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        try {
            return new Iterator<Entry<K, V>>() {
                private final TupleBrowser browser = btree.browse();
                private final Tuple curTuple = new Tuple();
                private Boolean cachedHasNext = null;

                @Override
                public boolean hasNext() {
                    try {
                        if (cachedHasNext != null)
                            return cachedHasNext;

                        cachedHasNext = browser.getNext(curTuple);
                        return cachedHasNext;
                    } catch (IOException e) {
                        throw new JDBMException("Error while iterating over BTree", e);
                    }
                }

                @Override
                public Entry<K, V> next() {
                    if (!hasNext())
                        throw new NoSuchElementException();
                    cachedHasNext = null;

                    return new Entry<>() {
                        @SuppressWarnings("unchecked")
                        private final K key = (K) curTuple.getKey();
                        @SuppressWarnings("unchecked")
                        private final V value = (V) curTuple.getValue();

                        @Override
                        public K getKey() {
                            return key;
                        }

                        @Override
                        public V getValue() {
                            return value;
                        }

                        @Override
                        public V setValue(V value) {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public boolean equals(Object obj) {
                            if (!(obj instanceof Entry))
                                return false;

                            return getKey().equals(((Entry<?, ?>) obj).getKey())
                                    && getValue().equals(((Entry<?, ?>) obj).getValue());
                        }

                        @Override
                        public int hashCode() {
                            return Objects.hash(getKey(), getValue());
                        }
                    };

                }
            };
        } catch (IOException e) {
            throw new JDBMException("Error while iterating over BTree", e);
        }
    }
}
