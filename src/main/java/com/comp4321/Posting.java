package com.comp4321;

import java.io.Serializable;

class Posting implements Serializable {
    public String doc;
    public int freq;

    Posting(String doc, int freq) {
        this.doc = doc;
        this.freq = freq;
    }

    @Override
    public String toString() {
        return String.format("%s %d", doc, freq);
    }
}
