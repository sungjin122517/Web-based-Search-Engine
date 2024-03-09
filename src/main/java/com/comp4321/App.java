package com.comp4321;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class App {
    public static void main(String[] arg) {
        StopStem stopStem = new StopStem("stopwords.txt");
        String input = "";
        try {
            do {
                System.out.print("Please enter a single English word: ");
                BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
                input = in.readLine();
                if (input.length() > 0) {
                    if (stopStem.isStopWord(input))
                        System.out.println("It should be stopped");
                    else
                        System.out.println("The stem of it is \"" + stopStem.stem(input) + "\"");
                }
            } while (input.length() > 0);
        } catch (IOException ioe) {
            System.err.println(ioe.toString());
        }
    }
}
