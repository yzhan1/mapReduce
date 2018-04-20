package models;

import java.io.Serializable;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
public class Word implements Serializable {
    private String word;
    private String positions;

    public Word() {
    }

    public Word(String w, String p) {
        this();
        setWord(w);
        setPositions(p);
    }

    public String getWord() {
        return word;
    }

    public String getPositions() {
        return positions;
    }

    public void setWord(String w) {
        word = w;
    }

    public void setPositions(String a) {
        positions = a;
    }
}