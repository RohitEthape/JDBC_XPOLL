package com.crio.xpoll.model;

public class Choice {
    private int id;
    private int pollId;
    private String choiceText;

    public Choice(int id, int pollId, String choiceText) {
        this.id = id;
        this.pollId = pollId;
        this.choiceText = choiceText;
    }

    public int getId() {
        return id;
    }

    public int getPollId() {
        return pollId;
    }

    public String getChoiceText() {
        return choiceText;
    }
}
