package com.crio.xpoll.model;

import java.util.List;

public class Poll {
    private int id;
    private int userId;
    private String question;
    private boolean isClosed;
    private List<Choice> choices;

    public Poll(int id, int userId, String question, List<Choice> choices, boolean isClosed) {
        this.id = id;
        this.userId = userId;
        this.question = question;
        this.choices = choices;
        this.isClosed = isClosed;
    }

    public Poll(int id, int userId, String question, List<Choice> choices) {
        this.id = id;
        this.userId = userId;
        this.question = question;
        this.choices = choices;
        this.isClosed = false;
    }

    public int getId() {
        return id;
    }

    public int getUserId() {
        return userId;
    }

    public String getQuestion() {
        return question;
    }

    public List<Choice> getChoices(){
        return choices;
    }

    public boolean isClosed() {
        return isClosed;
    }

}

