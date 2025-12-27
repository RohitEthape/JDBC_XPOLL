package com.crio.xpoll.model;

public class PollSummary {
    private String question;
    private String choiceText;
    private int responseCount;

    public PollSummary(String question, String choiceText, int responseCount) {
        this.question = question;
        this.choiceText = choiceText;
        this.responseCount = responseCount;
    }

    public String getQuestion() {
        return question;
    }

    public String getChoiceText() {
        return choiceText;
    }

    public int getResponseCount() {
        return responseCount;
    }


}
