package com.crio.xpoll.model;

public class Response {
    private int pollId;
    private int choiceId;
    private int userId;

    public Response(int pollId, int choiceId, int userId) {
        this.pollId = pollId;
        this.choiceId = choiceId;
        this.userId = userId;
    }

    public int getPollId() {
        return pollId;
    }

    public int getChoiceId() {
        return choiceId;
    }

    public int getUserId() {
        return userId;
    }
}
