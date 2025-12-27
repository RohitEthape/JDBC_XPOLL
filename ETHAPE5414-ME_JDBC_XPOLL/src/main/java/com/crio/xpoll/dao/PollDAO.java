package com.crio.xpoll.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.crio.xpoll.model.Choice;
import com.crio.xpoll.model.Poll;
import com.crio.xpoll.model.PollSummary;
import com.crio.xpoll.util.DatabaseConnection;

/**
 * Data Access Object (DAO) for managing polls in the XPoll application.
 * Provides methods for creating, retrieving, closing polls, and fetching poll summaries.
 */
public class PollDAO {

    private final DatabaseConnection databaseConnection;

    /**
     * Constructs a PollDAO with the specified DatabaseConnection.
     *
     * @param databaseConnection The DatabaseConnection to be used for database operations.
     */
    public PollDAO(DatabaseConnection databaseConnection) {
        this.databaseConnection = databaseConnection;
    }

    /**
     * Creates a new poll with the specified question and choices.
     *
     * @param userId   The ID of the user creating the poll.
     * @param question The question for the poll.
     * @param choices  A list of choices for the poll.
     * @return The created Poll object with its associated choices.
     * @throws SQLException If a database error occurs during poll creation.
     */
    public Poll createPoll(int userId, String question, List<String> choiceTexts) throws SQLException {

        String pollSql = "INSERT INTO polls (user_id, question, is_closed) VALUES (?, ?, FALSE)";

        String choiceSql = "INSERT INTO choices (poll_id, choice_text) VALUES (?, ?)";

        Connection conn = null;

        try {
            conn = databaseConnection.getConnection();
            conn.setAutoCommit(false);

            // 1️⃣ Insert poll
            int pollId;
            try (PreparedStatement pollStmt = conn.prepareStatement(pollSql, Statement.RETURN_GENERATED_KEYS)) {
                pollStmt.setInt(1, userId);
                pollStmt.setString(2, question);
                pollStmt.executeUpdate();

                try (ResultSet rs = pollStmt.getGeneratedKeys()) {
                    if (!rs.next()) throw new SQLException("Failed to create poll");
                    pollId = rs.getInt(1);
                }
            }

            // 2️⃣ Insert choices
            List<Choice> choices = new ArrayList<>();
            try (PreparedStatement choiceStmt = conn.prepareStatement(choiceSql, Statement.RETURN_GENERATED_KEYS)) {
                for (String choiceText : choiceTexts) {
                    choiceStmt.setInt(1, pollId);
                    choiceStmt.setString(2, choiceText);
                    choiceStmt.addBatch();
                }
                choiceStmt.executeBatch();

                // Get generated choice IDs
                try (ResultSet rs = choiceStmt.getGeneratedKeys()) {
                    int index = 0;
                    while (rs.next()) {
                        int choiceId = rs.getInt(1);
                        choices.add(new Choice(choiceId, pollId, choiceTexts.get(index)));
                        index++;
                    }
                }
            }

            conn.commit();

            // 3️⃣ Return Poll object
            return new Poll(pollId, userId, question, choices);

        } catch (SQLException ex) {
            if (conn != null) {
                try { conn.rollback(); } catch (SQLException rollbackEx) { rollbackEx.printStackTrace(); }
            }
            throw ex;

        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException closeEx) {
                    closeEx.printStackTrace();
                }
            }
        }
    }


    /**
     * Retrieves a poll by its ID.
     *
     * @param pollId The ID of the poll to retrieve.
     * @return The Poll object with its associated choices.
     * @throws SQLException If a database error occurs or the poll is not found.
     */
    public Poll getPoll(int pollId) throws SQLException {
        String pollSql = "SELECT id, user_id, question, is_closed FROM polls WHERE id = ?";
        try (Connection conn = databaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(pollSql)) {
            ps.setInt(1, pollId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("Poll not found");
                }
                int id = rs.getInt("id");
                int userId = rs.getInt("user_id");
                String question = rs.getString("question");
                boolean isClosed = rs.getBoolean("is_closed");

                // Fetch choices
                List<Choice> choiceObjects = new ArrayList<>();
                String choiceSql = "SELECT id, choice_text FROM choices WHERE poll_id = ?";
                try (PreparedStatement psChoices = conn.prepareStatement(choiceSql)) {
                    psChoices.setInt(1, id);
                    try (ResultSet rsChoices = psChoices.executeQuery()) {
                        while (rsChoices.next()) {
                            choiceObjects.add(new Choice(rsChoices.getInt("id"), id, rsChoices.getString("choice_text")));
                        }
                    }
                }

                return new Poll(id, userId, question, choiceObjects, isClosed);
            }
        }
    }


    /**
     * Closes a poll by updating its status in the database.
     *
     * @param pollId The ID of the poll to close.
     * @throws SQLException If a database error occurs during the update.
     */
    public void closePoll(int pollId) throws SQLException {
        String sql = "UPDATE polls SET is_closed = TRUE WHERE id = ?";

        try (Connection conn = databaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setInt(1, pollId);

            int updatedRows = ps.executeUpdate();

            if (updatedRows == 0) {
                throw new SQLException("Poll not found with id: " + pollId);
            }
        }
    }

    /**
     * Retrieves a list of poll summaries for the specified poll.
     *
     * @param pollId The ID of the poll for which to retrieve summaries.
     * @return A list of PollSummary objects containing the poll question, choice text, and response count.
     * @throws SQLException If a database error occurs during the query.
     */
    public List<PollSummary> getPollSummaries(int pollId) throws SQLException {
        List<PollSummary> summaries = new ArrayList<>();
        String sql = "SELECT question, choice_text, response_count FROM poll_summaries WHERE poll_id = ?";

        try (Connection conn = databaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, pollId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    summaries.add(new PollSummary(
                            rs.getString("question"),
                            rs.getString("choice_text"),
                            rs.getInt("response_count")
                    ));
                }
            }
        }
        return summaries;
    }



}