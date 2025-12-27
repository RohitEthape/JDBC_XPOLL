package com.crio.xpoll.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.crio.xpoll.model.Response;
import com.crio.xpoll.util.DatabaseConnection;

/**
 * Data Access Object (DAO) for managing responses in the XPoll application.
 * Provides methods for creating responses to polls.
 */
public class ResponseDAO {
    private final DatabaseConnection databaseConnection;

    /**
     * Constructs a ResponseDAO with the specified DatabaseConnection.
     *
     * @param databaseConnection The DatabaseConnection to be used for database operations.
     */
    public ResponseDAO(DatabaseConnection databaseConnection) {
        this.databaseConnection = databaseConnection;
    }

    /**
     * Creates a new response for a specified poll, choice, and user.
     *
     * @param pollId   The ID of the poll to which the response is made.
     * @param choiceId The ID of the choice selected by the user.
     * @param userId   The ID of the user making the response.
     * @return A Response object representing the created response.
     * @throws SQLException If a database error occurs during response creation.
     */
    public Response createResponse(int pollId, int choiceId, int userId) throws SQLException {

        String pollCheckSql = "SELECT is_closed FROM polls WHERE id = ?";
        String choiceCheckSql = "SELECT id FROM choices WHERE id = ? AND poll_id = ?";
        String insertSql = "INSERT INTO responses (poll_id, choice_id, user_id) VALUES (?, ?, ?)";

        Connection conn = null;

        try {
            conn = databaseConnection.getConnection();
            conn.setAutoCommit(false); // start transaction

            // 1️⃣ Check if poll exists and is open
            try (PreparedStatement ps = conn.prepareStatement(pollCheckSql)) {
                ps.setInt(1, pollId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) throw new SQLException("Poll not found");
                    if (rs.getBoolean("is_closed")) throw new SQLException("Poll is closed");
                }
            }

            // 2️⃣ Check if choice exists for this poll
            try (PreparedStatement ps = conn.prepareStatement(choiceCheckSql)) {
                ps.setInt(1, choiceId);
                ps.setInt(2, pollId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) throw new SQLException("Choice does not exist for this poll");
                }
            }

            // 3️⃣ Insert response
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                ps.setInt(1, pollId);
                ps.setInt(2, choiceId);
                ps.setInt(3, userId);
                ps.executeUpdate();
            }

            conn.commit(); // commit transaction

            return new Response(pollId, choiceId, userId);

        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback(); // rollback on exception
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            throw e; // rethrow original exception
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true); // reset autoCommit
                    conn.close();              // close connection
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}