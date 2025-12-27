package com.crio.xpoll.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.crio.xpoll.model.User;
import com.crio.xpoll.util.DatabaseConnection;

/**
 * Data Access Object (DAO) for managing users in the XPoll application.
 * Provides methods for creating and retrieving user information.
 */
public class UserDAO {

    private final DatabaseConnection databaseConnection;

    /**
     * Constructs a UserDAO with the specified DatabaseConnection.
     *
     * @param databaseConnection The DatabaseConnection to be used for database operations.
     */
    public UserDAO(DatabaseConnection databaseConnection) {
        this.databaseConnection = databaseConnection;
    }

    /**
     * Creates a new user with the specified username and password.
     *
     * @param username The username for the new user.
     * @param password The password for the new user.
     * @return A User object representing the created user.
     * @throws SQLException If a database error occurs during user creation.
     */
    public User createUser(String username, String password) throws SQLException {

        String sql = "INSERT INTO users (username, password) VALUES (?, ?)";

        try (Connection conn = databaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     sql, Statement.RETURN_GENERATED_KEYS)) {

            ps.setString(1, username);
            ps.setString(2, password); // (hash later)

            ps.executeUpdate();

            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    int userId = rs.getInt(1);
                    return new User(userId, username, password);
                }
            }
        }

        throw new SQLException("Failed to create user");
    }

    /**
     * Retrieves a user by their ID.
     *
     * @param userId The ID of the user to retrieve.
     * @return A User object representing the user with the specified ID, or null if no user is found.
     * @throws SQLException If a database error occurs during the retrieval.
     */
    public User getUserById(int userId) throws SQLException {

        String sql = "SELECT user_id, username FROM users WHERE user_id = ?";

        try(Connection conn = databaseConnection.getConnection();
            PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, userId);

            try(ResultSet rs = ps.executeQuery()){
                if(rs.next()){
                    int id = rs.getInt("user_id");
                    String userName = rs.getString("username");
                    String password = rs.getString("password");

                    return new User(id, userName, password);
                }
            }


        }


        return null;
    }
}