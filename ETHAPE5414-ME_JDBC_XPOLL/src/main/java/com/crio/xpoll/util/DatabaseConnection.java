package com.crio.xpoll.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnection {
    private String url;
    private String username;
    private String password;
    private String driverClassName;
    private Connection connection;
    private static DatabaseConnection instance;

    private DatabaseConnection(String url, String username, String password, String driverClassName) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.driverClassName = driverClassName;

        // Load the JDBC driver
        try {
            Class.forName(this.driverClassName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        // Register shutdown hook to close the connection
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (connection != null) {
                try {
                    connection.close();
                    System.out.println("Database connection closed.");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }));
    }

    public static DatabaseConnection getInstance(String url, String username, String password, String driverClassName) {
        if (instance == null) {
            synchronized (DatabaseConnection.class) {
                if (instance == null) {
                    instance = new DatabaseConnection(url, username, password, driverClassName);
                }
            }
        }
        return instance;
    }

    public Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(url, username, password);
        }
        return connection;
    }
}