package com.crio.xpoll.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseSetup {

    public static void executeSQLScript(DatabaseConnection dbConnection) {
        try (Connection conn = dbConnection.getConnection();
             Statement stmt = conn.createStatement();
             BufferedReader reader = new BufferedReader(new InputStreamReader(
                     DatabaseSetup.class.getResourceAsStream("/init-schema.sql")))) {

            StringBuilder sql = new StringBuilder();
            String line;

            while ((line = reader.readLine()) != null) {
                sql.append(line);
                sql.append("\n");
            }

            String[] statements = sql.toString().split(";");
            for (String statement : statements) {
                if (!statement.trim().isEmpty()) {
                    stmt.execute(statement);
                }
            }

            System.out.println("Database setup completed.");

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
