package com.crio.xpoll;

import com.crio.xpoll.dao.PollDAO;
import com.crio.xpoll.dao.ResponseDAO;
import com.crio.xpoll.dao.UserDAO;
import com.crio.xpoll.model.Poll;
import com.crio.xpoll.model.PollSummary;
import com.crio.xpoll.model.Response;
import com.crio.xpoll.model.User;
import com.crio.xpoll.util.DatabaseConnection;
import com.crio.xpoll.util.DatabaseSetup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AppTest {

    private DatabaseConnection databaseConnection;
    private UserDAO userDAO;
    private PollDAO pollDAO;
    private ResponseDAO responseDAO;

    @BeforeAll
    public void setupDatabase() throws SQLException {
        // Load properties from application.properties
        Properties properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find application.properties");
                return;
            }
            // Load properties file
            properties.load(input);

            // Get the property values
            String url = properties.getProperty("db.url");
            String username = properties.getProperty("db.username");
            String password = properties.getProperty("db.password");
            String driverClassName = properties.getProperty("jdbc.driverClassName");

            // Initialize database connection
            databaseConnection = DatabaseConnection.getInstance(url, username, password, driverClassName);
            userDAO = new UserDAO(databaseConnection);
            pollDAO = new PollDAO(databaseConnection);
            responseDAO = new ResponseDAO(databaseConnection);

        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        // Setup the database
        DatabaseSetup.executeSQLScript(databaseConnection);
    }

    @BeforeEach
    public void resetDatabase() throws SQLException {
        try (Connection conn = databaseConnection.getConnection()) {
            conn.createStatement().execute("DELETE FROM responses");
            conn.createStatement().execute("DELETE FROM choices");
            conn.createStatement().execute("DELETE FROM polls");
            conn.createStatement().execute("DELETE FROM users");
        }
    }

    @Test
    public void testCreateUser() throws SQLException {
        User user = userDAO.createUser("testUser", "password");
        assertNotNull(user);
        assertEquals("testUser", user.getUsername());
    }

    @Test
    public void testCreatePoll() throws SQLException {
        User user = userDAO.createUser("testUser", "password");
        List<String> choices = Arrays.asList("Option 1", "Option 2", "Option 3");
        Poll poll = pollDAO.createPoll(user.getUserId(), "Sample Question", choices);

        assertNotNull(poll);
        assertEquals(user.getUserId(), poll.getUserId());
        assertEquals("Sample Question", poll.getQuestion());
        assertEquals(3, poll.getChoices().size());
        assertFalse(poll.isClosed());
    }

    @Test
    public void testRespondToPoll() throws SQLException {
        User user = userDAO.createUser("testUser", "password");
        List<String> choices = Arrays.asList("Option 1", "Option 2", "Option 3");
        Poll poll = pollDAO.createPoll(user.getUserId(), "Sample Question", choices);

        Response response = responseDAO.createResponse(poll.getId(), poll.getChoices().get(1).getId(), user.getUserId());

        assertNotNull(response);
        assertEquals(1, response.getPollId());
        assertEquals(2, poll.getChoices().get(1).getId());
        assertEquals(1, response.getUserId());
    }

    @Test
    public void testViewPollSummary() throws SQLException {
        User user = userDAO.createUser("testUser", "password");
        List<String> choices = Arrays.asList("Option 1", "Option 2", "Option 3");
        Poll poll = pollDAO.createPoll(user.getUserId(), "Sample Question", choices);

        responseDAO.createResponse(poll.getId(), poll.getChoices().get(0).getId(), user.getUserId());
        responseDAO.createResponse(poll.getId(), poll.getChoices().get(1).getId(), user.getUserId());
        responseDAO.createResponse(poll.getId(), poll.getChoices().get(2).getId(), user.getUserId());

        List<PollSummary> summaries = pollDAO.getPollSummaries(poll.getId());

        assertEquals(3, summaries.size());
        assertEquals(1, summaries.get(0).getResponseCount());
        assertEquals(1, summaries.get(1).getResponseCount());
        assertEquals(1, summaries.get(2).getResponseCount());
    }

    @Test
    public void testClosePoll() throws SQLException {
        User user = userDAO.createUser("testUser", "password");
        List<String> choices = Arrays.asList("Option 1", "Option 2", "Option 3");
        Poll poll = pollDAO.createPoll(user.getUserId(), "Sample Question", choices);

        pollDAO.closePoll(poll.getId());
        Poll closedPoll = pollDAO.getPoll(poll.getId());

        assertTrue(closedPoll.isClosed());
    }
}