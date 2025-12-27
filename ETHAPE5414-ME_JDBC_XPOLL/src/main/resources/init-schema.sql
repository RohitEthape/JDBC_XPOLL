-- Drop the database if it exists
DROP DATABASE IF EXISTS xpoll;

-- Create the database
CREATE DATABASE xpoll;

-- Use the created database
USE xpoll;

-- Create users table
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    password VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create polls table
CREATE TABLE polls (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    question VARCHAR(255) NOT NULL,
    is_closed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Create poll choices table
CREATE TABLE choices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    poll_id INT NOT NULL,
    choice_text VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (poll_id) REFERENCES polls(id) ON DELETE CASCADE
);

-- Create responses table
CREATE TABLE responses (
    poll_id INT NOT NULL,
    choice_id INT NOT NULL,
    user_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (poll_id,choice_id,user_id),
    FOREIGN KEY (poll_id) REFERENCES polls(id) ON DELETE CASCADE,
    FOREIGN KEY (choice_id) REFERENCES choices(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Create a view for poll summaries
CREATE VIEW poll_summaries AS
SELECT p.id AS poll_id, p.question, c.choice_text, COUNT(r.poll_id) AS response_count
FROM polls p
JOIN choices c ON p.id = c.poll_id
LEFT JOIN responses r ON c.id = r.choice_id
GROUP BY p.id, p.question, c.choice_text, c.id;