-- Create the database
-- CREATE DATABASE flights;
-- \c flights

-- Create the airport table
DROP TABLE IF EXISTS airport;

CREATE TABLE airport(
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    city VARCHAR(255) NOT NULL,
    state VARCHAR(255) NOT NULL
);

-- Create the flight table
DROP TABLE IF EXISTS flight;

CREATE TABLE flight(
    id GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date DATE NOT NULL,
    origin INT NOT NULL,
    destination INT NOT NULL,
    FOREIGN KEY (origin) REFERENCES airport (id),
    FOREIGN KEY (destination) REFERENCES airport (id)
);