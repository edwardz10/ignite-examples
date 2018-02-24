package org.apache.ignite.sqlquery;

import java.sql.*;

public class SqlQuery {

    private static void createTables(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {

            // Create table based on REPLICATED template.
            stmt.executeUpdate("CREATE TABLE City (" +
                    " id LONG PRIMARY KEY, name VARCHAR) " +
                    " WITH \"template=replicated\"");

            // Create table based on PARTITIONED template with one backup.
            stmt.executeUpdate("CREATE TABLE Person (" +
                    " id LONG, name VARCHAR, city_id LONG, " +
                    " PRIMARY KEY (id, city_id)) " +
                    " WITH \"backups=1, affinityKey=city_id\"");

            // Create an index on the City table.
            stmt.executeUpdate("CREATE INDEX idx_city_name ON City (name)");

            // Create an index on the Person table.
            stmt.executeUpdate("CREATE INDEX idx_person_name ON Person (name)");
        }
    }

    private static void populateData(Connection connection) throws SQLException {
        // Populate City table
        try (PreparedStatement stmt =
                     connection.prepareStatement("INSERT INTO City (id, name) VALUES (?, ?)")) {

            stmt.setLong(1, 1L);
            stmt.setString(2, "Forest Hill");
            stmt.executeUpdate();

            stmt.setLong(1, 2L);
            stmt.setString(2, "Denver");
            stmt.executeUpdate();

            stmt.setLong(1, 3L);
            stmt.setString(2, "St. Petersburg");
            stmt.executeUpdate();
        }

        // Populate Person table
        try (PreparedStatement stmt =
                     connection.prepareStatement("INSERT INTO Person (id, name, city_id) VALUES (?, ?, ?)")) {

            stmt.setLong(1, 1L);
            stmt.setString(2, "John Doe");
            stmt.setLong(3, 3L);
            stmt.executeUpdate();

            stmt.setLong(1, 2L);
            stmt.setString(2, "Jane Roe");
            stmt.setLong(3, 2L);
            stmt.executeUpdate();

            stmt.setLong(1, 3L);
            stmt.setString(2, "Mary Major");
            stmt.setLong(3, 1L);
            stmt.executeUpdate();

            stmt.setLong(1, 4L);
            stmt.setString(2, "Richard Miles");
            stmt.setLong(3, 2L);
            stmt.executeUpdate();
        }
    }

    private static void queryData(Connection connection) throws SQLException {
        // Get data
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs =
                         stmt.executeQuery("SELECT p.name, c.name " +
                                 " FROM Person p, City c " +
                                 " WHERE p.city_id = c.id")) {

                while (rs.next())
                    System.out.println(rs.getString(1) + ", " + rs.getString(2));
            }
        }
    }

    public static void main(String[] args) {
        try {
            // Register JDBC driver.
            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

            // Open JDBC connection.
            Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");

            createTables(conn);
            populateData(conn);
            queryData(conn);
        } catch (Exception e) {
            System.err.println("Failed to run: " + e);
        }
    }
}
