package testDBConnection;

import java.sql.*;

public class MainDBTest {

    // Establishes database connection
    public Connection connectToDatabase(String databaseUrl) throws SQLException {
        return DriverManager.getConnection(databaseUrl);
    }

    // Task 1: Reading and Printing Database Structure and Data
    public void printDatabaseStructureAndData(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});

        while (tables.next()) {
            String tableName = tables.getString("TABLE_NAME");
            System.out.println("Table: " + tableName);

            ResultSet columns = metaData.getColumns(null, null, tableName, "%");
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String columnType = columns.getString("TYPE_NAME");
                System.out.println("    Column: " + columnName + " Type: " + columnType);
            }

            printTableData(connection, tableName);
        }
    }

    // Retrieves and prints data from a specific table
    private void printTableData(Connection connection, String tableName) throws SQLException {
        String query = "SELECT * FROM " + tableName;
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) System.out.print(", ");
                    String columnValue = resultSet.getString(i);
                    System.out.print(metaData.getColumnName(i) + ": " + columnValue);
                }
                System.out.println();
            }
        }
    }

    // Task 2: Using PreparedStatement
    public void insertDataUsingPreparedStatement(Connection connection) throws SQLException {
        String insertSQL = "INSERT INTO EmployeeData (Emp_Num, Email_Work, Email_Personal, WorkCellNumber) VALUES (?, ?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            pstmt.setInt(1, 145);
            pstmt.setString(2, "adeena@touro");
            pstmt.setString(3, "adeena@gmail");
            pstmt.setString(4, "9172732579");
            pstmt.executeUpdate();

            // Insert more data with different values
            pstmt.setInt(1, 155);
            pstmt.setString(2, "shira@touro");
            pstmt.setString(3, "shira@gmail");
            pstmt.setString(4, "7185753004");
            pstmt.executeUpdate();

            pstmt.setInt(1, 165);
            pstmt.setString(2, "ahuva@touro");
            pstmt.setString(3, "ahuva@gmail");
            pstmt.setString(4, "9172575031");
            pstmt.executeUpdate();
        }
    }

    // Task 3: Working with Savepoints and Rollbacks
    public void demonstrateSavepointsAndRollbacks(Connection connection) throws SQLException {
        Savepoint savepoint = connection.setSavepoint();
        try (Statement statement = connection.createStatement()) {
            String updateSQL = "UPDATE Parts SET Units_On_Hand = Units_On_Hand - 10 WHERE part_Num = 'A100'";
            statement.executeUpdate(updateSQL);

            // Simulate an error to trigger a rollback
            // Uncomment to see the rollback in action
            // if (true) throw new SQLException("Simulated error");

            connection.commit();
        } catch (SQLException e) {
            connection.rollback(savepoint);
            System.out.println("Rolled back to savepoint due to: " + e.getMessage());
        }

        connection.commit();
        System.out.println("Changes committed successfully");
    }

    public static void main(String[] args) {
        final String DATABASE_URL =
                "jdbc:sqlserver://localhost:1433;" +
                        "databaseName=PREMIERECO;integratedSecurity=true;encrypt=true;TrustServerCertificate=true";

        MainDBTest dbInteraction = new MainDBTest();
        try (Connection connection = dbInteraction.connectToDatabase(DATABASE_URL)) {
            System.out.println("Connected successfully");

            // Task 1
            dbInteraction.printDatabaseStructureAndData(connection);

            // Task 2
            dbInteraction.insertDataUsingPreparedStatement(connection);

            // Task 3
            dbInteraction.demonstrateSavepointsAndRollbacks(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
