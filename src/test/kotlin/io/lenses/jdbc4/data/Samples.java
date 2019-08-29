package io.lenses.jdbc4.data;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Samples {

    Connection conn = DriverManager.getConnection("jdbc:lenses:kafka:https://localhost:3030", "user", "pass");

    public Samples() throws SQLException {
    }

    public void preparedInsert() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:lenses:kafka:https://localhost:3030", "user", "pass");
        PreparedStatement stmt = conn.prepareStatement("INSERT INTO mytopic (name, city, lat, long) VALUES (?, ?, ?, ?)");
        stmt.setString(1, "Tyrian Lannister");
        stmt.setString(2, "Kings Landing");
        stmt.setDouble(3, 67.5);
        stmt.setDouble(4, -41.2);
        stmt.execute();
    }

    public void loopedPreparedInsert() throws SQLException {

        List<String> characters = Arrays.asList("Tyrian Lannister", "Cersei Lannister", "Tywin Lannister");

        Connection conn = DriverManager.getConnection("jdbc:lenses:kafka:https://localhost:3030", "user", "pass");
        PreparedStatement stmt = conn.prepareStatement("INSERT INTO mytopic (name, city, lat, long) VALUES (?, ?, ?, ?)");
        stmt.setString(2, "Kings Landing");
        stmt.setDouble(3, 67.5);
        stmt.setDouble(4, -41.2);

        for (String character : characters) {
            stmt.setString(1, character);
            stmt.execute();
        }

        stmt.close();
    }

    public void batchedLoopedPreparedInsert() throws SQLException {

        List<String> characters = Arrays.asList("Tyrian Lannister", "Cersei Lannister", "Tywin Lannister");

        Connection conn = DriverManager.getConnection("jdbc:lenses:kafka:https://localhost:3030", "user", "pass");
        PreparedStatement stmt = conn.prepareStatement("INSERT INTO mytopic (name, city, lat, long) VALUES (?, ?, ?, ?)");
        stmt.setString(2, "Kings Landing");
        stmt.setDouble(3, 67.5);
        stmt.setDouble(4, -41.2);

        for (String character : characters) {
            stmt.setString(1, character);
            stmt.addBatch();
        }

        stmt.executeBatch();
        stmt.close();
    }

    public void metadata() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:lenses:kafka:https://localhost:3030", "user", "pass");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM mytopic");
        ResultSetMetaData meta = rs.getMetaData();
        for (int k = 1; k <= meta.getColumnCount(); k++) {
            System.out.println("ColumnName=" + meta.getColumnName(k));
            System.out.println("ColumnType=" + meta.getColumnTypeName(k));
            System.out.println("Nullability=" + meta.isNullable(k));
            System.out.println("Signed=" + meta.isSigned(k));
            System.out.println("Precision=" + meta.getPrecision(k));
            System.out.println("Scale=" + meta.getScale(k));
        }
    }

    public void resultSetWhile() throws SQLException {

        Connection conn = DriverManager.getConnection(
                "jdbc:lenses:kafka:http://localhost:3030",
                "username",
                "pasword");

        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT * FROM mytopic WHERE _ktype='STRING' AND _vtype='AVRO'");

        while (rs.next()) {
            System.out.println(rs.getString("name"));
            System.out.println(rs.getInt("age"));
            System.out.println(rs.getString("location"));
        }
    }

    public void resultSetOffset() throws SQLException {
        Connection conn = DriverManager.getConnection(
                "jdbc:lenses:kafka:http://localhost:3030",
                "username",
                "pasword");

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM mytopic WHERE _ktype='STRING' AND _vtype='AVRO'");

        rs.last();
        System.out.println(rs.getString("name"));

        rs.first();
        System.out.println(rs.getString("name"));

        while (rs.next()) {
            System.out.println(rs.getString("name"));
            System.out.println(rs.getInt("age"));
            System.out.println(rs.getString("location"));
        }
    }

    public void tableMeta() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getTables(null, null, "sometable", null);
        while (rs.next()) {
            System.out.println("Table=" + rs.getString(3));
            System.out.println("Type=" + rs.getString(4));
        }
    }

    public void columnMeta() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getColumns(null, null, "sometable", "name*");
        while (rs.next()) {
            System.out.println("Table=" + rs.getString(3));
            System.out.println("Column=" + rs.getString(4));
            System.out.println("Datatype=" + rs.getString(5));
        }
    }

    public void weakConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "myuser");
        props.setProperty("password", "mypass");
        props.setProperty("weakssl", "true");
        Connection conn = DriverManager.getConnection(
                "jdbc:lenses:kafka:http://localhost:3030",
                props);
    }

    public void nestedExample() throws SQLException {
        Connection conn = DriverManager.getConnection(
                "jdbc:lenses:kafka:http://localhost:3030",
                "username",
                "pasword");

        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT name, address.postcode FROM mytopic WHERE _ktype='STRING' AND _vtype='AVRO'");
        while (rs.next()) {
            System.out.println(rs.getString("name"));
            System.out.println(rs.getString("address.postcode"));
        }
    }

    public void insert() throws SQLException {
        Statement stmt = conn.createStatement();
        int result = stmt.executeUpdate("INSERT INTO mytopic (name, city, lat, long) VALUES ('James T Kirk', 'Iowa City', 43.3, -54.2)");
        stmt.getResultSet();
    }

    public void preparedStatementmeta() throws SQLException {

        Map<String, Object> values = new HashMap<>();
        values.put("name", "Walter White");
        values.put("city", "Albuquerque");
        values.put("lat", 51.0);
        values.put("long", 12.3);

        Connection conn = DriverManager.getConnection("jdbc:lenses:kafka:https://localhost:3030", "user", "pass");
        PreparedStatement stmt = conn.prepareStatement("INSERT INTO mytopic (name, city, lat, long) VALUES (?, ?, ?, ?)");
        ResultSetMetaData meta = stmt.getMetaData();
        for (int k = 1; k <= meta.getColumnCount(); k++) {
            String columnName = meta.getColumnName(k);
            Object value = values.get(columnName);
            stmt.setObject(k, value);
        }
        stmt.execute();
    }
}
