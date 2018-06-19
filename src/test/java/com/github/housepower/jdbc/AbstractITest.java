package com.github.housepower.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

public abstract class AbstractITest {

    private static final int SERVER_PORT = Integer.valueOf(System.getProperty("CLICK_HOUSE_SERVER_PORT", "9000"));

    protected void withNewConnection(WithConnection withConnection) throws Exception {

        // deregisterDriver other jdbc drivers
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            DriverManager.deregisterDriver(drivers.nextElement());
        }
        DriverManager.registerDriver(new ClickHouseDriver());

        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://127.0.0.1:" + SERVER_PORT);

        try {
            withConnection.apply(connection);
        } finally {
            connection.close();
        }
    }

    interface WithConnection {
        void apply(Connection connection) throws Exception;
    }
}
