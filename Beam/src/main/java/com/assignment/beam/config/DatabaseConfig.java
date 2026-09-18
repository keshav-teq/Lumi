package com.assignment.beam.config;

/**
 * DatabaseConfig class initializes the Database connection.
 * Initializes the connection from the env values passed through the DAG.
 */
public final class DatabaseConfig {
    private DatabaseConfig() {
    }

    public static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String JDBC_URL = databaseSetting("DB_URL", "jdbc:mysql://localhost:3306/lumi");
    public static final String DB_USERNAME = requiredSetting("DB_USERNAME");
    public static final String DB_PASSWORD = requiredSetting("DB_PASSWORD");

    private static String databaseSetting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String requiredSetting(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " must be provided by the runtime environment");
        }
        return value;
    }
}