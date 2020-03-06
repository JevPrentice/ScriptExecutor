package com.scriptexecutor;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple program to run SQL scripts in a directory.
 * @author jevprentice
 */
public class ScriptExecutor implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(ScriptExecutor.class.getName());
    public static final ScriptExecutor INSTANCE = new ScriptExecutor();

    private ScriptExecutor() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (final ClassNotFoundException ex) {
            throw new RuntimeException("Postgres driver not loaded.", ex);
        }
    }

    private static Properties readProperties() {
        try (final InputStream resource = ScriptExecutor.class.getResourceAsStream("/config.properties")) {
            final Properties properties = new Properties();
            properties.load(resource);
            return properties;
        } catch (final IOException e) {
            LOGGER.log(Level.SEVERE, "Unable to load properties", e);
            throw new RuntimeException(e);
        }
    }

    private static Connection createConnection(final Properties properties) {
        try {
            final StringBuilder sb = new StringBuilder(properties.getProperty("database_url"));
            if (!sb.toString().contains("?"))
                sb.append("?currentSchema=" + properties.getProperty("schema_name"));
            final Connection connection = DriverManager.getConnection(sb.toString(),
                    properties.getProperty("database_user"), properties.getProperty("database_password"));
            LOGGER.log(Level.INFO, "Connection created to {0}", sb.toString());
            return connection;
        } catch (final SQLException e) {
            LOGGER.log(Level.SEVERE, "SQL Exception while trying to create database connection", e);
            throw new RuntimeException(e);
        }
    }

    private static List<File> findFiles(final List<String> extensions, final String dir) throws IOException {
        final ArrayList<File> files = new ArrayList<>();
        final FileVisitor<Path> visitor = new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs) {
                final File file = path.toFile();
                final String fileExt = file.getName()
                        .substring(file.getName().lastIndexOf(".") + 1)
                        .toLowerCase(Locale.ENGLISH);
                if (file.isFile() && extensions.contains(fileExt))
                    files.add(file);
                return FileVisitResult.CONTINUE;
            }
        };
        Files.walkFileTree(Paths.get(dir), visitor);
        return files;
    }

    private static String readFileContents(Properties properties, Path path, Charset charset) throws IOException {
        final StringBuilder sql = new StringBuilder();
        for (String line : Files.readAllLines(path, charset)) {
            if (line.contains("--")) // Skip comments.
                line = line.substring(0, line.indexOf("--"));
            sql.append(line).append(System.getProperty("line.separator"));
        }
        return sql.toString().replaceAll("schema_name", properties.getProperty("schema_name"));
    }


    @Override
    public void run() {
        final Properties properties = readProperties();
        int count = 1;
        try (final Connection connection = createConnection(properties);
             final Statement stmt = connection.createStatement()) {
            for (final File file : findFiles(List.of("sql"), properties.getProperty("sql_dir"))) {
                try {
                    final String sql = readFileContents(properties, Paths.get(file.getParent(), file.getName()), StandardCharsets.UTF_8);
                    final boolean execute = stmt.execute(sql);
                    if (execute) {
                        final ResultSet rs = stmt.getResultSet();
                        final ResultSetMetaData rsmd = rs.getMetaData();
                        final int columnsNumber = rsmd.getColumnCount();
                        LOGGER.log(Level.INFO, "Script #{0} {1} has been executed and returned {2} columns",
                                new Object[]{count, file.getAbsolutePath(), columnsNumber});
                    } else {
                        LOGGER.log(Level.INFO, "Script #{0} {1} has been executed and returned no Result Set",
                                new Object[]{count, file.getAbsolutePath()});
                    }
                    count++;
                } catch (final IOException | SQLException e) {
                    LOGGER.log(Level.SEVERE, "Error on script: {0}", file.getAbsolutePath());
                    throw new RuntimeException(e);
                }
            }
        } catch (final IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
