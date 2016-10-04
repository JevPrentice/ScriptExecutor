package com.mezzznineware.scriptexecutor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This program iterates over a given directory executing all '.SQL' files on a
 * specific schema to a connected database using jdbc Only postgres has been
 * tested.
 *
 * How to use this software:
 * <ul>
 * <li></li>
 * <li>Setup the config.properties</li>
 * <li>Forward the remote servers postgres to your port 10000 (or anything that
 * matches the database_url config)</li>
 * <li>Make sure all scripts in the sql_dir contain only working reusable SQL,
 * ordered alphabetically for execution (files execute before folders)</li>
 * <li>Make sure all scripts in the sql_dir will execute correctly when running
 * from context 'set search_path to schema_name;'</li>
 * <li>When the ScriptExecutor reads the sql scripts into memory it replaces the
 * string pattern 'schema_name' in the sql files with the schema_name config
 * value</li>
 * <li>references to public schema must be explicit</li>
 * <li>Customize PROPERTIES_DIR or pass your directory into main</li>
 * <li>Run it.</li>
 * </ul>
 *
 * @author jevprentice
 */
public class ScriptExecutor {

    private static final ScriptExecutor INSTANCE = new ScriptExecutor();
    private static final String PROPERTIES_DIR = "/Users/jev/NetBeansProjects/ScriptExecutor/src/main/resources/config.properties";

    /**
     *
     */
    private ScriptExecutor() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(ScriptExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     *
     * @return
     */
    private static ScriptExecutor getInstance() {
        return INSTANCE;
    }

    /**
     *
     * @param configFile
     * @return
     * @throws IOException
     */
    private static Properties getProperties(String configFile) throws IOException {

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(configFile));
        } catch (IOException e) {
            Logger.getLogger(ScriptExecutor.class.getName()).log(Level.SEVERE, "Unable to load properties from: " + System.getProperty("user.dir") + "/" + configFile, e);
            throw new IOException(e);
        }
        return properties;
    }

    private Connection getConnection(Properties properties) throws SQLException {
        return INSTANCE.createConnection(properties);
    }

    private Connection createConnection(Properties properties) throws SQLException {
        Connection connection = null;
        try {
            String database = properties.getProperty("database_url");

            if (!database.contains("?")) {
                database = database + "?currentSchema=" + properties.getProperty("schema_name");
            }

            connection = DriverManager.getConnection(database, properties.getProperty("database_user"), properties.getProperty("database_password"));
            Logger.getLogger(ScriptExecutor.class.getName()).log(Level.INFO, "Connection created to {0}", database);
        } catch (SQLException e) {
            Logger.getLogger(ScriptExecutor.class.getName()).log(Level.SEVERE, "SQL Exception while trying to create database connection", e);
            throw e;
        }
        return connection;
    }

    protected static ArrayList<File> getFilesInAppSource(final String[] extensions, final String dir) throws IOException {

        final ArrayList<File> files = new ArrayList();
        final Path directory = Paths.get(dir);

        final File file = directory.toFile();

        if (!file.exists()) {
            file.mkdirs();
        }

        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                final File file = path.toFile();
                final String fileExtension = file.getName().substring(file.getName().lastIndexOf(".") + 1);

                if (file.isFile()) {
                    for (String extension : extensions) {
                        if (fileExtension.equalsIgnoreCase(extension)) {
                            files.add(file);
                        }
                    }
                }

                return FileVisitResult.CONTINUE;
            }
        });

        return files;
    }

    private static String getSqlFileText(Properties properties, Path path, Charset charset) throws FileNotFoundException, IOException {

        StringBuilder sql = new StringBuilder();
        for (String line : Files.readAllLines(path, charset)) {

            if (line.contains("#")) {
                line = line.substring(0, line.indexOf("#"));
            }

            sql.append(line).append(System.getProperty("line.separator"));

        }

        return sql.toString().replaceAll("schema_name", properties.getProperty("schema_name"));
    }

    protected void executeFiles(Properties properties) throws SQLException, IOException {
        File tmpFile = null;
        try (Connection connection = getConnection(properties)) {
            try (Statement stmt = connection.createStatement()) {

                String[] extenstions = {"sql"};
                int i = 1;
                for (final File file : getFilesInAppSource(extenstions, properties.getProperty("sql_dir"))) {
                    tmpFile = file;
                    String sql = getSqlFileText(properties, Paths.get(file.getParent(), file.getName()), Charset.forName("UTF-8"));

                    boolean execute = stmt.execute(sql);

                    if (execute) {

                        ResultSet rs = stmt.getResultSet();
                        ResultSetMetaData rsmd = rs.getMetaData();
                        int columnsNumber = rsmd.getColumnCount();

                        System.out.println("Script #" + i + " " + file.getAbsolutePath() + " has been executed and returned " + columnsNumber + " columns");

                    } else {
                        System.out.println("Script #" + i + " " + file.getAbsolutePath() + " has been executed and returned no Result Set");
                    }

                    i++;
                }
            } catch (Throwable e) {
                if (tmpFile != null) {
                    System.out.println("Error on script: " + tmpFile.getAbsolutePath());
                }
                e.printStackTrace(System.out);
            }
        }
    }

    public static void main(String[] args) throws SQLException, IOException {
        String configFile = (args != null && args.length == 1) ? args[0] : PROPERTIES_DIR;

        Logger.getLogger(ScriptExecutor.class.getName()).log(Level.INFO, "Starting - Using Config {0}", configFile);

        ScriptExecutor.getInstance().executeFiles(getProperties(configFile));

        Logger.getLogger(ScriptExecutor.class.getName()).log(Level.INFO, "Done");
    }
}
