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
 * Iterate over directory of .SQL files. Executing each file on the given
 * database schema
 *
 * Replaces string in .SQL file schema_name with the configuration value for key
 * schema_name
 *
 * @author jevprentice
 */
public class ScriptExecutor {

    private static final ScriptExecutor INSTANCE = new ScriptExecutor();
    private static final String PROPERTIES_DIR = "/Users/jevprentice/NetBeansProjects/ScriptExecutor 2/src/main/resources/config.properties";

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
        try (Connection connection = getConnection(properties)) {
            try (Statement stmt = connection.createStatement()) {

                String[] extenstions = {"sql"};
                int i = 1;
                for (final File file : getFilesInAppSource(extenstions, properties.getProperty("sql_dir"))) {

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
            }
        }
    }

    public static void main(String[] args) throws SQLException, IOException {
        String configFile = (args != null && args.length == 1) ? args[0] : PROPERTIES_DIR;

        Logger.getLogger(ScriptExecutor.class.getName()).log(Level.SEVERE, "Starting - Using Config {0}", configFile);

        ScriptExecutor.getInstance().executeFiles(getProperties(configFile));

        Logger.getLogger(ScriptExecutor.class.getName()).log(Level.SEVERE, "Done");
    }
}
