package com.mezzznineware.scriptexecutor;

import java.io.File;
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

import java.io.FileReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

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

    /**
     *
     */
    private ScriptExecutor() {
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
     * @param properties
     * @return
     * @throws SQLException
     */
    private Connection getConnection(Properties properties) throws SQLException {
        return INSTANCE.createConnection(properties);
    }

    /**
     *
     * @param properties
     * @return
     * @throws SQLException
     */
    private Connection createConnection(Properties properties) {
        Connection connection = null;
        try {
            String database = properties.getProperty("database_url");

            if (!database.contains("?")) {
                database = database + "?currentSchema=" + properties.getProperty("schema_name");
            }

            connection = DriverManager.getConnection(database, properties.getProperty("database_user"), properties.getProperty("database_password"));

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }

    /**
     *
     * @param extensions
     * @param dir
     * @return
     */
    private static ArrayList<File> getFilesInAppSource(final String[] extensions, final String dir) {

        final ArrayList<File> files = new ArrayList();
        final Path directory = Paths.get(dir);

        final File file = directory.toFile();

        if (!file.exists()) {
            file.mkdirs();
        }

        try {
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return files;
    }

    /**
     *
     * @param properties
     * @param path
     * @param charset
     * @return
     */
    private static String getSqlFileText(Properties properties, Path path, Charset charset) {

        StringBuilder sql = new StringBuilder();

        try {

            for (String line : Files.readAllLines(path, charset)) {
                if (line.contains("#")) {
                    line = line.substring(0, line.indexOf("#"));
                }
                sql.append(line).append(System.getProperty("line.separator"));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return sql.toString().replaceAll("schema_name", properties.getProperty("schema_name"));
    }

    private static void doSshTunnel(String sshUser, String sshPassword, String sshHost, int sshPort, String remoteHost, int localPort, int remotePort) {
        try {
            final JSch jsch = new JSch();
            final Session session = jsch.getSession(sshUser, sshHost, sshPort);
            session.setPassword(sshPassword);

            final Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            session.connect();
            session.setPortForwardingL(localPort, remoteHost, remotePort);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param properties
     */
    private void executeFiles(Properties properties) {

        String sshUser = properties.getProperty("sshUser");
        String sshPassword = properties.getProperty("sshPassword");
        String sshHostname = properties.getProperty("sshHostname");
        int sshPort = Integer.parseInt(properties.getProperty("sshPort"));
        String sshRemoteHost = properties.getProperty("sshRemoteHost");
        int localPort = Integer.parseInt(properties.getProperty("localPort"));
        int remotePort = Integer.parseInt(properties.getProperty("remotePort"));

        ScriptExecutor.doSshTunnel(sshUser, sshPassword, sshHostname, sshPort, sshRemoteHost, localPort, remotePort);

        try (Connection connection = getConnection(properties)) {
            try (Statement stmt = connection.createStatement()) {

                String[] extenstions = {"sql"};
                int i = 1;

                System.out.println("Executing files sql_dir - " + properties.getProperty("sql_dir"));

                for (final File file : getFilesInAppSource(extenstions, properties.getProperty("sql_dir"))) {

                    String sql = getSqlFileText(properties, Paths.get(file.getParent(), file.getName()), Charset.forName("UTF-8"));

                    boolean execute = stmt.execute(sql);

                    if (execute) {

                        ResultSet rs = stmt.getResultSet();
                        ResultSetMetaData rsmd = rs.getMetaData();
                        int columnsNumber = rsmd.getColumnCount();

                        System.out.println("Script #" + i + " " + file.getAbsolutePath() + " has been executed and returned " + columnsNumber + " columns");

                    } else {
                        System.out.println("Script #" + i + " " + file.getAbsolutePath() + " executed");
                    }

                    i++;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            System.out.println("All scripts have been executed");
            System.exit(0);
        }
    }

    public static void main(String[] args) {
        String configFile = (args != null && args.length == 1) ? args[0] : null;

        JSONParser parser = new JSONParser();
        JSONObject jsonObject;

        try {
            jsonObject = (JSONObject) parser.parse(new FileReader(configFile));
        } catch (IOException io) {
            System.out.println("configFile = " + configFile);
            System.out.println("user.dir = " + System.getProperty("user.dir"));
            throw new RuntimeException(io);
        } catch (ParseException pe) {
            throw new RuntimeException(pe);
        }

        Properties properties = new Properties();

        for (Object keyObject : jsonObject.keySet()) {
            String key = (String) keyObject;
            Object valueObject = jsonObject.get(key);

            if (valueObject instanceof String) {
                String valueString = String.valueOf(valueObject);
                properties.put(keyObject, valueString);
            }
        }

        try {
            Class.forName(properties.getProperty("database_driver"));
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }

        //System.out.println(properties.toString());
        ScriptExecutor instance = ScriptExecutor.getInstance();
        instance.executeFiles(properties);
    }
}
