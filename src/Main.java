import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class Main {

    private final static String jsonsFolderPath = "/home/user/IntelliJProjects/SeleniumProject/jsons/";
    private final static String textsFilePath = jsonsFolderPath + "jsonTexts.json";
    private final static String linksFilePath = jsonsFolderPath + "jsonLinks.json";
    private final static String picturesFilePath = jsonsFolderPath + "jsonPictures.json";
    private final static String lockFilePath = jsonsFolderPath + "isBusy";
    private final static String[] threadNames = {"TextsThread", "LinksThread", "PicturesThread"};
    private final static String[] filesPaths = {textsFilePath, linksFilePath, picturesFilePath};
    private final static String[] dbColumns = {"Texts", "Links", "Pictures"};

    // Preparing sql templates
    private final static String getPostIdTemplate = "SELECT PostId FROM %s";
    private final static String insertNewTemplate = "INSERT INTO %s(PostId, %s) VALUES(?, ?)";

    private static void daemonize() throws IOException {
        System.in.close();
        System.out.close();
    }

    private static MysqlDataSource getConnectionData() throws IOException {
        Properties props = new Properties();

        String fileName = "./db.properties";
        props.load(new FileInputStream(fileName));

        MysqlDataSource ds = new MysqlDataSource();
        ds.setURL(props.getProperty("db.url"));
        ds.setUser(props.getProperty("db.user"));
        ds.setPassword(props.getProperty("db.password"));

        return ds;
    }

    private static class MyThread extends Thread {

        private String filePath;
        private String columnName;
        private Connection connection;

        public MyThread(String ThreadName, String filePath, String columnName, Connection connection) {
            super(ThreadName);
            this.filePath = filePath;
            this.columnName = columnName;
            this.connection = connection;
        }

        public void run() {
            try {
                // First SQL query
                PreparedStatement getPosts = connection.prepareStatement(String.format(getPostIdTemplate, columnName));
                ResultSet dbPosts = getPosts.executeQuery();

                Set<String> dbPostsSet = new HashSet<>();
                while (dbPosts.next())
                    dbPostsSet.add(dbPosts.getString(1));

                JSONParser parser = new JSONParser();
                JSONArray array = (JSONArray) parser.parse(
                        new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8));

                // Second SQL query
                PreparedStatement insertPost = connection.prepareStatement(
                        String.format(insertNewTemplate, columnName, columnName));

                for (Object record : array)
                    for (Object key : ((JSONObject) record).keySet()) {
                        String stringKey = (String) key;
                        if (!dbPostsSet.contains(stringKey)) {
                            insertPost.setString(1, stringKey);
                            if (columnName.equals("Texts"))
                                insertPost.setString(2, ((JSONObject) record).get(stringKey).toString());
                            else
                                insertPost.setString(2, (
                                        (JSONArray)(((JSONObject)record).get(stringKey))).toJSONString());
                            insertPost.executeUpdate();
                        }
                    }

            } catch (IOException | ParseException | SQLException e) {
                e.printStackTrace();
            }
        }

    }

    private static void run() throws InterruptedException, IOException, SQLException {
        while (true) {
            MysqlDataSource ds = getConnectionData();

            // Checking if files are not occupied
            boolean isOccupied = true;
            while (isOccupied) {
                FileReader fr = new FileReader(lockFilePath);
                isOccupied = (fr.read() == '1');
                fr.close();
                if (isOccupied)
                    Thread.sleep(1000);
            }
            // Occupy the file
            FileWriter fr = new FileWriter(lockFilePath);
            fr.write('1');
            fr.close();

            // Connect to db
            Connection con = ds.getConnection();

            ArrayList<MyThread> threads = new ArrayList<>();
            for (int i = 0; i < 3; ++i)
                threads.add(new MyThread(threadNames[i], filesPaths[i], dbColumns[i], con));

            for (MyThread thread : threads)
                thread.start();

            for (MyThread thread : threads)
                thread.join();

            con.close();

            // Free the file
            fr = new FileWriter(lockFilePath);
            fr.write('0');
            fr.close();

            // Do this every 10 minutes
            Thread.sleep(600000);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, SQLException {
        daemonize();
        run();
    }
}
