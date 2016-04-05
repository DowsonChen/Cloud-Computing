package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;

public class ProfileServlet extends HttpServlet {
    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "dowson";
    private static final String URL = "jdbc:mysql://dongsongmysql.cmzyt7fdioxc.us-east-1.rds.amazonaws.com/" + DB_NAME;
    // Database credentials
    private static final String DB_USER = "dowson";
    private static final String DB_PWD = "cds921209";

    private static Connection conn;
    Statement stmt = null;

    public ProfileServlet() {
        try {
            // Register JDBC driver
            Class.forName(JDBC_DRIVER);
            // Open a connection
            conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {

        String id = request.getParameter("id");
        String pwd = request.getParameter("pwd");
        Statement checkStmt = null;
        Statement queryStmt = null;
        /*
            If YES, send back the user's Name and Profile Image URL.
            If NOT, set Name as "Unauthorized" and Profile Image URL as "#".
        */
        // Query database
        String check = "select * from users where id = " + id + " and password = '" + pwd +"';";
        String query = "select * from usersinfo where id = " + id + ";";
        String name = null;
        String profile = null;
        try {
            checkStmt = conn.createStatement();
            // Check if user exists
            ResultSet checkResult = checkStmt.executeQuery(check);
            if (!checkResult.next()) {
                name = "Unauthorized";
                profile = "#";
            } else {
                queryStmt = conn.createStatement();
                ResultSet queryResult = queryStmt.executeQuery(query);
                if (queryResult.next()) {
                    name = queryResult.getString("name");
                    profile = queryResult.getString("profile");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (checkStmt != null && queryStmt != null) {
                try {
                    checkStmt.close();
                    queryStmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        // Create JSON response
        JSONObject result = new JSONObject();
        result.put("name", name);
        result.put("profile", profile);
        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        doGet(request, response);
    }
}
