package org.example.functions;

import java.util.*;
import java.sql.*;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

/**
 * Azure Functions with HTTP Trigger.
 */
public class HttpTriggerJava {
    /**
     * This function listens at endpoint "/api/HttpTriggerJava". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpTriggerJava
     * 2. curl {your host}/api/HttpTriggerJava?name=HTTP%20Query
     */
    @FunctionName("GetSRI")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET}, authLevel = AuthorizationLevel.FUNCTION) HttpRequestMessage<Optional<String>> request,
            //@BindingName("SqlConnectionString") String connectionString,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger - GetSRI() called.");

        // Parse query parameter
        //String query = request.getQueryParameters().get("name");
        //String name = request.getBody().orElse(query);

        String connectionString = System.getenv("SqlConnectionString");
        String query = "SELECT TOP 10 ID, SRI FROM [dbo].[SRI MASTER]";

        List<Map<String, Object>> results = new ArrayList<>();

        context.getLogger().info("Starting SQL connection attempt...");

        if (connectionString == null || connectionString.isEmpty()) {
            context.getLogger().severe("SqlConnectionString is null or empty!");
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Database connection string is missing!")
                    .build();
        }

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            context.getLogger().info("SQL Server JDBC Driver loaded.");
        } catch (ClassNotFoundException e) {
            context.getLogger().severe("JDBC Driver class not found: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("JDBC Driver not found!")
                    .build();
        }

        try(Connection conn = DriverManager.getConnection(connectionString);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("ID", rs.getInt("ID"));
                row.put("SRI", rs.getString("SRI"));
                results.add(row);
            }
        } catch (SQLException e) {
            context.getLogger().severe("DB Error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Database error: " + e.getMessage())
                    .build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(results)
                .build();
    }
}
