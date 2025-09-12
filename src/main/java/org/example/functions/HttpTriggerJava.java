package org.example.functions;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.sql.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import com.azure.storage.blob.*;
import com.azure.storage.blob.models.*;

/**
 * Azure Functions with HTTP Trigger.
 */
public class HttpTriggerJava {
    /**
     * This function listens at endpoint "/api/HttpTriggerJava". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpTriggerJava
     * 2. curl {your host}/api/HttpTriggerJava?name=HTTP%20Query
     */
    @FunctionName("GetMaintenanceCrew")
    public HttpResponseMessage getMaintenanceCrew(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET}, authLevel = AuthorizationLevel.FUNCTION)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        String[] columns = {
                "ID", // PK, int
                "SRI", // nvarchar
                "Start_Milepost", // float
                "End_Milepost", // float
                "Crew_Type", // nvarchar
                "Crew_Id", // smallint
                "Last_Update_Date" // datetime2
        };
        return queryTop10(request, context, "Maintenance Crew", columns);
    }

    @FunctionName("GetSRI")
    public HttpResponseMessage getSRI(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET}, authLevel = AuthorizationLevel.FUNCTION)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        String[] columns = {
                "ID", // PK, int
                "SRI", // nvarchar
                "Start_Milepost", // float
                "End_Milepost", // float
                "Direction", // nvarchar
                "Name", // nvarchar
                "Parent_SRI", // nvarchar
                "Parent_Start_Milepost", // float
                "Parent_End_Milepost", // float
                "Last_Update_Date" // datetime2
        };
        return queryTop10(request, context, "SRI Master", columns);
    }

    /**
     * Test function for interacting with SQL Database
     *
     * @param request Passed from request of function used.
     * @param context Passed from context of function used.
     * @param tableName The name of the table to get TOP 10 from.
     * @param columns The columns of the table to return.
     * @return An ArrayList of the TOP 10 results from the table
     */
    @FunctionName("QueryTop10")
    private HttpResponseMessage queryTop10(
            HttpRequestMessage<Optional<String>> request,
            ExecutionContext context,
            String tableName,
            String[] columns
            ) {
        context.getLogger().info("Querying TOP 10 records of " + tableName + "...");

        String connectionString = System.getenv("SqlConnectionString");
        String query = "SELECT TOP 10 " + String.join(", ", columns) + " FROM [dbo].[" + tableName + "]";

        List<Map<String, Object>> results = new ArrayList<>();

        context.getLogger().info("Starting SQL Connection Attempt...");

        // Ensure Connection String is correctly being received from environment variables
        if (connectionString == null || connectionString.isEmpty()) {
            context.getLogger().severe("SqlConnectionString is null or empty!");
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Database connection string is missing!")
                    .build();
        }

        // Ensure SQL JDBC Driver is loaded before continuing
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            context.getLogger().info("SQL Server JDBC Driver loaded.");
        } catch (ClassNotFoundException e) {
            context.getLogger().severe("JDBC Driver class not found: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("JDBC Driver not found!")
                    .build();
        }

        // Query TOP 10 results
        try(Connection conn = DriverManager.getConnection(connectionString);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (String col : columns) {
                    row.put(col, rs.getObject(col));
                }
                results.add(row);
            }
        } catch (SQLException e) {
            context.getLogger().severe("DB Error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Database error: " + e.getMessage())
                    .build();
        }

        // Return TOP 10 stored in results
        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(results)
                .build();
    }

    @FunctionName("UploadImage")
    public HttpResponseMessage uploadImage(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.FUNCTION)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context
            ) {
        context.getLogger().info("Processing image...");

        try {
            String body = request.getBody().orElse(null);
            if (body == null || body.isEmpty()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .body("Empty request body.")
                        .build();
            }

            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> jsonMap = mapper.readValue(body, Map.class);

            // Extract image
            String base64Image = (String) jsonMap.get("image");
            byte[] imageBytes = Base64.getDecoder().decode(base64Image);

            // Extract metadata
            List<Map<String, Object>> metadataList = (List<Map<String, Object>>) jsonMap.get("metadata");
            Map<String, String> metadataMap = new HashMap<>();

            for (Map<String, Object> entry : metadataList) {
                String key = (String) entry.get("key");
                Object value = entry.get("value"); // left as object just in case values are parsed weird
                metadataMap.put(key, value.toString());
            }

            context.getLogger().info("Uploading to blob storage...");

            String connectStr = System.getenv("ConnectBlobStorage");
            String containerName = "images";
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(connectStr)
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            if (!containerClient.exists()) {
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("Container 'images' doesn't exist.")
                        .build();
            }

            // Name Blob
            String blobName = UUID.randomUUID().toString() + ".png";
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            // Upload image
            ByteArrayInputStream dataStream = new ByteArrayInputStream(imageBytes);
            blobClient.upload(dataStream, imageBytes.length, true);

            // Set metadata for Blob
            if (!metadataMap.isEmpty()) {
                blobClient.setMetadata(metadataMap);
            }

            context.getLogger().info("Uploaded blob: " + blobName + " with metadata: " + metadataMap);

            Map<String, Object> response = new HashMap<>();
            response.put("message", "Upload successful");
            response.put("blobName", blobName);
            response.put("metadata", metadataMap);

            return request.createResponseBuilder(HttpStatus.OK)
                    .header("Content-Type", "application/json")
                    .body(mapper.writeValueAsString(response))
                    .build();
        } catch (Exception e) {
            context.getLogger().severe("Error: " + e.getMessage());

            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error: " + e.getMessage()).build();
        }
    }
}
