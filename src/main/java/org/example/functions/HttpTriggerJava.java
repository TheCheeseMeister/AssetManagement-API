package org.example.functions;

import java.io.ByteArrayInputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.sql.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
        return queryTop10(request, context, "SLD Maintenance Crew", columns);
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
        return queryTop10(request, context, "SLD SRI Master", columns);
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

    /**
     * Takes in a post request and uses the fields of the request to create a new record in the Signage table.
     *
     * @param request A group of all the fields that will be imported into the Signage table.
     * @param context General context
     * @return Response request stating a successful upload to the Signage table.
     */
    @FunctionName("UploadSignage")
    public HttpResponseMessage uploadSignage(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.FUNCTION)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context
            ) {
        context.getLogger().info("Processing Upload to Signage table...");

        // Handle metadata
        String json = request.getBody().orElse("");
        if (json.isEmpty()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Request body is empty.")
                    .build();
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode data = mapper.readTree(json);

            // Fill out metadata variables - specific variables aren't allowed to be null
            String street = data.get("street") != null ? data.get("street").asText() : null;
            Double milepost = data.hasNonNull("milepost") ? data.get("milepost").asDouble() : null;
            Double lat = data.get("lat") != null ? data.get("lat").asDouble() : null;
            Double lon = data.get("long") != null ? data.get("long").asDouble() : null;
            String location = data.get("location") != null ? data.get("location").asText() : null;
            Integer posts = data.hasNonNull("posts") ? data.get("posts").asInt() : null;
            String type = data.hasNonNull("type") ? data.get("type").asText() : null;
            Double height = data.hasNonNull("height") ? data.get("height").asDouble() : null;
            Boolean illuminated = data.get("illuminated") != null ? data.get("illuminated").asBoolean() : null;
            Boolean walkway = data.get("walkway") != null ? data.get("walkway").asBoolean() : null;
            String ground_treatment = data.hasNonNull("ground_treatment") ? data.get("ground_treatment").asText() : null;

            String dateStr = data.get("inventory_date").asText();
            LocalDateTime inventoryDate = !dateStr.isEmpty() ? LocalDateTime.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : null;

            String base64Image = data.get("image") != null ? data.get("image").asText() : null;

            if (street == null || street.isEmpty() || lat == null || lon == null
                    || location == null || location.isEmpty()
                    || illuminated == null || walkway == null
                    || base64Image == null || base64Image.isEmpty()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .body("Request is missing non-nullable fields.")
                        .build();
            }

            context.getLogger().info("Metadata received. Uploading image and retrieving filename...");

            // Upload image to blob and get path
            byte[] imageBytes = Base64.getDecoder().decode(base64Image);

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

            // Name Blob (also what will fill image field in database)
            String blobName = String.format("%f_%f.png", lat, lon);
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            // Upload image
            ByteArrayInputStream dataStream = new ByteArrayInputStream(imageBytes);
            blobClient.upload(dataStream, imageBytes.length, true);

            // Set metadata for Blob (currently don't need, but will leave here for now)
            //if (!metadataMap.isEmpty()) {
            //    blobClient.setMetadata(metadataMap);
            //}

            // Place into database
            String connectionString = System.getenv("SqlConnectionString");

            if (connectionString == null || connectionString.isEmpty()) {
                context.getLogger().severe("SqlConnectionString is null or empty!");
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("Database connection string is missing!")
                        .build();
            }

            Connection conn = DriverManager.getConnection(connectionString);
            String sql = "INSERT INTO dbo.[Signage] (Street, Milepost, Latitude, Longitude, Location, Posts, Type, Height, Illuminated, Walkway, Ground_Treatment, Inventory_Date, Image) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, street);
            if (milepost != null) {
                stmt.setDouble(2, milepost);
            } else {
                stmt.setNull(2, Types.DOUBLE);
            }
            stmt.setDouble(3, lat);
            stmt.setDouble(4, lon);
            stmt.setString(5, location);
            if (posts != null) {
                stmt.setInt(6, posts);
            } else {
                stmt.setNull(6, Types.INTEGER);
            }
            if (type != null) {
                stmt.setString(7, type);
            } else {
                stmt.setNull(7, Types.NVARCHAR);
            }
            if (height != null) {
                stmt.setDouble(8, height);
            } else {
                stmt.setNull(8, Types.DOUBLE);
            }
            stmt.setBoolean(9, illuminated);
            stmt.setBoolean(10, walkway);
            if (ground_treatment != null) {
                stmt.setString(11, ground_treatment);
            } else {
                stmt.setNull(11, Types.NVARCHAR);
            }
            stmt.setTimestamp(12, Timestamp.valueOf(inventoryDate));
            stmt.setString(13, blobName);

            int rowsInserted = stmt.executeUpdate();
            if (rowsInserted > 0) {
                // Return success
                return request.createResponseBuilder(HttpStatus.OK)
                        .body("Signage uploaded successfully")
                        .build();
            } else {
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("Failed to update database.")
                        .build();
            }
        } catch (Exception e) {
            context.getLogger().severe("Error processing request: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error processing request: " + e.getMessage())
                    .build();
        }
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
