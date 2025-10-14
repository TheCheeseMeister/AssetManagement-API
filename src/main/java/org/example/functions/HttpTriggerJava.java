package org.example.functions;

import java.io.ByteArrayInputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.sql.*;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import com.azure.storage.blob.*;
import com.azure.storage.blob.models.*;

import org.example.functions.KeyVaultHelper;
import org.example.functions.JwtGenerator;

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

        // Ensure Bearer token is present
        String authHeader = request.getHeaders().get("authorization");
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            return request.createResponseBuilder(HttpStatus.UNAUTHORIZED)
                    .body("Missing or invalid Authorization header")
                    .build();
        }

        String token = authHeader.substring(7);
        String singingKey = KeyVaultHelper.getSigningKey();

        try {
            JwtGenerator.validateToken(token, singingKey);
        } catch (Exception e) {
            context.getLogger().warning("Invalid JWT: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.UNAUTHORIZED)
                    .body("Invalid or expired token")
                    .build();
        }

        // Authenticated

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
     * Takes in a post request and uses an array to upload new images to the Signage Image Container.
     *
     * @param request An array of images encoded in base64
     * @param context General context
     * @return Response request stating a successful upload to the Signage Image container.
     */
    @FunctionName("BulkSignageImages")
    public HttpResponseMessage bulkSignageImages(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.FUNCTION)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context
            ) {
        context.getLogger().info("Processing Bulk Upload to Signage Image container...");

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

            if (!data.has("images") || !data.get("images").isArray()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .body("Missing or invalid 'images' array in request.")
                        .build();
            }

            ArrayNode arr = (ArrayNode) data.get("images");
            context.getLogger().info("Uploading images to container...");

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

            int index = 1;
            for (JsonNode imageNode : arr) {
                String base64Image = imageNode.hasNonNull("image") ? imageNode.get("image").asText() : null;

                byte[] imageBytes = Base64.getDecoder().decode(base64Image);

                // Name Blob (also what will fill image field in database)
                LocalDateTime timestamp = LocalDateTime.now();
                String date = timestamp.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                String time = timestamp.format(DateTimeFormatter.ofPattern("HHmmss"));
                String blobName = String.format("%s_%s_%d.png", date, time, index);

                BlobClient blobClient = containerClient.getBlobClient(blobName);

                // Upload image
                ByteArrayInputStream dataStream = new ByteArrayInputStream(imageBytes);
                blobClient.upload(dataStream, imageBytes.length, true);

                index++;
            }

            return request.createResponseBuilder(HttpStatus.OK)
                    .body("Successfully processed images.")
                    .build();
        } catch (Exception e) {
            context.getLogger().severe("Error processing request: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error processing request: " + e.getMessage())
                    .build();
        }
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
            //Double lat = data.get("lat") != null ? data.get("lat").asDouble() : null;
            //Double lon = data.get("long") != null ? data.get("long").asDouble() : null;
            Double lat = data.hasNonNull("lat") ? data.get("lat").asDouble() : null;
            Double lon = data.hasNonNull("long") ? data.get("long").asDouble() : null;
            String location = data.get("location") != null ? data.get("location").asText() : null;
            Integer posts = data.hasNonNull("posts") ? data.get("posts").asInt() : null;
            String type = data.hasNonNull("type") ? data.get("type").asText() : null;
            Double height = data.hasNonNull("height") ? data.get("height").asDouble() : null;
            Boolean illuminated = data.get("illuminated") != null ? data.get("illuminated").asBoolean() : null;
            Boolean walkway = data.get("walkway") != null ? data.get("walkway").asBoolean() : null;
            String ground_treatment = data.hasNonNull("ground_treatment") ? data.get("ground_treatment").asText() : null;
            String condition = data.hasNonNull("condition") ? data.get("condition").asText() : null;
            String defect = data.hasNonNull("defect") ? data.get("defect").asText() : null;
            String weather_condition = data.hasNonNull("weather_condition") ? data.get("weather_condition").asText() : null;
            Integer vehicle_speed = data.hasNonNull("vehicle_speed") ? data.get("vehicle_speed").asInt() : null;
            String road_type = data.hasNonNull("road_type") ? data.get("road_type").asText() : null;
            String image_type = data.hasNonNull("image_type") ? data.get("image_type").asText() : null;
            String created_by = data.hasNonNull("created_by") ? data.get("created_by").asText() : null;

            // Inventory_date split up into date and time
            String dateStr = data.get("inventory_date").asText();
            LocalDate inventoryDate;
            LocalTime inventoryTime;

            if (!dateStr.isEmpty()) {
                LocalDateTime dateParse = LocalDateTime.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

                inventoryDate = dateParse.toLocalDate();
                inventoryTime = dateParse.toLocalTime();
            } else {
                inventoryDate = null;
                inventoryTime = null;
            }

            String base64Image = data.get("image") != null ? data.get("image").asText() : null;

            if (street == null || street.isEmpty()
                    || location == null || location.isEmpty()
                    || illuminated == null || walkway == null
                    || base64Image == null || base64Image.isEmpty() || inventoryDate == null || inventoryTime == null) {
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
            String blobName = "";
            if (lat == null || lon == null) {
                blobName = String.format(
                        "%s_%s_%s.png",
                        street,
                        inventoryDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")),
                        inventoryTime.format(DateTimeFormatter.ofPattern("HHmmss"))
                );
            } else {
                blobName = String.format("%f_%f.png", lat, lon);
            }

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
            String sql = "INSERT INTO dbo.[Signage] (Street, Milepost, Latitude, Longitude, Location, Posts, Type, Height, " +
                         "Illuminated, Walkway, Ground_Treatment, Inventory_Date, Image, " +
                         "Inventory_Time, Condition, Defect, Weather_Condition, Vehicle_Speed, Road_Type, Image_Type, Created_By) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, street);
            if (milepost != null) {
                stmt.setDouble(2, milepost);
            } else {
                stmt.setNull(2, Types.DOUBLE);
            }
            if (lat != null) {
                stmt.setDouble(3, lat);
            } else {
                stmt.setNull(3, Types.DOUBLE);
            }
            if (lon != null) {
                stmt.setDouble(4, lon);
            } else {
                stmt.setNull(4, Types.DOUBLE);
            }
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
            stmt.setDate(12, java.sql.Date.valueOf(inventoryDate));
            stmt.setString(13, blobName);

            // new fields
            stmt.setTime(14, java.sql.Time.valueOf(inventoryTime));
            if (condition != null) {
                stmt.setString(15, condition);
            } else {
                stmt.setNull(15, Types.NVARCHAR);
            }
            if (defect != null) {
                stmt.setString(16, defect);
            } else {
                stmt.setNull(16, Types.NVARCHAR);
            }
            if (weather_condition != null) {
                stmt.setString(17, weather_condition);
            } else {
                stmt.setNull(17, Types.NVARCHAR);
            }
            if (vehicle_speed != null) {
                stmt.setInt(18, vehicle_speed);
            } else {
                stmt.setNull(18, Types.INTEGER);
            }
            if (road_type != null) {
                stmt.setString(19, road_type);
            } else {
                stmt.setNull(19, Types.NVARCHAR);
            }
            if (image_type != null) {
                stmt.setString(20, image_type);
            } else {
                stmt.setNull(20, Types.NVARCHAR);
            }
            if (created_by != null) {
                stmt.setString(21, created_by);
            } else {
                stmt.setNull(21, Types.NVARCHAR);
            }

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

    @FunctionName("GetDataForImageSignage")
    public HttpResponseMessage getDataForImageSignage(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.FUNCTION)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context
            ) {
        context.getLogger().info("Getting image filename...");

        try {
            String body = request.getBody().orElse(null);
            if (body == null || body.isEmpty()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .body("Empty request body.")
                        .build();
            }

            ObjectMapper mapper = new ObjectMapper();
            JsonNode data = mapper.readTree(body);

            String image = data.get("image").asText();

            String[] columns = {"ID", "Street", "Milepost", "Latitude", "Longitude", "Location", "Posts",
                                "Type", "Height", "Illuminated", "Walkway", "Ground_Treatment", "Inventory_Date",
                                "Inventory_Time", "Condition", "Defect", "Weather_Condition", "Vehicle_Speed",
                                "Road_Type", "Image_Type", "Created_By"};

            String connectionString = System.getenv("SqlConnectionString");
            String query = "SELECT " + String.join(", ", columns) + " FROM [dbo].[Signage] WHERE Image = ?";

            List<Map<String, Object>> results = new ArrayList<>();

            // Query Data based on image
            try(Connection conn = DriverManager.getConnection(connectionString)) {
                PreparedStatement stmt = conn.prepareStatement(query);
                stmt.setString(1, image);
                ResultSet rs = stmt.executeQuery();

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
        } catch (Exception e) {
            context.getLogger().severe("Error: " + e.getMessage());

            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error: " + e.getMessage()).build();
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

    @FunctionName("signUp")
    public HttpResponseMessage signUp(
            @HttpTrigger(name = "req", methods = { HttpMethod.POST }, authLevel = AuthorizationLevel.FUNCTION)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        context.getLogger().info("Processing user signup...");

        try {
            String body = request.getBody().orElse("");
            if (body.isEmpty()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .body("Request body is empty.")
                        .build();
            }

            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(body);

            String username    = json.hasNonNull("username") ? json.get("username").asText().trim() : null;
            String email       = json.hasNonNull("email")    ? json.get("email").asText().trim()    : null;
            String password    = json.hasNonNull("password") ? json.get("password").asText()        : null;
            String phoneNumber = json.hasNonNull("phonenumber") ? json.get("phonenumber").asText()   : null;

            if (username == null || username.isEmpty()
                    || email == null || email.isEmpty()
                    || password == null || password.isEmpty()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .body("Missing required fields: username, email, password.")
                        .build();
            }

            // ----- DB connection -----
            String connStr = System.getenv("SqlConnectionString");
            if (connStr == null || connStr.isEmpty()) {
                context.getLogger().severe("SqlConnectionString is not configured.");
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("Server misconfiguration: missing database connection string.")
                        .build();
            }

            try (Connection conn = DriverManager.getConnection(connStr)) {
                String sql = "INSERT INTO dbo.[Users] (username, password, email, phonenumber) OUTPUT INSERTED.ID VALUES (?, ?, ?, ?)";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, username);
                    ps.setString(2, password);      
                    ps.setString(3, email);
                    if (phoneNumber != null && !phoneNumber.isEmpty()) {
                        ps.setString(4, phoneNumber);
                    } else {
                        ps.setNull(4, Types.NVARCHAR);
                    }

                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        int newID = rs.getInt(1);

                        String signingKey = KeyVaultHelper.getSigningKey();
                        String accessToken = JwtGenerator.generateAccessToken(newID, signingKey);
                        String refreshToken = JwtGenerator.generateRefreshToken(newID, signingKey);

                        Map<String, Object> resp = new HashMap<>();
                        resp.put("message", "Signup successful");
                        resp.put("email", email);
                        resp.put("accessToken", accessToken);
                        resp.put("refreshToken", refreshToken);

                        return request.createResponseBuilder(HttpStatus.CREATED)
                                .header("Content-Type", "application/json")
                                .body(mapper.writeValueAsString(resp))
                                .build();
                    } else {
                        return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                                .body("Unexpected result while creating user.")
                                .build();
                    }
                }
            } catch (SQLException se) {
                // SQL Server unique/duplicate key: 2627 or 2601
                int code = se.getErrorCode();
                context.getLogger().warning("SQL error " + code + ": " + se.getMessage());
                if (code == 2627 || code == 2601) {
                    return request.createResponseBuilder(HttpStatus.CONFLICT)
                            .body("Email already registered.")
                            .build();
                }
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("Database error: " + se.getMessage())
                        .build();
            }
        } catch (Exception e) {
            context.getLogger().severe("Unhandled error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error processing request: " + e.getMessage())
                    .build();
        }
    }

    @FunctionName("Login")
    public HttpResponseMessage login(
            @HttpTrigger(name = "req", methods = { HttpMethod.POST }, authLevel = AuthorizationLevel.FUNCTION)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        context.getLogger().info("Processing user login...");

        try {
            // ---- Parse body ----
            String body = request.getBody().orElse("");
            if (body.isEmpty()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .body("Request body is empty.")
                        .build();
            }

            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(body);

            String email    = json.hasNonNull("email")    ? json.get("email").asText().trim() : null;
            String password = json.hasNonNull("password") ? json.get("password").asText()     : null;

            if (email == null || email.isEmpty() || password == null || password.isEmpty()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .body("Missing required fields: email, password.")
                        .build();
            }

            // ---- DB connection ----
            String connStr = System.getenv("SqlConnectionString");
            if (connStr == null || connStr.isEmpty()) {
                context.getLogger().severe("SqlConnectionString is not configured.");
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("Server misconfiguration: missing database connection string.")
                        .build();
            }

            String sql = "SELECT ID, username, password, phonenumber FROM dbo.[Users] WHERE email = ?";
            try (Connection conn = DriverManager.getConnection(connStr);
                 PreparedStatement ps = conn.prepareStatement(sql)) {

                ps.setString(1, email);

                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return request.createResponseBuilder(HttpStatus.UNAUTHORIZED)
                                .body("Invalid email or password.")
                                .build();
                    }

                    int id              = rs.getInt("ID");
                    String username     = rs.getString("username");
                    String storedPass   = rs.getString("password");
                    String phoneNumber  = rs.getString("phonenumber");

                    if (!password.equals(storedPass)) {
                        return request.createResponseBuilder(HttpStatus.UNAUTHORIZED)
                                .body("Invalid email or password.")
                                .build();
                    }

                    String signingKey;
                    String accessToken;
                    String refreshToken;
                    try {
                        context.getLogger().info("Getting signing key...");
                        signingKey = KeyVaultHelper.getSigningKey();
                        context.getLogger().info("Succesfully got key.");
                        accessToken = JwtGenerator.generateAccessToken(id, signingKey);
                        refreshToken = JwtGenerator.generateRefreshToken(id, signingKey);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                                .body("Error: " + e.getMessage())
                                .build();
                    }

                    // Success
                    Map<String, Object> resp = new HashMap<>();
                    resp.put("message", "Login successful");
                    resp.put("id", id);
                    resp.put("username", username);
                    resp.put("email", email);
                    if (phoneNumber != null) resp.put("phonenumber", phoneNumber);
                    resp.put("accessToken", accessToken);
                    resp.put("refreshToken", refreshToken);

                    return request.createResponseBuilder(HttpStatus.OK)
                            .header("Content-Type", "application/json")
                            .body(mapper.writeValueAsString(resp))
                            .build();
                }
            } catch (SQLException se) {
                context.getLogger().severe("Database error: " + se.getMessage());
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("Database error: " + se.getMessage())
                        .build();
            }
        } catch (Exception e) {
            context.getLogger().severe("Unhandled error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error processing request: " + e.getMessage())
                    .build();
        }
    }
}
