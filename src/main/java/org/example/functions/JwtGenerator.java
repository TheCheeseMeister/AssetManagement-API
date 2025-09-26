package org.example.functions;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Base64;

public class JwtGenerator {
    private static final long ACCESS_TOKEN_EXPIRY = 15 * 60 * 1000; // Lasts for 15 minutes
    private static final long REFRESH_TOKEN_EXPIRY = 30L * 24 * 60 * 60 * 1000; // Lasts for 30 Days

    public static String generateAccessToken(int userId, String signingKey) {
        // Encoded in base64
        byte[] keyBytes = Base64.getDecoder().decode(signingKey);
        SecretKey sk = Keys.hmacShaKeyFor(keyBytes);

        return Jwts.builder()
                .subject(String.valueOf(userId))
                .claim("token-type", "access")
                .expiration(new Date(System.currentTimeMillis() + ACCESS_TOKEN_EXPIRY))
                .signWith(sk)
                .compact();
    }

    public static String generateRefreshToken(int userId, String signingKey) {
        // Encoded in base64
        byte[] keyBytes = Base64.getDecoder().decode(signingKey);
        SecretKey sk = Keys.hmacShaKeyFor(keyBytes);

        return Jwts.builder()
                .subject(String.valueOf(userId))
                .claim("token-type", "refresh")
                .expiration(new Date(System.currentTimeMillis() + REFRESH_TOKEN_EXPIRY))
                .signWith(sk)
                .compact();
    }

    public static Claims validateToken(String token, String signingKey) throws Exception {
        byte[] keyBytes = Base64.getDecoder().decode(signingKey);
        SecretKey sk = Keys.hmacShaKeyFor(keyBytes);

        return Jwts.parser()
                .verifyWith(sk)
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }
}
