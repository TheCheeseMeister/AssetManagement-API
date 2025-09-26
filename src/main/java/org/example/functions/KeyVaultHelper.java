package org.example.functions;

import com.azure.core.exception.HttpResponseException;
import com.azure.core.exception.ResourceNotFoundException;
import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;

public class KeyVaultHelper {
    private static final String VAULT_URL = "https://am-auth.vault.azure.net/";

    public static String getSigningKey() {
        System.setProperty("AZURE_IDENTITY_LOG_LEVEL", "DEBUG");

        SecretClient secretClient = new SecretClientBuilder()
                .vaultUrl(VAULT_URL)
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();

        try {
            String value = secretClient.getSecret("jwt-key").getValue();
            System.out.println("Secret retrieved successfully: " + value);
            return value;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error retrieving secret from Key Vault", e);
        }
    }
}
