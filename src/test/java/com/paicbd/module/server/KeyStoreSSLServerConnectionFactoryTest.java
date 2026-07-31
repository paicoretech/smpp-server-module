package com.paicbd.module.server;

import org.jsmpp.session.connection.ServerConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyStoreSSLServerConnectionFactoryTest {

    @TempDir
    static Path tempDir;

    private static String keystorePath;
    private static final String KEYSTORE_PASSWORD = "changeit";

    @BeforeAll
    static void generateTestKeystore() throws Exception {
        keystorePath = tempDir.resolve("test.p12").toString();
        ProcessBuilder pb = new ProcessBuilder(
                "keytool", "-genkeypair",
                "-alias", "test",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-storetype", "PKCS12",
                "-keystore", keystorePath,
                "-storepass", KEYSTORE_PASSWORD,
                "-validity", "1",
                "-dname", "CN=test"
        );
        pb.redirectErrorStream(true);
        int exitCode = pb.start().waitFor();
        assertEquals(0, exitCode, "keytool failed to generate test keystore");
    }

    @Test
    @DisplayName("Constructor with blank keystore path throws IllegalArgumentException with descriptive message")
    void constructorWithBlankKeystorePathThrowsIllegalArgumentException() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new KeyStoreSSLServerConnectionFactory("", KEYSTORE_PASSWORD));
        assertEquals("Keystore file path must not be empty when TLS is enabled", ex.getMessage());
    }

    @Test
    @DisplayName("Constructor with null keystore path throws IllegalArgumentException with descriptive message")
    void constructorWithNullKeystorePathThrowsIllegalArgumentException() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new KeyStoreSSLServerConnectionFactory(null, KEYSTORE_PASSWORD));
        assertEquals("Keystore file path must not be empty when TLS is enabled", ex.getMessage());
    }

    @Test
    @DisplayName("Constructor with nonexistent keystore file throws RuntimeException naming the missing path")
    void constructorWithNonexistentKeystoreFileThrowsRuntimeException() {
        String missingPath = tempDir.resolve("nonexistent.p12").toString();
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new KeyStoreSSLServerConnectionFactory(missingPath, KEYSTORE_PASSWORD));
        assertTrue(ex.getMessage().contains("Failed to initialize TLS keystore from '"));
        assertTrue(ex.getMessage().contains(missingPath));
    }

    @Test
    @DisplayName("Constructor with wrong keystore password throws RuntimeException naming the keystore path")
    void constructorWithWrongKeystorePasswordThrowsRuntimeException() {
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new KeyStoreSSLServerConnectionFactory(keystorePath, "wrongpassword"));
        assertTrue(ex.getMessage().contains("Failed to initialize TLS keystore from '"));
        assertTrue(ex.getMessage().contains(keystorePath));
    }

    @Test
    @DisplayName("listen on OS-assigned port returns non-null SSL server connection")
    void listenOnOsAssignedPortReturnsServerConnection() throws IOException {
        KeyStoreSSLServerConnectionFactory factory = new KeyStoreSSLServerConnectionFactory(keystorePath, KEYSTORE_PASSWORD);
        ServerConnection connection = factory.listen(0);
        assertNotNull(connection);
        connection.close();
    }

    @Test
    @DisplayName("listen with timeout returns non-null SSL server connection with timeout applied")
    void listenWithTimeoutReturnsServerConnection() throws IOException {
        KeyStoreSSLServerConnectionFactory factory = new KeyStoreSSLServerConnectionFactory(keystorePath, KEYSTORE_PASSWORD);
        ServerConnection connection = factory.listen(0, 5000);
        assertNotNull(connection);
        connection.close();
    }

    @Test
    @DisplayName("listen with timeout and backlog returns non-null SSL server connection")
    void listenWithTimeoutAndBacklogReturnsServerConnection() throws IOException {
        KeyStoreSSLServerConnectionFactory factory = new KeyStoreSSLServerConnectionFactory(keystorePath, KEYSTORE_PASSWORD);
        ServerConnection connection = factory.listen(0, 5000, 50);
        assertNotNull(connection);
        connection.close();
    }

    @Test
    @DisplayName("listen with InetAddress binds SSL server connection to the specified address")
    void listenWithInetAddressReturnsServerConnectionBoundToLoopback() throws IOException {
        KeyStoreSSLServerConnectionFactory factory = new KeyStoreSSLServerConnectionFactory(keystorePath, KEYSTORE_PASSWORD);
        InetAddress loopback = InetAddress.getByName("127.0.0.1");
        ServerConnection connection = factory.listen(loopback, 0);
        assertNotNull(connection);
        connection.close();
    }
}
