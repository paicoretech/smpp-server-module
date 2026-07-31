package com.paicbd.module.server;


import com.paicbd.smsc.exception.RTException;
import org.jsmpp.session.connection.ServerConnection;
import org.jsmpp.session.connection.ServerConnectionFactory;
import org.jsmpp.session.connection.socket.ServerSocketConnection;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

public class KeyStoreSSLServerConnectionFactory implements ServerConnectionFactory {

    private final SSLServerSocketFactory sslServerSocketFactory;

    public KeyStoreSSLServerConnectionFactory(String keystorePath, String keystorePassword) {
        if (keystorePath == null || keystorePath.isBlank()) {
            throw new IllegalArgumentException("Keystore file path must not be empty when TLS is enabled");
        }
        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            try (FileInputStream fis = new FileInputStream(keystorePath)) {
                keyStore.load(fis, keystorePassword.toCharArray());
            }
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
            keyManagerFactory.init(keyStore, keystorePassword.toCharArray());
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
            sslServerSocketFactory = sslContext.getServerSocketFactory();
        } catch (GeneralSecurityException | IOException e) {
            throw new RTException("Failed to initialize TLS keystore from '" + keystorePath + "': " + e.getMessage(), e);
        }
    }

    @Override
    public ServerConnection listen(int port) throws IOException {
        ServerSocket serverSocket = sslServerSocketFactory.createServerSocket(port);
        return new ServerSocketConnection(serverSocket);
    }

    @Override
    public ServerConnection listen(int port, int timeout) throws IOException {
        ServerSocket serverSocket = sslServerSocketFactory.createServerSocket(port);
        serverSocket.setSoTimeout(timeout);
        return new ServerSocketConnection(serverSocket);
    }

    @Override
    public ServerConnection listen(int port, int timeout, int backlog) throws IOException {
        ServerSocket serverSocket = sslServerSocketFactory.createServerSocket(port, backlog);
        serverSocket.setSoTimeout(timeout);
        return new ServerSocketConnection(serverSocket);
    }

    @Override
    public ServerConnection listen(InetAddress inetAddress, int port) throws IOException {
        // Using backlog 50 since that is the default value
        ServerSocket serverSocket = sslServerSocketFactory.createServerSocket(port, 50, inetAddress);
        return new ServerSocketConnection(serverSocket);
    }
}
