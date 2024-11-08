/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.airlift.security;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.security.pem.PemReader;
import com.facebook.airlift.units.Duration;
import com.google.common.hash.HashCode;
import com.google.common.io.Files;

import javax.annotation.concurrent.GuardedBy;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.hash.Hashing.sha256;
import static java.util.Objects.requireNonNull;

public final class ReloadableSslContext
        implements Supplier<SSLContext>
{
    private static final Logger log = Logger.get(ReloadableSslContext.class);

    private final FileWatch trustCertificatesFileWatch;
    private final FileWatch clientCertificatesFileWatch;

    private final AtomicReference<SSLContext> sslContext;
    private final Duration sslContextRefreshTime;

    @GuardedBy("this")
    private Thread sslContextRefreshThread;

    @GuardedBy("this")
    private volatile boolean started;

    public ReloadableSslContext(File trustCertificatesFile, File clientCertificatesFile, Duration sslContextRefreshTime)
            throws IOException, GeneralSecurityException
    {
        this.trustCertificatesFileWatch = new FileWatch(requireNonNull(trustCertificatesFile, "trustCertificatesFile is null"));
        this.clientCertificatesFileWatch = new FileWatch(requireNonNull(clientCertificatesFile, "clientCertificatesFile is null"));
        this.sslContextRefreshTime = requireNonNull(sslContextRefreshTime, "sslContextRefreshTime is null");

        this.sslContext = new AtomicReference<>(loadSslContext());
    }

    @Override
    public SSLContext get()
    {
        checkState(started, "ReloadableSslContext must be in the started state");
        return sslContext.get();
    }

    public synchronized void start()
    {
        checkState(!started, "already started");
        refresh();
        sslContextRefreshThread = new Thread(this::run, "SSLContext Refresh Thread");
        sslContextRefreshThread.setDaemon(true);
        sslContextRefreshThread.start();
        started = true;
    }

    public synchronized void stop()
    {
        checkState(started, "must be started");
        sslContextRefreshThread.interrupt();
        sslContextRefreshThread = null;
        started = false;
    }

    private synchronized void refresh()
    {
        try {
            // every watch must be called each time to update status
            if (trustCertificatesFileWatch.updateState() || clientCertificatesFileWatch.updateState()) {
                sslContext.set(loadSslContext());
            }
        }
        catch (IOException | GeneralSecurityException | RuntimeException e) {
            // this method is not allowed to throw
            log.error(e, "Unable to reload SSL context");
        }
    }

    private void run()
    {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                refresh();
                Thread.sleep(sslContextRefreshTime.toMillis());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private SSLContext loadSslContext()
            throws IOException, GeneralSecurityException
    {
        KeyStore trustStore = PemReader.loadTrustStore(trustCertificatesFileWatch.getFile());
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        KeyStore keyStore = PemReader.loadKeyStore(clientCertificatesFileWatch.getFile(), clientCertificatesFileWatch.getFile(), Optional.empty());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, new char[0]);
        KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagerFactory.getTrustManagers(), null);
        return sslContext;
    }

    private static final class FileWatch
    {
        private final File file;
        private long lastModified = -1;
        private long length = -1;
        private HashCode hashCode = sha256().hashBytes(new byte[0]);

        public FileWatch(File file)
                throws IOException
        {
            this.file = requireNonNull(file, "file is null");
            updateState();
        }

        public File getFile()
        {
            return file;
        }

        public boolean updateState()
                throws IOException
        {
            // only check contents if length or modified time changed
            long newLastModified = file.lastModified();
            long newLength = file.length();
            if (lastModified == newLastModified && length == newLength) {
                return false;
            }

            // update stats
            lastModified = newLastModified;
            length = newLength;

            // check if contents changed
            HashCode newHashCode = Files.asByteSource(file).hash(sha256());
            if (Objects.equals(hashCode, newHashCode)) {
                return false;
            }
            hashCode = newHashCode;
            return true;
        }
    }
}
