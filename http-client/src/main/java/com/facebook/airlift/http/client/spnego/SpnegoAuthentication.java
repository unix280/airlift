package com.facebook.airlift.http.client.spnego;

import com.facebook.airlift.http.client.KerberosNameType;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.sun.security.auth.module.Krb5LoginModule;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.eclipse.jetty.client.Authentication;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.util.Attributes;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.io.File;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
import static org.ietf.jgss.GSSContext.INDEFINITE_LIFETIME;
import static org.ietf.jgss.GSSCredential.DEFAULT_LIFETIME;
import static org.ietf.jgss.GSSCredential.INITIATE_ONLY;
import static org.ietf.jgss.GSSName.NT_USER_NAME;

public class SpnegoAuthentication
        implements Authentication
{
    private static final String NEGOTIATE = HttpHeader.NEGOTIATE.asString();
    private static final Logger LOG = Logger.get(SpnegoAuthentication.class);
    private static final Duration MIN_CREDENTIAL_LIFE_TIME = new Duration(60, TimeUnit.SECONDS);

    private static final GSSManager GSS_MANAGER = GSSManager.getInstance();

    private static final Oid SPNEGO_OID;
    private static final Oid KERBEROS_OID;

    static {
        try {
            SPNEGO_OID = new Oid("1.3.6.1.5.5.2");
            KERBEROS_OID = new Oid("1.2.840.113554.1.2.2");
        }
        catch (GSSException e) {
            throw new AssertionError(e);
        }
    }

    private static final RetryPolicy<?> AUTHENTICATION_RETRY_POLICY = new RetryPolicy<>()
            .withBackoff(1, 5, ChronoUnit.SECONDS)
            .withMaxRetries(5)
            .handleIf(SpnegoAuthentication::isNetworkIssue)
            .onRetry(event -> LOG.debug(
                    "Authentication failed on attempt %s, will retry. Exception: %s",
                    event.getAttemptCount(),
                    event.getLastFailure().getMessage()));

    private final File keytab;
    private final File credentialCache;
    private final String servicePrincipalPattern;
    private final String principal;
    private final String remoteServiceName;
    private final boolean useCanonicalHostname;
    private final Oid nameType;

    @GuardedBy("this")
    private Session clientSession;

    public SpnegoAuthentication(
            File keytab,
            File kerberosConfig,
            File credentialCache,
            String servicePrincipalPattern,
            String principal,
            String remoteServiceName,
            KerberosNameType nameType,
            boolean useCanonicalHostname)
    {
        requireNonNull(kerberosConfig, "Kerberos config path is null");
        requireNonNull(remoteServiceName, "Kerberos remote service name is null");
        requireNonNull(nameType, "GSS name type is null");

        this.keytab = keytab;
        this.credentialCache = credentialCache;
        this.servicePrincipalPattern = servicePrincipalPattern;
        this.principal = principal;
        this.remoteServiceName = remoteServiceName;
        this.nameType = nameType.getOid();
        this.useCanonicalHostname = useCanonicalHostname;

        System.setProperty("java.security.krb5.conf", kerberosConfig.getAbsolutePath());
    }

    @Override
    public Result authenticate(Request request, ContentResponse response, HeaderInfo headerInfo, Attributes attributes)
    {
        URI normalizedUri = UriUtil.normalizedUri(request.getURI());

        return new Result()
        {
            @Override
            public URI getURI()
            {
                return normalizedUri;
            }

            @Override
            public void apply(Request request)
            {
                Failsafe.with(AUTHENTICATION_RETRY_POLICY).run(() -> authenticate(request));
            }

            private void authenticate(Request request)
            {
                GSSContext context = null;
                try {
                    String servicePrincipal = makeServicePrincipal(servicePrincipalPattern, remoteServiceName, normalizedUri.getHost(), useCanonicalHostname);
                    Session session = getSession();
                    context = doAs(session.getLoginContext().getSubject(), () -> {
                        GSSContext result = GSS_MANAGER.createContext(
                                GSS_MANAGER.createName(servicePrincipal, nameType),
                                SPNEGO_OID,
                                session.getClientCredential(),
                                INDEFINITE_LIFETIME);

                        result.requestMutualAuth(true);
                        result.requestConf(true);
                        result.requestInteg(true);
                        result.requestCredDeleg(false);
                        return result;
                    });

                    byte[] token = context.initSecContext(new byte[0], 0, 0);
                    if (token != null) {
                        request.headers(headers -> headers.add(headerInfo.getHeader(), format("%s %s", NEGOTIATE, Base64.getEncoder().encodeToString(token))));
                    }
                    else {
                        throw new RuntimeException(format("No token generated from GSS context for %s", request.getURI()));
                    }
                }
                catch (GSSException e) {
                    throw new RuntimeException(format("Failed to establish GSSContext for request %s", request.getURI()), e);
                }
                catch (LoginException e) {
                    throw new RuntimeException(format("Failed to establish LoginContext for request %s", request.getURI()), e);
                }
                finally {
                    try {
                        if (context != null) {
                            context.dispose();
                        }
                    }
                    catch (GSSException e) {
                        // ignore
                    }
                }
            }
        };
    }

    private static boolean isNetworkIssue(Throwable throwable)
    {
        return Throwables.getCausalChain(throwable).stream()
                .anyMatch(SocketException.class::isInstance);
    }

    @Override
    public boolean matches(String type, URI uri, String realm)
    {
        // The class matches all requests for Negotiate scheme. Realm is not used for now
        return NEGOTIATE.equalsIgnoreCase(type);
    }

    private synchronized Session getSession()
            throws LoginException, GSSException
    {
        if (clientSession == null || clientSession.getClientCredential().getRemainingLifetime() < MIN_CREDENTIAL_LIFE_TIME.getValue(TimeUnit.SECONDS)) {
            // TODO: do we need to call logout() on the LoginContext?

            LoginContext loginContext = new LoginContext("", null, null, new Configuration()
            {
                @Override
                public AppConfigurationEntry[] getAppConfigurationEntry(String name)
                {
                    ImmutableMap.Builder<String, String> optionsBuilder = ImmutableMap.builder();
                    optionsBuilder.put("refreshKrb5Config", "true");
                    optionsBuilder.put("doNotPrompt", "true");
                    optionsBuilder.put("useKeyTab", "true");
                    if (LOG.isDebugEnabled()) {
                        optionsBuilder.put("debug", "true");
                    }

                    if (keytab != null) {
                        optionsBuilder.put("keyTab", keytab.getAbsolutePath());
                    }

                    if (credentialCache != null) {
                        optionsBuilder.put("ticketCache", credentialCache.getAbsolutePath());
                        optionsBuilder.put("useTicketCache", "true");
                        optionsBuilder.put("renewTGT", "true");
                    }

                    if (principal != null) {
                        optionsBuilder.put("principal", principal);
                    }

                    return new AppConfigurationEntry[] {
                            new AppConfigurationEntry(Krb5LoginModule.class.getName(), REQUIRED, optionsBuilder.build())
                    };
                }
            });

            loginContext.login();
            Subject subject = loginContext.getSubject();
            Principal clientPrincipal = subject.getPrincipals().iterator().next();
            GSSCredential clientCredential = doAs(subject, () -> GSS_MANAGER.createCredential(
                    GSS_MANAGER.createName(clientPrincipal.getName(), NT_USER_NAME),
                    DEFAULT_LIFETIME,
                    KERBEROS_OID,
                    INITIATE_ONLY));

            clientSession = new Session(loginContext, clientCredential);
        }

        return clientSession;
    }

    private static String makeServicePrincipal(String servicePrincipalPattern, String serviceName, String hostName, boolean useCanonicalHostname)
    {
        String serviceHostName = hostName;
        if (useCanonicalHostname) {
            serviceHostName = canonicalizeServiceHostname(hostName);
        }
        return servicePrincipalPattern.replaceAll("\\$\\{SERVICE}", serviceName).replaceAll("\\$\\{HOST}", serviceHostName.toLowerCase(Locale.US));
    }

    private static String canonicalizeServiceHostname(String hostName)
    {
        try {
            InetAddress address = InetAddress.getByName(hostName);
            String fullHostName;
            if ("localhost".equalsIgnoreCase(address.getHostName())) {
                fullHostName = InetAddress.getLocalHost().getCanonicalHostName();
            }
            else {
                fullHostName = address.getCanonicalHostName();
            }
            return fullHostName;
        }
        catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }

    private interface GssSupplier<T>
    {
        T get()
                throws GSSException;
    }

    private static <T> T doAs(Subject subject, GssSupplier<T> action)
    {
        return Subject.doAs(subject, (PrivilegedAction<T>) () -> {
            try {
                return action.get();
            }
            catch (GSSException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static class Session
    {
        private final LoginContext loginContext;
        private final GSSCredential clientCredential;

        Session(LoginContext loginContext, GSSCredential clientCredential)
        {
            requireNonNull(loginContext, "loginContext is null");
            requireNonNull(clientCredential, "gssCredential is null");

            this.loginContext = loginContext;
            this.clientCredential = clientCredential;
        }

        LoginContext getLoginContext()
        {
            return loginContext;
        }

        GSSCredential getClientCredential()
        {
            return clientCredential;
        }
    }
}
