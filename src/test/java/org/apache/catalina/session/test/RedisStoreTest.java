package org.apache.catalina.session.test;

import junit.framework.Assert;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.session.PersistentManager;
import org.apache.catalina.session.RedisStore;
import org.apache.catalina.session.SessionSerializationHelper;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.util.StandardSessionIdGenerator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;
import redis.embedded.util.OS;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class RedisStoreTest extends Assert {
    private static PersistentManager manager;
    private static RedisStore rs;
    private static RedisServer redisServer;

    @BeforeClass
    public static void startUp() throws LifecycleException {
        redisServer = RedisServer.builder()
                          .redisExecProvider(RedisExecProvider.defaultProvider().override(OS.MAC_OS_X, "/opt/homebrew/opt/redis/bin/redis-server"))
                          .build();
        redisServer.start();

        rs = new RedisStore();
        manager = new PersistentManager();
        manager.setContext(new StandardContext());
        manager.setSessionIdGenerator(new StandardSessionIdGenerator());
        rs.setManager(manager);
        rs.setSessionSerializationHelper(new SessionSerializationHelper(manager));
        rs.setSessionEmptyLimit(0);
        rs.start();
    }

    @AfterClass
    public static void shutDown() {
        redisServer.stop();
    }

    @Test
    public void save() throws IOException {
        StandardSession session = new StandardSession(manager);
        session.setId("test-id");
        rs.save(session);

        Jedis j = new Jedis();
        j.connect();
        Map<String, String> data = j.hgetAll(session.getId());
        j.quit();
        j.disconnect();

        assertNotNull(data);

        ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new ByteArrayOutputStream()));

        session.writeObjectData(oos);
        oos.close();
        assertEquals(session.getId(), data.get("id"));
    }

    @Test
    public void load() throws IOException, ClassNotFoundException {
        Session savedSession = manager.createSession(null);
        ((StandardSession) savedSession).setAttribute("foo", "bar");
        rs.save(savedSession);

        assertNotNull(savedSession.getId());
        Session loadedSession = rs.load(savedSession.getId());

        assertNotNull(loadedSession);
        assertEquals(savedSession.getId(), loadedSession.getId());
        assertEquals("bar", ((StandardSession) loadedSession)
                .getAttribute("foo"));
    }
}
