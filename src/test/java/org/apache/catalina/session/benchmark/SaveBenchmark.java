package org.apache.catalina.session.benchmark;

import java.io.IOException;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.catalina.Session;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.session.PersistentManager;
import org.apache.catalina.session.RedisStore;
import org.apache.catalina.session.StandardSession;

import redis.clients.jedis.Protocol;

public class SaveBenchmark {
    private static final int TOTAL_OPERATIONS = 1000;

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Logger.getLogger("RedisStore").setLevel(Level.OFF);

        String info = new String(new byte[30000]);
        RedisStore rs = new RedisStore();
        rs.setDatabase(0);
        rs.setHost("localhost");
        rs.setPassword("foobared");
        rs.setPort(Protocol.DEFAULT_PORT);

        PersistentManager manager = new PersistentManager();
        manager.setContext(new StandardContext());

        rs.setManager(manager);

        long begin = Calendar.getInstance().getTimeInMillis();
        for (int n = 0; n < TOTAL_OPERATIONS; n++) {
            Session session = manager.createSession(null);
            ((StandardSession) session).setAttribute("info", info);
            rs.save(session);
        }
        long ellapsed = Calendar.getInstance().getTimeInMillis() - begin;

        System.out.println((1000 * TOTAL_OPERATIONS / ellapsed)
                + " saves / second");
    }

}