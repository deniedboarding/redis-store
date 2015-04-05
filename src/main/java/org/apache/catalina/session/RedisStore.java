package org.apache.catalina.session;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.catalina.*;
import org.apache.catalina.util.CustomObjectInputStream;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisStore extends StoreBase implements Store {
    private static final byte[] DATA_FIELD = "data".getBytes();
    private static final byte[] ID_FIELD = "id".getBytes();
    private static Logger log = Logger.getLogger("RedisStore");
    /**
     * Redis Host
     */
    protected static String host = "localhost";

    /**
     * Redis Port
     */
    protected static int port = 6379;

    /**
     * Redis Password
     */
    protected static String password;

    /**
     * Redis database
     */
    protected static int database = 0;

    /**
     * The maximum inactive interval for Sessions in this Store.
     */
    protected static int maxInactiveInterval = -1;

    /**
     * Whether the serialised session should be compressed before saving in Redis.
     */
    protected static boolean deflate = false;

    /**
     * Size limit in bytes under which sessions are considered empty and thus not saved. Defaults to 300.
     */
    protected static int sessionEmptyLimit = 300;

    protected static SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    protected static JedisPool pool;

    /**
     * Get the redis host
     * 
     * @return host
     */
    public static String getHost() {
        return host;
    }

    /**
     * Set redis host
     * 
     * @param host
     *            Redis host
     */
    public static void setHost(String host) {
        RedisStore.host = host;
    }

    /**
     * Get redis port. Defaults to 6379.
     * 
     * @return port
     */
    public static int getPort() {
        return port;
    }

    /**
     * Set redis port
     * 
     * @param port
     *            Redis port
     */
    public static void setPort(int port) {
        RedisStore.port = port;
    }

    /**
     * Get redis password
     * 
     * @return password
     */
    public static String getPassword() {
        return password;
    }

    /**
     * Set redis password
     * 
     * @param password
     *            Redis password
     */
    public static void setPassword(String password) {
        RedisStore.password = password;
    }

    /**
     * Get redis database
     * 
     * @return Redis database
     */
    public static int getDatabase() {
        return database;
    }

    /**
     * Return the maximum inactive interval (in seconds)
     * for Sessions in this Store.
     * Set this instead of the equivalent on the Manager to have expiration done in Redis instead of in Tomcat.
     * This prevents Tomcat from reading sessions from Redis just to be able to expire them.
     */
    public static int getMaxInactiveInterval() {
        return maxInactiveInterval;
    }

    /**
     * Set the maximum inactive interval (in seconds)
     * for Sessions in this Store.
     *
     * @param interval The new default value
     */
    public static void setMaxInactiveInterval(int interval) {
        RedisStore.maxInactiveInterval = interval;
    }

    public static boolean isDeflate() {
        return deflate;
    }

    public static void setDeflate(boolean deflate) {
        RedisStore.deflate = deflate;
    }

    public static String getLogDateFormat() {
        return logDateFormat.toPattern();
    }

    /**
     * Sets format of expiry date in log messages.
     *
     * @param logDateFormat
     *            The format string in the syntax accepted by {@link java.text.SimpleDateFormat}.
     */
    public static void setLogDateFormat(String logDateFormat) {
        RedisStore.logDateFormat = new SimpleDateFormat(logDateFormat);
    }

    public static int getSessionEmptyLimit() {
        return sessionEmptyLimit;
    }

    /**
     *
     * @param sessionEmptyLimit
     *          The limit in bytes below which a serialised session is considered empty
     */
    public static void setSessionEmptyLimit(int sessionEmptyLimit) {
        RedisStore.sessionEmptyLimit = sessionEmptyLimit;
    }

    /**
     * Set redis database
     * 
     * @param database
     *            Redis database. Defaults to 0.
     */
    public static void setDatabase(int database) {
        RedisStore.database = database;
    }

    public void clear() throws IOException {
        Jedis jedis = pool.getResource();
        try {
            jedis.flushDB();
        } catch (JedisConnectionException e) {
            pool.returnBrokenResource(jedis);
            jedis = null;
            throw new IOException(e);
        } finally {
            if (jedis != null)
                pool.returnResource(jedis);
        }
    }

    public int getSize() throws IOException {
        Jedis jedis = pool.getResource();
        try {
            return jedis.dbSize().intValue();
        } catch (JedisConnectionException e) {
            pool.returnBrokenResource(jedis);
            jedis = null;
            throw new IOException(e);
        } finally {
            if (jedis != null)
                pool.returnResource(jedis);
        }
    }

    public String[] keys() throws IOException {
        Jedis jedis = pool.getResource();
        try {
            Set<String> keySet = jedis.keys("*");
            return keySet.toArray(new String[keySet.size()]);
        } catch (JedisConnectionException e) {
            pool.returnBrokenResource(jedis);
            jedis = null;
            throw new IOException(e);
        } finally {
            if (jedis != null)
                pool.returnResource(jedis);
        }
    }

    public Session load(String id) throws ClassNotFoundException, IOException {
        StandardSession session = null;
        ObjectInputStream ois;
        Container container = manager.getContainer();
        long start = System.currentTimeMillis();
        Jedis jedis = pool.getResource();
        Map<byte[], byte[]> hash;
        try {
            hash = jedis.hgetAll(id.getBytes());
        } catch (JedisConnectionException e) {
            pool.returnBrokenResource(jedis);
            jedis = null;
            throw new IOException(e);
        } finally {
            if (jedis != null)
                pool.returnResource(jedis);
        }
        if (!hash.isEmpty()) {
            try {
                ByteArrayInputStream in = new ByteArrayInputStream(hash.get(DATA_FIELD));
                BufferedInputStream bis = new BufferedInputStream(
                        deflate ? new InflaterInputStream(in) : in);

                if (manager.getContext() != null &&
                        manager.getContext().getLoader() != null &&
                        manager.getContext().getLoader().getClassLoader() != null) {
                    ois = new CustomObjectInputStream(bis, manager.getContext().getLoader().getClassLoader());
                } else {
                    ois = new ObjectInputStream(bis);
                }
                session = (StandardSession) manager.createEmptySession();
                session.readObjectData(ois);
                session.setManager(manager);
                // Not strictly true, but without it we seem to sometimes reach a race condition where a session is
                // swapped out by PersistentManagerBase.processMaxIdleBackups before session.access() is called.
                session.setCreationTime(System.currentTimeMillis());
                if (log.isLoggable(Level.INFO)) {
                    log.info("Loaded session id " + id + " In " + (System.currentTimeMillis() - start) + " ms. Size (B): "
                            + hash.get(DATA_FIELD).length);
                }
            } catch (Exception ex) {
                StringBuffer sb = new StringBuffer(ex.getMessage());
                for(StackTraceElement frame : ex.getStackTrace()) {
                    sb.append("\n\t");
                    sb.append(frame.toString());
                }
                log.severe("Failed to load session id " + id + ": " + sb);
            }
        } else {
            log.warning("No persisted data object found for id " + id);
        }
        return session;
    }

    public void remove(String id) throws IOException {
        Jedis jedis = pool.getResource();
        try {
            jedis.del(id);
            if (log.isLoggable(Level.INFO)) {
                log.info("Removed session id " + id);
            }
        } catch (JedisConnectionException e) {
            pool.returnBrokenResource(jedis);
            jedis = null;
            throw new IOException(e);
        } finally {
            if (jedis != null)
                pool.returnResource(jedis);
        }
    }

    public void save(Session session) throws IOException {
        ObjectOutputStream oos = null;
        ByteArrayOutputStream bos = null;
        long start = System.currentTimeMillis();

        Map<byte[], byte[]> hash = new HashMap<byte[], byte[]>();
        bos = new ByteArrayOutputStream();
        OutputStream os = deflate ? new DeflaterOutputStream(bos) : bos;

        oos = new ObjectOutputStream(
                new BufferedOutputStream(os));

        ((StandardSession) session).writeObjectData(oos);
        oos.close();
        oos = null;
        if (bos.size() < sessionEmptyLimit) {
            log.info("Session with id " + session.getIdInternal() + " not saved since its size ("
                    + bos.size() + "B) is below sessionEmptyLimit");
            return;
        }
        hash.put(ID_FIELD, session.getIdInternal().getBytes());
        hash.put(DATA_FIELD, bos.toByteArray());
        Jedis jedis = pool.getResource();
        try {
            jedis.hmset(session.getIdInternal().getBytes(), hash);
            if (maxInactiveInterval > -1) {
                long expireAt = ((StandardSession) session).thisAccessedTime / 1000L + maxInactiveInterval;
                jedis.expireAt(session.getIdInternal().getBytes(), expireAt);
                if (log.isLoggable(Level.INFO)) {
                    log.info("Expire session with id " + session.getIdInternal() + " at: "
                            + logDateFormat.format(new Date(expireAt * 1000L)));
                }
            }
        } catch (JedisConnectionException e) {
            pool.returnBrokenResource(jedis);
            jedis = null;
            throw new IOException(e);
        } finally {
            if (jedis != null)
                pool.returnResource(jedis);
        }
        if (log.isLoggable(Level.INFO)) {
            log.info("Saved session with id " + session.getIdInternal() + " In " +
                    (System.currentTimeMillis() - start) +
                    " ms. Size (B): "
                    + bos.size());
        }
    }

    @Override
    public void processExpires() {
        // Expiring in Redis is the problem of Redis if maxInactiveInterval is set, so in that case I do nothing here.
        if (maxInactiveInterval == -1) {
            super.processExpires();
        }
    }

    @Override
    protected synchronized void startInternal() throws LifecycleException {
        super.startInternal();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMinEvictableIdleTimeMillis(2000);
        poolConfig.setTimeBetweenEvictionRunsMillis(10000);
        pool = new JedisPool(poolConfig, getHost(), getPort(), Protocol.DEFAULT_TIMEOUT, null, getDatabase());
    }

    @Override
    protected synchronized void stopInternal() throws LifecycleException {
        super.stopInternal();
        pool.destroy();
    }
}
