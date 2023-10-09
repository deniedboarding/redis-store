package org.apache.catalina.session;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.Store;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisStore extends StoreBase implements Store {

	public static final byte[] DATA_FIELD = "data".getBytes();
	public static final byte[] ID_FIELD = "id".getBytes();

	private final Logger logger = LoggerFactory.getLogger(getClass());

	/**
	 * Redis Host
	 */
	private volatile String host = "localhost";
	/**
	 * Redis Port
	 */
	private volatile int port = 6379;
	/**
	 * Redis Password
	 */
	private volatile String password;
	/**
	 * Redis database
	 */
	private volatile int database = 0;
	/**
	 * Redis pool size
	 */
	private volatile int connectionPoolSize = -1;
	/**
	 * Date format for logging dates
	 */
	private volatile SimpleDateFormat expireDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	/**
	 * Helper for serializing/deserializing
	 */
	private volatile SessionSerializationHelper sessionSerializationHelper;
	/**
	 * The jedis pool
	 */
	private volatile JedisPool pool;
	/**
	 * Template helper for handling of the jedis connections
	 */
	private volatile JedisTemplate jedisTemplate;
	/**
	 * The maximum inactive interval for Sessions in this Store.
	 */
	private volatile int maxInactiveInterval = -1;
	/**
	 * Whether the serialised session should be compressed before saving in Redis.
	 */
	private volatile boolean deflate = false;
	/**
	 * Size limit in bytes under which sessions are considered empty and thus not saved. Defaults to 300.
	 */
	private volatile int sessionEmptyLimit = 300;

	/**
	 * Return the maximum inactive interval (in seconds)
	 * for Sessions in this Store.
	 * Set this instead of the equivalent on the Manager to have expiration done in Redis instead of in Tomcat.
	 * This prevents Tomcat from reading sessions from Redis just to be able to expire them.
	 */
	public int getMaxInactiveInterval() {
		return this.maxInactiveInterval;
	}

	/**
	 * Set the maximum inactive interval (in seconds)
	 * for Sessions in this Store.
	 *
	 * @param interval The new default value
	 */
	public void setMaxInactiveInterval(int interval) {
		this.maxInactiveInterval = interval;
	}

	/**
	 * Set whether the serialised session should be compressed before saving in Redis.
	 *
	 * @param deflate If persisted session should be compressed
	 */
	public void setDeflate(boolean deflate) {
		this.deflate = deflate;
	}

	/**
	 * @param sessionEmptyLimit The limit in bytes below which a serialised session is considered empty
	 */
	public void setSessionEmptyLimit(int sessionEmptyLimit) {
		this.sessionEmptyLimit = sessionEmptyLimit;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setConnectionPoolSize(int connectionPoolSize) {
		this.connectionPoolSize = connectionPoolSize;
	}

	public void setPool(JedisPool pool) {
		this.pool = pool;
	}

	public void setExpireDateFormat(SimpleDateFormat expireDateFormat) {
		this.expireDateFormat = expireDateFormat;
	}

	public void setJedisTemplate(JedisTemplate jedisTemplate) {
		this.jedisTemplate = jedisTemplate;
	}

	public void setSessionSerializationHelper(SessionSerializationHelper sessionSerializationHelper) {
		this.sessionSerializationHelper = sessionSerializationHelper;
	}

	@Override
	public void setManager(final Manager manager) {
		this.manager = manager;
		this.sessionSerializationHelper = new SessionSerializationHelper(manager);
		this.sessionSerializationHelper.setDeflate(deflate);
	}

	@Override
	public void clear() throws IOException {
		try {
			jedisTemplate.withJedis(new JedisTemplate.JedisOperation<Void>() {
				@Override
				public Void invoke(Jedis jedis) {
					jedis.flushDB();
					return null;
				}
			});
		} catch (JedisConnectionException e) {
			throw new IOException(e);
		}
	}

	@Override
	public int getSize() throws IOException {
		try {
			return jedisTemplate.withJedis(new JedisTemplate.JedisOperation<Integer>() {
				@Override
				public Integer invoke(Jedis jedis) {
					return jedis.dbSize().intValue();
				}
			});
		} catch (JedisConnectionException e) {
			throw new IOException(e);
		}
	}

	@Override
	public String[] keys() throws IOException {
		try {
			return jedisTemplate.withJedis(new JedisTemplate.JedisOperation<String[]>() {
				@Override
				public String[] invoke(Jedis jedis) {
					Set<String> keySet = jedis.keys("*");
					return keySet.toArray(new String[keySet.size()]);
				}
			});
		} catch (JedisConnectionException e) {
			throw new IOException(e);
		}
	}

	@Override
	public Session load(final String id) throws ClassNotFoundException, IOException {
		final long start = System.currentTimeMillis();

		try {
			return jedisTemplate.withJedis(new JedisTemplate.JedisOperation<Session>() {
				@Override
				public Session invoke(Jedis jedis) {
					Map<byte[], byte[]> hash = jedis.hgetAll(id.getBytes());
					try {
						Session session = sessionSerializationHelper.deserialize(hash.get(DATA_FIELD));

						if (session != null) {
							logger.info(String.format("Loaded session id %s In %d ms. Size (B): %d",
									id,
									System.currentTimeMillis() - start,
									hash.get(DATA_FIELD).length));
						} else {
							logger.info(String.format("no session found to load for id %s", id));
						}

						return session;
					} catch (ClassNotFoundException | IOException e) {
						logger.error( "Failed to deserialize session id " + id, e);
						return manager.createSession(id);
					}
				}
			});
		} catch (JedisConnectionException e) {
			logger.error("Failed to load session id " + id, e);
			return manager.createSession(id);
		}
	}

	@Override
	public void remove(final String id) throws IOException {
		jedisTemplate.withJedis(new JedisTemplate.JedisOperation<Void>() {
			@Override
			public Void invoke(Jedis jedis) {
				jedis.del(id);
				logger.info("Removed session id " + id);
				return null;
			}
		});
	}

	@Override
	public void save(final Session session) throws IOException {
		long start = System.currentTimeMillis();

		final Map<byte[], byte[]> hash;
		try {
			hash = sessionSerializationHelper.serialize(session);
		} catch (IOException e) {
			logger.error(String.format("Unable to save session %s", session.getIdInternal()), e);
			return;
		}

		int sessionSize = hash.get(DATA_FIELD).length;
		if (sessionSize < sessionEmptyLimit) {
			logger.debug(String.format("Session with id %s not saved since its size (%d B) is below sessionEmptyLimit",
					session.getIdInternal(),
					sessionSize));
			return;
		}

		try {
			jedisTemplate.withJedis(new JedisTemplate.JedisOperation<Void>() {
				@Override
				public Void invoke(Jedis jedis) {
					jedis.hmset(session.getIdInternal().getBytes(), hash);

					if (maxInactiveInterval > -1) {
						setRedisSessionExpire(session, jedis);
					}
					return null;
				}
			});

			logger.info(String.format("Saved session with id %s In %d ms. Size: %d B",
					session.getIdInternal(),
					System.currentTimeMillis() - start,
					sessionSize));

		} catch (JedisConnectionException e) {
			logger.error(String.format("Unable to save session %s", session.getIdInternal()), e);
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
	public synchronized void startInternal() throws LifecycleException {
		super.startInternal();
		if (pool == null) {
			JedisPoolConfig poolConfig = new JedisPoolConfig();
			poolConfig.setMaxTotal(this.connectionPoolSize);
			poolConfig.setMinEvictableIdleTimeMillis(2000);
			poolConfig.setTimeBetweenEvictionRunsMillis(10000);

			pool = new JedisPool(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, password, database);

			jedisTemplate = new JedisTemplate(pool);
		}
	}

	@Override
	protected synchronized void stopInternal() throws LifecycleException {
		super.stopInternal();
		if (pool != null) {
			pool.destroy();
		}
	}

	private void setRedisSessionExpire(Session session, Jedis jedis) {
		long expireAt = ((StandardSession) session).thisAccessedTime / 1000L + maxInactiveInterval;
		jedis.expireAt(session.getIdInternal().getBytes(), expireAt);
		logger.info(String.format("Expire session with id %s at: %s",
				session.getIdInternal(),
				expireDateFormat.format(new Date(expireAt * 1000L))));
	}
}
