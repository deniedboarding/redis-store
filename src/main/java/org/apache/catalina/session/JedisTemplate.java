package org.apache.catalina.session;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.logging.Logger;

import static java.util.logging.Level.WARNING;

final class JedisTemplate {

    private final JedisPool jedisPool;

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    JedisTemplate(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    <T> T withJedis(JedisOperation<T> operation) {
        Jedis jedis = null;
        try {
            jedis = this.jedisPool.getResource();
            return operation.invoke(jedis);
        } catch (JedisConnectionException e) {
            returnBrokenResourceQuietly(jedis);
            jedis = null;
            throw e;
        } finally {
            returnResourceQuietly(jedis);
        }
    }

    private void returnBrokenResourceQuietly(Jedis jedis) {
        if (jedis != null) {
            try {
                this.jedisPool.returnBrokenResource(jedis);
            } catch (RuntimeException e) {
                this.logger.log(WARNING, "Exception encountered when returning broken Jedis resource", e);
            }
        }
    }

    private void returnResourceQuietly(Jedis jedis) {
        if (jedis != null) {
            try {
                this.jedisPool.returnResource(jedis);
            } catch (RuntimeException e) {
                this.logger.log(WARNING, "Exception encountered when returning Jedis resource", e);
            }
        }
    }

    interface JedisOperation<T> {

        /**
         * Invoke the operation
         *
         * @param jedis the {@link Jedis} instance to use
         * @return the return value of the operation
         */
        T invoke(Jedis jedis);
    }

}
