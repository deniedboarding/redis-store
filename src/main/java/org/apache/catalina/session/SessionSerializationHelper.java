package org.apache.catalina.session;

import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.util.CustomObjectInputStream;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Utilities for serializing and deserializing {@link Session}s
 */
public final class SessionSerializationHelper {

    private final Manager manager;

    /**
     * Whether the serialised session should be compressed before saving in Redis.
     */
    private boolean deflate = false;

    /**
     * Creates a new instance
     *
     * @param manager the manager to use when recreating sessions
     */
    public SessionSerializationHelper(Manager manager) {
        this.manager = manager;
    }

    public void setDeflate(boolean deflate) {
        this.deflate = deflate;
    }

    public boolean isDeflate() {
        return deflate;
    }

    /**
     * Deserialize a {@link Session}
     *
     * @param session a {@code byte[]} representing the serialized {@link Session}
     * @return the deserialized {@link Session} or {@code null} if the session data is {@code null}
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public Session deserialize(byte[] session) throws ClassNotFoundException, IOException {
        if (session == null) {
            return null;
        }
        ByteArrayInputStream in = null;
        BufferedInputStream bis = null;
        ObjectInputStream ois = null;

        try {
            in = new ByteArrayInputStream(session);
            bis = new BufferedInputStream(deflate ? new InflaterInputStream(in) : in);

            if (manager.getContext() != null &&
                    manager.getContext().getLoader() != null &&
                    manager.getContext().getLoader().getClassLoader() != null) {
                ois = new CustomObjectInputStream(bis, manager.getContext().getLoader().getClassLoader());
            } else {
                ois = new ObjectInputStream(bis);
            }
            StandardSession standardSession = (StandardSession) manager.createEmptySession();
            standardSession.readObjectData(ois);
            standardSession.setManager(manager);
            // Not strictly true, but without it we seem to sometimes reach a race condition where a session is
            // swapped out by PersistentManagerBase.processMaxIdleBackups before session.access() is called.
            standardSession.setCreationTime(System.currentTimeMillis());

            return standardSession;
        } finally {
            closeQuietly(in, bis, ois);
        }
    }

    /**
     * Serialize a {@link Session}
     */
    public Map<byte[], byte[]> serialize(Session session) throws IOException {

        ObjectOutputStream oos = null;
        ByteArrayOutputStream bos = null;
        OutputStream os = null;
        try {

            bos = new ByteArrayOutputStream();
            os = deflate ? new DeflaterOutputStream(bos) : bos;
            oos = new ObjectOutputStream(new BufferedOutputStream(os));
            StandardSession standardSession = (StandardSession) session;
            standardSession.writeObjectData(oos);
            oos.close();
            oos = null;

            Map<byte[], byte[]> hash = new HashMap<>();
            hash.put(RedisStore.ID_FIELD, session.getIdInternal().getBytes());
            hash.put(RedisStore.DATA_FIELD, bos.toByteArray());

            return hash;
        } finally {
            closeQuietly(oos, bos, os);
        }

    }

    private void closeQuietly(Closeable... closeables) {
        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception e) {
                // Nothing to do
            }
        }
    }
}
