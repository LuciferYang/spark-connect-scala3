package org.apache.spark.sql.connect.common;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;

/**
 * Serialization proxy for {@code UdfPacket} that avoids the
 * {@code DefaultSerializationProxy}/{@code Seq} type-check mismatch in Java 17+.
 *
 * <h2>Problem</h2>
 * The server-side {@code UdfPacket} case class uses {@code defaultReadObject}.  Java 17+
 * checks that each deserialized field value is assignable to its declared type <em>before</em>
 * {@code readResolve} is called on the value.  Scala's {@code List} serializes via
 * {@code writeReplace} → {@code DefaultSerializationProxy}, which is <b>not</b> a subtype
 * of {@code scala.collection.immutable.Seq}.  If the {@code DefaultSerializationProxy}'s
 * own {@code readResolve} hasn't completed (or failed silently), the field type check
 * throws {@code ClassCastException}.
 *
 * <h2>Solution</h2>
 * On the client, {@code UdfPacket.writeReplace()} substitutes this proxy.  The proxy
 * stores encoders as a plain {@code Object[]} (no Scala collections).  On the server,
 * {@code readResolve} reconstructs the real Scala 2.13 {@code UdfPacket} case class
 * via reflection, converting the array back to a {@code Seq}.
 *
 * <p>Because the proxy is the top-level serialized object (not nested inside a
 * {@code defaultReadObject} call), its own {@code readResolve} runs <em>after</em>
 * all fields are fully resolved and there is no type-check issue.
 */
@SuppressWarnings("serial")
public final class UdfPacketSerializationProxy implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Object function;
    private final Object[] inputEncoders;
    private final Object outputEncoder;

    public UdfPacketSerializationProxy(Object function, Object[] inputEncoders, Object outputEncoder) {
        this.function = function;
        this.inputEncoders = inputEncoders;
        this.outputEncoder = outputEncoder;
    }

    /**
     * On deserialization, reconstruct the server's Scala 2.13 {@code UdfPacket} case class.
     */
    private Object readResolve() throws ObjectStreamException {
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();

            // Convert Object[] to scala.collection.immutable.Seq via
            // scala.jdk.CollectionConverters or manual construction.
            // Use scala.collection.immutable.ArraySeq$.MODULE$.unsafeWrapArray(Object[])
            // which returns an immutable.ArraySeq (subtype of immutable.Seq).
            Class<?> arraySeqModule = Class.forName(
                    "scala.collection.immutable.ArraySeq$", true, cl);
            Object arraySeqCompanion = arraySeqModule.getField("MODULE$").get(null);
            // unsafeWrapArray(Array[T]): ArraySeq[T] — wraps without copying
            java.lang.reflect.Method wrapMethod = null;
            for (java.lang.reflect.Method m : arraySeqModule.getMethods()) {
                if ("unsafeWrapArray".equals(m.getName())
                        && m.getParameterCount() == 1) {
                    wrapMethod = m;
                    break;
                }
            }
            if (wrapMethod == null) {
                throw new RuntimeException(
                        "Cannot find ArraySeq$.unsafeWrapArray method");
            }
            Object seq = wrapMethod.invoke(arraySeqCompanion, (Object) inputEncoders);

            // Construct the server-side UdfPacket via the 3-arg constructor.
            // Constructor signature (after erasure):
            //   UdfPacket(Object, Seq, AgnosticEncoder)
            // Use getDeclaredConstructors to find the 3-arg one.
            Class<?> udfPacketClass = Class.forName(
                    "org.apache.spark.sql.connect.common.UdfPacket", true, cl);
            Constructor<?> realCtor = null;
            for (Constructor<?> c : udfPacketClass.getDeclaredConstructors()) {
                if (c.getParameterCount() == 3) {
                    realCtor = c;
                    break;
                }
            }
            if (realCtor == null) {
                throw new RuntimeException("Cannot find 3-arg UdfPacket constructor");
            }
            realCtor.setAccessible(true);
            return realCtor.newInstance(function, seq, outputEncoder);
        } catch (java.lang.reflect.InvocationTargetException e) {
            java.io.InvalidObjectException ioe = new java.io.InvalidObjectException(
                    "Failed to reconstruct UdfPacket on server (InvocationTarget): "
                            + e.getCause());
            ioe.initCause(e.getCause());
            throw ioe;
        } catch (Exception e) {
            java.io.InvalidObjectException ioe = new java.io.InvalidObjectException(
                    "Failed to reconstruct UdfPacket on server: "
                            + e.getClass().getName() + ": " + e.getMessage());
            ioe.initCause(e);
            throw ioe;
        }
    }
}
