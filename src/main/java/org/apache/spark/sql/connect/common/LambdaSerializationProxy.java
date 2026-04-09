package org.apache.spark.sql.connect.common;

import java.io.Serializable;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleInfo;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.SerializedLambda;

/**
 * A cross-version serialization proxy that replaces {@code SerializedLambda} in the byte stream.
 *
 * <h2>Problem</h2>
 * Scala 3 lambdas serialize via {@code writeReplace} → {@code SerializedLambda}. On
 * deserialization, {@code SerializedLambda.readResolve()} calls {@code $deserializeLambda$} on
 * the capturing class. The Scala 3 version of that method requires
 * {@code scala.runtime.Scala3LambdaDeserialize}, which is not on the Scala 2.13 server
 * classpath — causing {@code ArrayStoreException} or {@code NoSuchMethodException}.
 *
 * <h2>Solution</h2>
 * Intercept {@code SerializedLambda} during serialization (via
 * {@code ObjectOutputStream.replaceObject}) and substitute this proxy. The proxy stores the
 * lambda's metadata (impl class, method name, descriptor, captured args) and implements
 * {@code Function0} and {@code Function1}.
 *
 * <p>On deserialization, the proxy is a regular {@code Serializable} class — no
 * {@code SerializedLambda}, no {@code $deserializeLambda$}. The {@code MethodHandle} to the impl
 * method is lazily resolved on first {@code apply()}. Since the impl class's {@code .class} file
 * is uploaded via {@code addClassDir}, the server's session classloader can find it.
 *
 * <p>Only {@code Function0}, {@code Function1}, and {@code Function2} are implemented as
 * interfaces. Higher-arity {@code apply} overloads are provided as regular methods to avoid
 * {@code tupled}/{@code curried} conflicts between Scala's {@code FunctionN} traits. All SC3
 * UDF adaptors ({@code FlatMapAdaptor}, {@code MapPartitionsAdaptor}, etc.) are {@code Function1},
 * and multi-arg UDFs use {@code Function2}, so this covers all current use cases.
 *
 * <p>Written in Java to avoid Scala 3's trait linearization conflicts.
 */
public final class LambdaSerializationProxy
        implements Serializable,
        scala.Function0<Object>,
        scala.Function1<Object, Object>,
        scala.Function2<Object, Object, Object> {

    private static final long serialVersionUID = 1L;

    private final String implClassName;
    private final String implMethodName;
    private final String implMethodDescriptor;
    private final int implMethodKind;
    private final Object[] capturedArgs;

    private transient MethodHandle mh;

    public LambdaSerializationProxy(
            String implClassName,
            String implMethodName,
            String implMethodDescriptor,
            int implMethodKind,
            Object[] capturedArgs) {
        this.implClassName = implClassName;
        this.implMethodName = implMethodName;
        this.implMethodDescriptor = implMethodDescriptor;
        this.implMethodKind = implMethodKind;
        this.capturedArgs = capturedArgs;
    }

    private MethodHandle getMethodHandle() {
        if (mh == null) {
            mh = resolveMethodHandle();
        }
        return mh;
    }

    private MethodHandle resolveMethodHandle() {
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            Class<?> implClass = Class.forName(implClassName.replace('/', '.'), true, loader);
            MethodHandles.Lookup lookup =
                    MethodHandles.privateLookupIn(implClass, MethodHandles.lookup());
            MethodType mt = MethodType.fromMethodDescriptorString(implMethodDescriptor, loader);
            switch (implMethodKind) {
                case MethodHandleInfo.REF_invokeStatic:
                    return lookup.findStatic(implClass, implMethodName, mt);
                case MethodHandleInfo.REF_invokeVirtual:
                case MethodHandleInfo.REF_invokeInterface:
                    return lookup.findVirtual(
                            implClass, implMethodName, mt.dropParameterTypes(0, 1));
                case MethodHandleInfo.REF_invokeSpecial:
                    return lookup.findSpecial(implClass, implMethodName, mt, implClass);
                case MethodHandleInfo.REF_newInvokeSpecial:
                    return lookup.findConstructor(implClass, mt);
                default:
                    return lookup.findStatic(implClass, implMethodName, mt);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve method handle for " + implClassName + "." + implMethodName,
                    e);
        }
    }

    private Object invokeImpl(Object[] samArgs) {
        try {
            Object[] allArgs = new Object[capturedArgs.length + samArgs.length];
            System.arraycopy(capturedArgs, 0, allArgs, 0, capturedArgs.length);
            System.arraycopy(samArgs, 0, allArgs, capturedArgs.length, samArgs.length);
            return getMethodHandle().invokeWithArguments(allArgs);
        } catch (Throwable t) {
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            if (t instanceof Error) throw (Error) t;
            throw new RuntimeException(t);
        }
    }

    // Function0
    @Override
    public Object apply() {
        return invokeImpl(new Object[0]);
    }

    // Function1
    @Override
    public Object apply(Object v1) {
        return invokeImpl(new Object[]{v1});
    }

    // Additional apply overloads for higher arities (Function2.apply is via interface)

    // Function2
    @Override
    public Object apply(Object v1, Object v2) {
        return invokeImpl(new Object[]{v1, v2});
    }

    public Object apply(Object v1, Object v2, Object v3) {
        return invokeImpl(new Object[]{v1, v2, v3});
    }

    public Object apply(Object v1, Object v2, Object v3, Object v4) {
        return invokeImpl(new Object[]{v1, v2, v3, v4});
    }

    public Object apply(Object v1, Object v2, Object v3, Object v4, Object v5) {
        return invokeImpl(new Object[]{v1, v2, v3, v4, v5});
    }

    @Override
    public String toString() {
        return "LambdaSerializationProxy(" + implClassName.replace('/', '.') + "."
                + implMethodName + ")";
    }

    /**
     * Create a proxy from a {@code SerializedLambda} extracted during serialization.
     */
    public static LambdaSerializationProxy fromSerializedLambda(SerializedLambda sl) {
        Object[] captured = new Object[sl.getCapturedArgCount()];
        for (int i = 0; i < captured.length; i++) {
            captured[i] = sl.getCapturedArg(i);
        }
        return new LambdaSerializationProxy(
                sl.getImplClass(),
                sl.getImplMethodName(),
                sl.getImplMethodSignature(),
                sl.getImplMethodKind(),
                captured);
    }
}
