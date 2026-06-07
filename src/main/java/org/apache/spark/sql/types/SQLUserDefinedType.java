package org.apache.spark.sql.types;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.spark.annotation.DeveloperApi;

/** Annotation for binding a user class to a Spark SQL UserDefinedType. */
@DeveloperApi
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SQLUserDefinedType {
  Class<? extends UserDefinedType<?>> udt();
}
