package org.apache.spark.sql.tags;

import java.lang.annotation.*;
import org.scalatest.TagAnnotation;

/**
 * Tag for integration tests that require a running Spark Connect server.
 *
 * <p>Annotate a test suite class with {@code @IntegrationTest} to tag all its
 * tests. Tagged tests are excluded from the default {@code sbt test} run and
 * can be executed explicitly with:
 * <pre>
 *   build/sbt 'testOnly -- -n org.apache.spark.sql.tags.IntegrationTest'
 * </pre>
 */
@TagAnnotation
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface IntegrationTest {}
