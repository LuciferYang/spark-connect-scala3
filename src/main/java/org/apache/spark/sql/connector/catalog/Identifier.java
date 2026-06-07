package org.apache.spark.sql.connector.catalog;

/** Identifies an object in a catalog. */
public interface Identifier {
  static Identifier of(String[] namespace, String name) {
    return new IdentifierImpl(namespace, name);
  }

  /** Returns the namespace in the catalog. */
  String[] namespace();

  /** Returns the object name. */
  String name();
}
