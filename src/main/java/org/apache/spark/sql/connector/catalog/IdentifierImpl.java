package org.apache.spark.sql.connector.catalog;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

/** An Identifier implementation. */
final class IdentifierImpl implements Identifier {
  private final String[] namespace;
  private final String name;

  IdentifierImpl(String[] namespace, String name) {
    this.namespace = Objects.requireNonNull(namespace, "Identifier namespace cannot be null");
    this.name = Objects.requireNonNull(name, "Identifier name cannot be null");
  }

  @Override
  public String[] namespace() {
    return namespace;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(".");
    for (String part : namespace) {
      joiner.add(quoteIfNeeded(part));
    }
    joiner.add(quoteIfNeeded(name));
    return joiner.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof IdentifierImpl that)) {
      return false;
    }
    return Arrays.equals(namespace, that.namespace) && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(name);
    result = 31 * result + Arrays.hashCode(namespace);
    return result;
  }

  private static String quoteIfNeeded(String ident) {
    if (ident.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      return ident;
    }
    return "`" + ident.replace("`", "``") + "`";
  }
}
