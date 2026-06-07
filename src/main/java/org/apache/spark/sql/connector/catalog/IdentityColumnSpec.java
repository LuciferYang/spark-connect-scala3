package org.apache.spark.sql.connector.catalog;

import java.util.Objects;

/** Identity column specification. */
public class IdentityColumnSpec {
  private final long start;
  private final long step;
  private final boolean allowExplicitInsert;

  public IdentityColumnSpec(long start, long step, boolean allowExplicitInsert) {
    this.start = start;
    this.step = step;
    this.allowExplicitInsert = allowExplicitInsert;
  }

  public long getStart() {
    return start;
  }

  public long getStep() {
    return step;
  }

  public boolean isAllowExplicitInsert() {
    return allowExplicitInsert;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    IdentityColumnSpec that = (IdentityColumnSpec) other;
    return start == that.start
        && step == that.step
        && allowExplicitInsert == that.allowExplicitInsert;
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, step, allowExplicitInsert);
  }

  @Override
  public String toString() {
    return "IdentityColumnSpec{"
        + "start=" + start
        + ", step=" + step
        + ", allowExplicitInsert=" + allowExplicitInsert
        + "}";
  }
}
