package org.apache.spark.sql.types;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Represents a Geometry value (WKB bytes + SRID).
 *
 * <p>Instances are created via the static {@link #fromWKB} factory methods.
 */
public final class Geometry implements Serializable {
  protected final byte[] value;
  protected final int srid;

  private Geometry(byte[] value, int srid) {
    this.value = value;
    this.srid = srid;
  }

  public byte[] getBytes() {
    return value;
  }

  public int getSrid() {
    return srid;
  }

  public static final int DEFAULT_SRID = 0;

  public static Geometry fromWKB(byte[] bytes, int srid) {
    return new Geometry(bytes, srid);
  }

  public static Geometry fromWKB(byte[] bytes) {
    return new Geometry(bytes, DEFAULT_SRID);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Geometry other = (Geometry) obj;
    return srid == other.srid && Arrays.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(value) + Integer.hashCode(srid);
  }
}
