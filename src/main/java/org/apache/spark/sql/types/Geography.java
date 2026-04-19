package org.apache.spark.sql.types;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Represents a Geography value (WKB bytes + SRID).
 *
 * <p>Instances are created via the static {@link #fromWKB} factory methods.
 */
public final class Geography implements Serializable {
  protected final byte[] value;
  protected final int srid;

  private Geography(byte[] value, int srid) {
    this.value = value;
    this.srid = srid;
  }

  public byte[] getBytes() {
    return value;
  }

  public int getSrid() {
    return srid;
  }

  public static final int DEFAULT_SRID = 4326;

  public static Geography fromWKB(byte[] bytes, int srid) {
    return new Geography(bytes, srid);
  }

  public static Geography fromWKB(byte[] bytes) {
    return new Geography(bytes, DEFAULT_SRID);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Geography other = (Geography) obj;
    return srid == other.srid && Arrays.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(value) + Integer.hashCode(srid);
  }
}
