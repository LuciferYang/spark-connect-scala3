package org.apache.spark.sql.types;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Java factory facade for Spark SQL data types. */
public final class DataTypes {
  private DataTypes() {}

  public static final DataType StringType = StringType$.MODULE$;
  public static final DataType BinaryType = BinaryType$.MODULE$;
  public static final DataType BooleanType = BooleanType$.MODULE$;
  public static final DataType DateType = DateType$.MODULE$;
  public static final DataType TimestampType = TimestampType$.MODULE$;
  public static final DataType TimestampNTZType = TimestampNTZType$.MODULE$;
  public static final DataType CalendarIntervalType = CalendarIntervalType$.MODULE$;
  public static final DataType DoubleType = DoubleType$.MODULE$;
  public static final DataType FloatType = FloatType$.MODULE$;
  public static final DataType ByteType = ByteType$.MODULE$;
  public static final DataType IntegerType = IntegerType$.MODULE$;
  public static final DataType LongType = LongType$.MODULE$;
  public static final DataType ShortType = ShortType$.MODULE$;
  public static final DataType NullType = NullType$.MODULE$;
  public static final DataType VariantType = VariantType$.MODULE$;

  public static ArrayType createArrayType(DataType elementType) {
    return createArrayType(elementType, true);
  }

  public static ArrayType createArrayType(DataType elementType, boolean containsNull) {
    requireNonNull(elementType, "elementType");
    return new ArrayType(elementType, containsNull);
  }

  public static DecimalType createDecimalType(int precision, int scale) {
    return DecimalType$.MODULE$.apply(precision, scale);
  }

  public static DecimalType createDecimalType() {
    return DecimalType$.MODULE$.DEFAULT();
  }

  public static MapType createMapType(DataType keyType, DataType valueType) {
    return createMapType(keyType, valueType, true);
  }

  public static MapType createMapType(
      DataType keyType,
      DataType valueType,
      boolean valueContainsNull) {
    requireNonNull(keyType, "keyType");
    requireNonNull(valueType, "valueType");
    return new MapType(keyType, valueType, valueContainsNull);
  }

  public static StructField createStructField(
      String name,
      DataType dataType,
      boolean nullable,
      Metadata metadata) {
    requireNonNull(name, "name");
    requireNonNull(dataType, "dataType");
    requireNonNull(metadata, "metadata");
    return new StructField(name, dataType, nullable, metadata);
  }

  public static StructField createStructField(String name, DataType dataType, boolean nullable) {
    return createStructField(name, dataType, nullable, Metadata.empty());
  }

  public static StructType createStructType(List<StructField> fields) {
    requireNonNull(fields, "fields");
    return createStructType(fields.toArray(new StructField[0]));
  }

  public static StructType createStructType(StructField[] fields) {
    requireNonNull(fields, "fields");
    Set<String> names = new HashSet<>();
    for (StructField field : fields) {
      requireNonNull(field, "fields");
      names.add(field.name());
    }
    if (names.size() != fields.length) {
      throw new IllegalArgumentException("fields should have distinct names.");
    }
    return StructType$.MODULE$.apply(fields);
  }

  public static CharType createCharType(int length) {
    return new CharType(length);
  }

  public static VarcharType createVarcharType(int length) {
    return new VarcharType(length);
  }

  public static TimeType createTimeType(int precision) {
    return new TimeType(precision);
  }

  public static TimeType createTimeType() {
    return new TimeType(TimeType$.MODULE$.DEFAULT_PRECISION());
  }

  public static GeometryType createGeometryType(int srid) {
    return new GeometryType(srid);
  }

  public static GeometryType createGeometryType(String crs) {
    return new GeometryType(parseSrid(crs, GeometryType$.MODULE$.DEFAULT_SRID()));
  }

  public static GeographyType createGeographyType(int srid) {
    return new GeographyType(srid);
  }

  public static GeographyType createGeographyType(String crs) {
    return new GeographyType(parseSrid(crs, GeographyType$.MODULE$.DEFAULT_SRID()));
  }

  private static int parseSrid(String crs, int defaultSrid) {
    requireNonNull(crs, "crs");
    String normalized = crs.trim().toUpperCase(java.util.Locale.ROOT);
    if (normalized.isEmpty()) {
      return defaultSrid;
    }
    if (normalized.startsWith("EPSG:")) {
      return Integer.parseInt(normalized.substring("EPSG:".length()));
    }
    return Integer.parseInt(normalized);
  }

  private static void requireNonNull(Object value, String name) {
    if (value == null) {
      throw new IllegalArgumentException(name + " should not be null.");
    }
  }
}
