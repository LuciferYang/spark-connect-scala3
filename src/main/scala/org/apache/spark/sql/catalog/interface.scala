package org.apache.spark.sql.catalog

import org.apache.spark.sql.Encoder

/** A catalog in Spark, as returned by the `listCatalogs` method defined in
  * [[org.apache.spark.sql.Catalog]].
  *
  * Mirrors upstream `org.apache.spark.sql.catalog.CatalogMetadata` but uses a Scala 3 case class
  * for compile-time encoder derivation in this thin client.
  */
case class CatalogMetadata(
    name: String,
    description: String
) derives Encoder

/** A database in Spark, as returned by the `listDatabases` method defined in
  * [[org.apache.spark.sql.Catalog]].
  */
case class Database(
    name: String,
    catalog: String,
    description: String,
    locationUri: String
) derives Encoder

/** A table in Spark, as returned by the `listTables` method defined in
  * [[org.apache.spark.sql.Catalog]].
  *
  * Note: upstream uses `Array[String]` for `namespace`; we use `Seq[String]` because our derived
  * encoder maps `Seq[T]` to `ArrayType` and Arrow `ListVector` decoding produces `Seq[Any]`.
  */
case class Table(
    name: String,
    catalog: String,
    namespace: Seq[String],
    description: String,
    tableType: String,
    isTemporary: Boolean
) derives Encoder

/** A column in Spark, as returned by the `listColumns` method defined in
  * [[org.apache.spark.sql.Catalog]].
  */
case class Column(
    name: String,
    description: String,
    dataType: String,
    nullable: Boolean,
    isPartition: Boolean,
    isBucket: Boolean,
    isCluster: Boolean
) derives Encoder

/** A user-defined function in Spark, as returned by the `listFunctions` method defined in
  * [[org.apache.spark.sql.Catalog]].
  */
case class Function(
    name: String,
    catalog: String,
    namespace: Seq[String],
    description: String,
    className: String,
    isTemporary: Boolean
) derives Encoder
