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
) derives Encoder:
  override def toString: String =
    s"Catalog[name='$name', ${Option(description).map(d => s"description='$d'").getOrElse("")}]"

/** A database in Spark, as returned by the `listDatabases` method defined in
  * [[org.apache.spark.sql.Catalog]].
  */
case class Database(
    name: String,
    catalog: String,
    description: String,
    locationUri: String
) derives Encoder:

  def this(name: String, description: String, locationUri: String) =
    this(name, null, description, locationUri)

  override def toString: String =
    "Database[" +
      s"name='$name', " +
      Option(catalog).map(c => s"catalog='$c', ").getOrElse("") +
      Option(description).map(d => s"description='$d', ").getOrElse("") +
      s"path='$locationUri']"

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
) derives Encoder:

  if namespace != null then assert(namespace.forall(_ != null))

  def this(
      name: String,
      catalog: String,
      namespace: Array[String],
      description: String,
      tableType: String,
      isTemporary: Boolean
  ) =
    this(
      name,
      catalog,
      Option(namespace).map(_.toIndexedSeq).orNull,
      description,
      tableType,
      isTemporary
    )

  def this(
      name: String,
      database: String,
      description: String,
      tableType: String,
      isTemporary: Boolean
  ) =
    this(
      name,
      null,
      if database != null then Seq(database) else null,
      description,
      tableType,
      isTemporary
    )

  def database: String =
    if namespace != null && namespace.length == 1 then namespace.head else null

  override def toString: String =
    "Table[" +
      s"name='$name', " +
      Option(catalog).map(c => s"catalog='$c', ").getOrElse("") +
      Option(database).map(d => s"database='$d', ").getOrElse("") +
      Option(description).map(d => s"description='$d', ").getOrElse("") +
      s"tableType='$tableType', " +
      s"isTemporary='$isTemporary']"

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
) derives Encoder:

  def this(
      name: String,
      description: String,
      dataType: String,
      nullable: Boolean,
      isPartition: Boolean,
      isBucket: Boolean
  ) =
    this(name, description, dataType, nullable, isPartition, isBucket, isCluster = false)

  override def toString: String =
    "Column[" +
      s"name='$name', " +
      Option(description).map(d => s"description='$d', ").getOrElse("") +
      s"dataType='$dataType', " +
      s"nullable='$nullable', " +
      s"isPartition='$isPartition', " +
      s"isBucket='$isBucket', " +
      s"isCluster='$isCluster']"

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
) derives Encoder:

  if namespace != null then assert(namespace.forall(_ != null))

  def this(
      name: String,
      catalog: String,
      namespace: Array[String],
      description: String,
      className: String,
      isTemporary: Boolean
  ) =
    this(
      name,
      catalog,
      Option(namespace).map(_.toIndexedSeq).orNull,
      description,
      className,
      isTemporary
    )

  def this(
      name: String,
      database: String,
      description: String,
      className: String,
      isTemporary: Boolean
  ) =
    this(
      name,
      null,
      if database != null then Seq(database) else null,
      description,
      className,
      isTemporary
    )

  def database: String =
    if namespace != null && namespace.length == 1 then namespace.head else null

  override def toString: String =
    "Function[" +
      s"name='$name', " +
      Option(database).map(d => s"database='$d', ").getOrElse("") +
      Option(description).map(d => s"description='$d', ").getOrElse("") +
      s"className='$className', " +
      s"isTemporary='$isTemporary']"
