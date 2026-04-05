package org.apache.spark.sql.streaming

/** State variable for a single value per key in transformWithState. */
trait ValueState[S]:
  def exists(): Boolean
  def get(): S
  def update(newState: S): Unit
  def clear(): Unit

/** State variable for a list of values per key in transformWithState. */
trait ListState[S]:
  def exists(): Boolean
  def get(): Iterator[S]
  def put(newState: Array[S]): Unit
  def appendValue(newState: S): Unit
  def appendList(newState: Array[S]): Unit
  def clear(): Unit

/** State variable for a map of key-value pairs per key in transformWithState. */
trait MapState[K, V]:
  def exists(): Boolean
  def getValue(key: K): V
  def containsKey(key: K): Boolean
  def updateValue(key: K, value: V): Unit
  def iterator(): Iterator[(K, V)]
  def keys(): Iterator[K]
  def values(): Iterator[V]
  def removeKey(key: K): Unit
  def clear(): Unit
