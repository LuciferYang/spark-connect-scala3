package org.apache.spark.sql

// Top-level case classes for integration tests.
// Inner classes cannot be used with UDFs on a remote Spark server because the
// server's Catalyst analyzer cannot access the outer scope needed to instantiate them.

case class Dept(name: String, dept: String) derives Encoder
case class Record(group: String, value: Int) derives Encoder
case class GroupedScore(group: String, score: Double) derives Encoder
