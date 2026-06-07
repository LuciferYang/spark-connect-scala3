package org.apache.spark.sql;

import java.util.Collections;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.ReduceFunction;

final class JavaTypedDatasetCompileTest {
  Dataset<Long> datasetOps(Dataset<Long> ds) {
    MapFunction<Long, Long> map = value -> value + 1L;
    FlatMapFunction<Long, Long> flatMap = value -> Collections.singletonList(value).iterator();
    MapPartitionsFunction<Long, Long> mapPartitions = values -> values;
    FilterFunction<Long> filter = value -> value > 1L;

    Dataset<Long> mapped = ds.map(map, Encoders.LONG());
    Dataset<Long> flatMapped =
        mapped.flatMap(flatMap, Encoders.LONG());
    Dataset<Long> partitionMapped = flatMapped.mapPartitions(mapPartitions, Encoders.LONG());
    return partitionMapped.filter(filter);
  }

  KeyValueGroupedDataset<String, Long> groupByKey(Dataset<Long> ds) {
    MapFunction<Long, String> keyFunc = value -> value % 2L == 0L ? "even" : "odd";
    return ds.groupByKey(keyFunc, Encoders.STRING());
  }

  void foreachOps(Dataset<Long> ds) {
    ForeachFunction<Long> foreach = value -> {};
    ForeachPartitionFunction<Long> foreachPartition = values -> {};
    ds.foreach(foreach);
    ds.foreachPartition(foreachPartition);
  }

  Long reduce(Dataset<Long> ds) {
    ReduceFunction<Long> reduce = (left, right) -> left + right;
    return ds.reduce(reduce);
  }

  Dataset<Long> groupedOps(KeyValueGroupedDataset<String, Long> grouped) {
    MapFunction<Long, Long> mapValue = value -> value + 1L;
    MapGroupsFunction<String, Long, Long> mapGroup =
        (key, values) -> values.hasNext() ? values.next() : 0L;
    FlatMapGroupsFunction<String, Long, Long> flatMapGroup =
        (key, values) -> Collections.singletonList(values.hasNext() ? values.next() : 0L)
            .iterator();
    ReduceFunction<Long> reduce = (left, right) -> left + right;

    KeyValueGroupedDataset<String, Long> mappedValues =
        grouped.mapValues(mapValue, Encoders.LONG());
    Dataset<Long> mappedGroups =
        mappedValues.mapGroups(mapGroup, Encoders.LONG());
    Dataset<Long> flatMappedGroups =
        mappedValues.flatMapGroups(flatMapGroup, Encoders.LONG());
    Dataset<Long> sortedGroups =
        mappedValues.flatMapSortedGroups(
            new Column[] {functions.col("value")},
            flatMapGroup,
            Encoders.LONG());
    Dataset<scala.Tuple2<String, Long>> reduced =
        mappedValues.reduceGroups(reduce);
    return mappedGroups
        .union(flatMappedGroups)
        .union(sortedGroups)
        .union(reduced.map((MapFunction<scala.Tuple2<String, Long>, Long>) t -> t._2, Encoders.LONG()));
  }
}
