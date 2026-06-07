package org.apache.spark.sql;

import java.util.Collections;

import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.SQLUserDefinedType;
import org.apache.spark.sql.types.UserDefinedType;

final class JavaStatefulGroupedDatasetCompileTest {
  Dataset<Long> mapGroupsWithState(KeyValueGroupedDataset<String, Long> grouped) {
    return grouped.mapGroupsWithState(
        (String key, java.util.Iterator<Long> values, GroupState<Long> state) ->
            values.hasNext() ? values.next() : 0L,
        Encoders.LONG(),
        Encoders.LONG());
  }

  Dataset<Long> mapGroupsWithStateWithTimeout(KeyValueGroupedDataset<String, Long> grouped) {
    return grouped.mapGroupsWithState(
        (String key, java.util.Iterator<Long> values, GroupState<Long> state) ->
            values.hasNext() ? values.next() : 0L,
        Encoders.LONG(),
        Encoders.LONG(),
        GroupStateTimeout.NoTimeout());
  }

  Dataset<Long> mapGroupsWithStateWithInitialState(
      KeyValueGroupedDataset<String, Long> grouped,
      KeyValueGroupedDataset<String, Long> initialState) {
    return grouped.mapGroupsWithState(
        (String key, java.util.Iterator<Long> values, GroupState<Long> state) ->
            values.hasNext() ? values.next() : 0L,
        Encoders.LONG(),
        Encoders.LONG(),
        GroupStateTimeout.ProcessingTimeTimeout(),
        initialState);
  }

  Dataset<Long> flatMapGroupsWithState(KeyValueGroupedDataset<String, Long> grouped) {
    return grouped.flatMapGroupsWithState(
        (String key, java.util.Iterator<Long> values, GroupState<Long> state) ->
            Collections.singletonList(values.hasNext() ? values.next() : 0L).iterator(),
        OutputMode.Update(),
        Encoders.LONG(),
        Encoders.LONG(),
        GroupStateTimeout.EventTimeTimeout());
  }

  Dataset<Long> flatMapGroupsWithStateWithInitialState(
      KeyValueGroupedDataset<String, Long> grouped,
      KeyValueGroupedDataset<String, Long> initialState) {
    return grouped.flatMapGroupsWithState(
        (String key, java.util.Iterator<Long> values, GroupState<Long> state) ->
            Collections.singletonList(values.hasNext() ? values.next() : 0L).iterator(),
        OutputMode.Append(),
        Encoders.LONG(),
        Encoders.LONG(),
        GroupStateTimeout.NoTimeout(),
        initialState);
  }

  @SQLUserDefinedType(udt = IntBoxUDT.class)
  static final class IntBox {
    final Integer value;

    IntBox(Integer value) {
      this.value = value;
    }
  }

  static final class IntBoxUDT extends UserDefinedType<IntBox> {
    @Override
    public DataType sqlType() {
      return DataTypes.IntegerType;
    }

    @Override
    public Object serialize(IntBox obj) {
      return obj.value;
    }

    @Override
    public IntBox deserialize(Object datum) {
      return new IntBox((Integer) datum);
    }

    @Override
    public Class<IntBox> userClass() {
      return IntBox.class;
    }

    @Override
    public String typeName() {
      return "int_box";
    }
  }
}
