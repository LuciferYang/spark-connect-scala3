package org.apache.spark.sql;

import org.apache.spark.sql.api.java.UDF10;
import org.apache.spark.sql.api.java.UDF22;
import org.apache.spark.sql.types.DataTypes;

final class JavaUdfArityCompileTest {
  private final UDF10<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> udf10 =
      (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10) ->
          t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10;

  private final UDF22<
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer,
          Integer>
      udf22 =
          (t1,
              t2,
              t3,
              t4,
              t5,
              t6,
              t7,
              t8,
              t9,
              t10,
              t11,
              t12,
              t13,
              t14,
              t15,
              t16,
              t17,
              t18,
              t19,
              t20,
              t21,
              t22) ->
              t1
                  + t2
                  + t3
                  + t4
                  + t5
                  + t6
                  + t7
                  + t8
                  + t9
                  + t10
                  + t11
                  + t12
                  + t13
                  + t14
                  + t15
                  + t16
                  + t17
                  + t18
                  + t19
                  + t20
                  + t21
                  + t22;

  UserDefinedFunction createHighArityUdf() {
    return functions.udf(udf22, DataTypes.IntegerType);
  }

  void registerHighArityUdf(UDFRegistration registration) {
    registration.register("sum22", udf22, DataTypes.IntegerType);
  }
}
