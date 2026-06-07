package org.apache.spark.sql;

final class JavaConnectorCatalogCompileTest {
  org.apache.spark.sql.connector.catalog.Identifier identifier() {
    org.apache.spark.sql.connector.catalog.Identifier ident =
        org.apache.spark.sql.connector.catalog.Identifier.of(
            new String[] {"catalog", "db"}, "table");
    String[] namespace = ident.namespace();
    String name = ident.name();
    return ident;
  }

  org.apache.spark.sql.connector.catalog.IdentityColumnSpec identityColumnSpec() {
    org.apache.spark.sql.connector.catalog.IdentityColumnSpec spec =
        new org.apache.spark.sql.connector.catalog.IdentityColumnSpec(1L, 2L, true);
    long start = spec.getStart();
    long step = spec.getStep();
    boolean explicit = spec.isAllowExplicitInsert();
    return spec;
  }
}
