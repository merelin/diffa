package net.lshift.diffa.sql;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import net.lshift.diffa.adapter.scanning.ScanResultEntry;
import net.lshift.diffa.scanning.ScanResultHandler;
import net.lshift.diffa.scanning.Scannable;
import org.joda.time.DateTime;
import org.jooq.*;
import org.jooq.impl.Factory;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.jooq.impl.Factory.*;

public class PartitionAwareDriver extends AbstractDatabaseAware implements Scannable {

  static Logger log = LoggerFactory.getLogger(PartitionAwareDriver.class);

  private PartitionMetadata config;

  private static HashMap<SQLDialect, String> md5FunctionDefinitions = new HashMap<SQLDialect, String>();

  static {
    md5FunctionDefinitions.put(SQLDialect.HSQLDB,
        "create function md5(v varchar(32672)) returns varchar(32) language java deterministic no sql external name 'CLASSPATH:org.apache.commons.codec.digest.DigestUtils.md5Hex'");
    md5FunctionDefinitions.put(SQLDialect.ORACLE,
        "create or replace function md5(input_string varchar2) " +
            "return varchar2 " +
            "is " +
            "begin " +
            "        declare " +
            "                h_string varchar2(255); " +
            "        begin " +
            "                dbms_obfuscation_toolkit.md5(input_string => input_string, checksum_string => h_string); " +
            "                return lower(rawtohex(utl_raw.cast_to_raw(h_string))); " +
            "        end; " +
            "end; ");
  }

  @Inject
  public PartitionAwareDriver(DataSource ds, PartitionMetadata config, SQLDialect dialect) {
    super(ds, dialect);
    this.config = config;

    Connection connection = getConnection();
    Factory db = getFactory(connection);

    String functionDefinition = md5DefinitionForDialect(this.dialect);

    try {

      // TODO Hack - Just try to create the function, if it fails, we just assume that it already exists
      // It is probably a better idea to introspect the information schema to find out for sure that this function is available

      db.execute(functionDefinition);
      log.info("Created md5 function");
    }
    catch (Exception e) {

      if (e.getMessage().contains("already exists")) {
        // Crude way of ignoring
        log.info("Did not create md5 function, since it looks like it already exists in the target database");
      }
      else {
        log.info("Find out whether we need to care about this", e);
      }

    }

  }
  private String md5DefinitionForDialect(SQLDialect dialect) {
    return md5FunctionDefinitions.get(dialect);
  }

  @Override
  public void scan(Set<ScanConstraint> constraints, Set<ScanAggregation> aggregations, int maxSliceSize, ScanResultHandler handler) {

    Connection connection = getConnection();
    Factory db = getFactory(connection);

//    DataType<?> partitionType = underlyingPartition.getDataType();
//
//    if (partitionType.equals(SQLDataType.DATE) || partitionType.equals(SQLDataType.TIMESTAMP)) {
//      Field<Date> typedField = (Field<Date>) underlyingPartition;
//      underlyingPartition = A.getField(typedField);
//    }
//    else {
//      throw new RuntimeException("Currently we can not handle partition type: " + underlyingPartition.getDataType() + " ; please contact your nearest software developer");
//    }

    DateBasedAggregationScanner scanner = new DateBasedAggregationScanner(db, config, maxSliceSize);
    scanner.scan(constraints, handler);

    handler.onCompletion();


    closeConnection(connection);
  }

  // TODO This stuff shouldn't really get invoked inline, we should have some kind of wrapping function ....

}
