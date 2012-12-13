package net.lshift.diffa.conductor.plumbing;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import net.lshift.diffa.conductor.DriverConfiguration;
import net.lshift.diffa.sql.PartitionMetadata;
import org.jooq.impl.SQLDataType;

import javax.sql.DataSource;

public class ConfigurationBuilder {

  public static DataSource buildDataSource(DriverConfiguration conf) {

    try {
      Class.forName(conf.getDriverClass());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    BoneCPConfig config = new BoneCPConfig();

    config.setJdbcUrl(conf.getUrl());
    config.setUsername(conf.getUsername());
    config.setPassword(conf.getPassword());

    return new BoneCPDataSource(config);
  }

  public static PartitionMetadata buildMetaData(DriverConfiguration conf) {
    return new PartitionMetadata(conf.getTableName()).
      withId(conf.getIdFieldName(), SQLDataType.VARCHAR). // TODO Make less hard coded
      withVersion(conf.getVersionFieldName(), SQLDataType.VARCHAR).
      partitionBy(conf.getPartitionFieldName(), SQLDataType.DATE);
  }
}
