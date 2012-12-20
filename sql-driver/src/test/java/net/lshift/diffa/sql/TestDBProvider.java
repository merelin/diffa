package net.lshift.diffa.sql;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import org.apache.commons.lang3.RandomStringUtils;
import org.jooq.SQLDialect;
import org.jooq.impl.Factory;

import javax.sql.DataSource;
import java.sql.SQLException;

public class TestDBProvider {
  public static DataSource getDataSource() {
    TestSchema schema = getSchema(
        getDialect(),
        System.getProperty("jdbcUsername"),
        System.getProperty("jdbcPass"),
        System.getProperty("jdbcUrl"));
    try {
      schema.create();
      schema.migrate();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return schema.getDataSource();
  }

  private static TestSchema getSchema(SQLDialect dialect, String username, String password, String jdbcUrl) {
    switch (dialect) {
      case ORACLE: return new OracleTestSchema(username, password, jdbcUrl);
      default: return new HsqldbTestSchema();
    }
  }

  public static SQLDialect getDialect() {
    return SQLDialect.valueOf(System.getProperty("jooqDialect", "HSQLDB"));
  }
}
