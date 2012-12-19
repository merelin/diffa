package net.lshift.diffa.sql;

import com.googlecode.flyway.core.Flyway;
import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import org.apache.commons.lang3.RandomStringUtils;

import javax.sql.DataSource;

public class TestDBProvider {

  public static DataSource getHSQLDBDataSource() {

    BoneCPConfig config = new BoneCPConfig();
    config.setJdbcUrl("jdbc:hsqldb:mem:" + RandomStringUtils.randomAlphabetic(5));
    config.setUsername("sa");
    config.setPassword("");

    BoneCPDataSource ds = new BoneCPDataSource(config);

    Flyway flyway = new Flyway();
    flyway.setDataSource(ds);
    flyway.setLocations("hsqldb");
    flyway.migrate();
    return ds;
  }
}
