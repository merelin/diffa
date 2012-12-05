package net.lshift.diffa.versioning;

import com.jolbox.bonecp.BoneCPDataSource;
import net.lshift.diffa.sql.PartitionAwareDriver;
import net.lshift.diffa.sql.StoreConfiguration;
import org.apache.commons.lang.RandomStringUtils;
import org.jooq.impl.*;
import java.sql.Connection;
import java.util.*;

public class PartitionAwareThings extends PartitionAwareDriver {

  public PartitionAwareThings(BoneCPDataSource ds, StoreConfiguration config) {
    super(ds, config);
  }

  public PartitionableThing createRandomThing(Map<String, ?> attributes) {

    String id = RandomStringUtils.randomAlphabetic(10);
    String version = RandomStringUtils.randomAlphabetic(10);

    PartitionableThing record = new PartitionableThing(attributes);

    record.setId(id);
    record.setVersion(version);

    Connection connection = getConnection();

    Factory db = getFactory(connection);
    db.execute("insert into things (id, version,entry_date) values (?,?,?)", id, version, record.getEntryDate());

    closeConnection(connection, true);

    return record;
  }

}