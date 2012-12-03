package net.lshift.diffa.versioning;

import com.jolbox.bonecp.BoneCPDataSource;
import net.lshift.diffa.sql.PartitionAwareStore;
import net.lshift.diffa.sql.StoreConfiguration;
import net.lshift.diffa.versioning.tables.records.ThingsRecord;
import org.apache.commons.lang.RandomStringUtils;
import org.jooq.impl.*;
import java.sql.Connection;
import java.util.*;

public class PartitionAwareThings extends PartitionAwareStore {

  public PartitionAwareThings(BoneCPDataSource ds, String name, StoreConfiguration config) {
    super(ds, name, config);
  }

  public PartitionableThing createRandomThing(Map<String, ?> attributes) {

    PartitionableThing record = new PartitionableThing(attributes);
    record.setId(RandomStringUtils.randomAlphabetic(10));
    record.setVersion(RandomStringUtils.randomAlphabetic(10));

    Connection connection = getConnection();

    Factory db = getFactory(connection);
    db.executeInsert((ThingsRecord)record);

    closeConnection(connection, true);

    return record;
  }

}
