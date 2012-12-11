package net.lshift.diffa.railyard.wiring;

import com.google.inject.AbstractModule;
import net.lshift.diffa.versioning.CassandraVersionStore;
import net.lshift.diffa.versioning.VersionStore;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.MappingJsonFactory;

public class RailYardModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(VersionStore.class).to(CassandraVersionStore.class);
    bind(JsonFactory.class).to(MappingJsonFactory.class);
  }
}
