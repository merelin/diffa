package net.lshift.diffa.railyard.wiring;

import com.google.inject.AbstractModule;
import net.lshift.diffa.system.SystemConfiguration;
import net.lshift.diffa.system.SystemConfigurationClient;
import net.lshift.diffa.versioning.CassandraVersionStore;
import net.lshift.diffa.versioning.VersionStore;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.MappingJsonFactory;

public class RailYardModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(JsonFactory.class).to(MappingJsonFactory.class);
    bind(VersionStore.class).to(CassandraVersionStore.class);
    bind(SystemConfiguration.class).to(SystemConfigurationClient.class);
  }
}
