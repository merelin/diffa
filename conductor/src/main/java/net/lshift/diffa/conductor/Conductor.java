package net.lshift.diffa.conductor;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import joptsimple.OptionSet;
import net.lshift.diffa.railyard.RailYard;
import net.lshift.diffa.railyard.RailYardClient;

import java.util.ArrayList;
import java.util.List;

public class Conductor extends SimpleDaemon {

  public static final int DEFAULT_PORT = 5150;

  public Conductor(String[] commandLineArgs) {
    super(commandLineArgs);
  }

  public static void main(String[] args) throws Exception {
    new Conductor(args);
 }

  @Override
  protected List<Object> getResources() {
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(RailYard.class).to(RailYardClient.class);
        bind(String.class).annotatedWith(Names.named("railYardUrl")).toInstance("http://localhost:7655");
      }
    });

    List<Object> resources = new ArrayList<Object>();
    resources.add(injector.getInstance(InterviewResource.class));
    return resources;
  }

  @Override
  protected String getName(OptionSet options) {
    return "Conductor";
  }

  @Override
  protected int getPort(OptionSet options) {
    return DEFAULT_PORT;
  }
}
