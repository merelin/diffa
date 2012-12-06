package net.lshift.diffa.railyard;

import com.google.common.collect.ImmutableList;
import net.lshift.diffa.railyard.plumbing.JsonProcessingExceptionMapper;
import org.jboss.resteasy.plugins.server.netty.NettyJaxrsServer;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RailYard {

  static Logger log = LoggerFactory.getLogger(RailYard.class);

  public static final int DEFAULT_PORT = 7655;

  public static void main(String[] args) {

    ResteasyDeployment deployment = new ResteasyDeployment();

    deployment.setResourceClasses(
      ImmutableList.of(
        ChangesResource.class.getName()
      )
    );

    deployment.setProviderClasses(
        ImmutableList.of(
            JsonProcessingExceptionMapper.class.getName()
        )
    );

    //deployment.ge

    NettyJaxrsServer server = new NettyJaxrsServer();
    server.setDeployment(deployment);
    server.setPort(DEFAULT_PORT);
    server.start();



    log.info("Started Railyard server on port " + DEFAULT_PORT);
  }

}
