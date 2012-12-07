package net.lshift.diffa.railyard;

import com.google.common.collect.ImmutableList;
import net.lshift.diffa.railyard.plumbing.JsonProcessingExceptionMapper;
import org.jboss.resteasy.plugins.server.netty.NettyJaxrsServer;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class RailYard implements Runnable {

  static Logger log = LoggerFactory.getLogger(RailYard.class);

  public static final int DEFAULT_PORT = 7655;

  @Override
  public void run() {
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


    NettyJaxrsServer server = new NettyJaxrsServer();
    server.setDeployment(deployment);
    server.setPort(DEFAULT_PORT);
    server.start();

    log.info("Started Railyard server on port " + DEFAULT_PORT);
  }


  public static void main(String[] args) throws Exception{

    RailYard railYard = new RailYard();
    Thread railYardThread = new Thread(railYard);
    railYardThread.setDaemon(true);
    railYardThread.start();

  }

}
