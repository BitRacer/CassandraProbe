package com.synchronoss;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

/**
 * Hello world!
 *
 */
public class CassandraProbe {

private enum EXIT_CODE {
  INCORRECT_USAGE
}
    public static void main( String[] args ) throws Exception {
      CassandraProbe probe = new CassandraProbe();
      if(args.length == 0) {
        probe.usage(EXIT_CODE.INCORRECT_USAGE);
      }
      else {
        probe.connect(args);
      }
    }

    private void usage(EXIT_CODE exitCode) throws Exception {
      switch(exitCode) {
        case INCORRECT_USAGE:
          System.out.println("!!! INCORRECT USAGE !!!");
          System.out.println("java -jar probe.jar [server.name]");
        break;
        default:
          System.out.println("unknown error");
      }
      throw new Exception("Exception thrown due to : " + exitCode.name());
    }

  public void connect(String[] args) {
    Cluster cluster = Cluster.builder().addContactPoint(args[0]).build();
    Metadata metadata = cluster.getMetadata();
    System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
    for ( Host host : metadata.getAllHosts() ) {
      System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
    }
    cluster.close();
  }


}
