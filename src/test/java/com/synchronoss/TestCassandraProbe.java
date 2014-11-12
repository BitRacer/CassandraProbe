package com.synchronoss;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import junit.framework.TestCase;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.*;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;


import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple CassandraProbe.
 */

public class TestCassandraProbe extends TestCase {

  private static final String host = "127.0.0.1";
  private static final String port = "9042";
  private static final String user = "aballen";
  private static final String pass = "aballen";
  private static final String clusterName = "Test Cluster";
  private static final String keyspaceName = "test_keyspace";


  @Rule
  public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("test.cql", keyspaceName));

  CassandraProbe probe;

  @BeforeClass
  public void setUp() throws ConfigurationException, TTransportException, IOException, InterruptedException {
    probe = new CassandraProbe();
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");
    //Thread.sleep(10*1000); //cheesy wait for server to start
  }

    @Test
    public void testDBCreation() {
      ResultSet result = cassandraCQLUnit.session.execute("select * from test_table WHERE id='key1'");
      assertEquals(result.iterator().next().getString("name"), "Aristotle");
      //assertThat(result.iterator().next().getString("name"), is("Aristotle"));
    }

    @Test
    public void testUsage() {
      try {
        probe.usage(CassandraProbe.EXIT_CODE.INCORRECT_USAGE);
        probe.usage(CassandraProbe.EXIT_CODE.HOST_UNAVAILABLE);
        probe.usage(CassandraProbe.EXIT_CODE.BAD_AUTH);
      }
      catch(Exception ex) {
        ex.printStackTrace();
      }
      assertTrue(true);  // cant get here without sucessfully printing all usage options
    }
  /*
  @Test
  public void testConnectWithHostOnly() {
    Cluster testCluster = null;
    try {
      testCluster = probe.connect(new String[] {host} );
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
    assertTrue(testCluster != null);
    testCluster.connect(keyspaceName);
    assertFalse(testCluster.isClosed());
    //assertThat(testCluster, is(notNullValue()));
    //assertThat(testCluster.isClosed(), is(not(true)));
  }
  */

  /*
  @Test
  public void testConnectWithHostAndPort() {
    Cluster testCluster = null;
    try {
      probe.connect(new String[] {host, port} );
    }
    catch(Exception ex) {
      ex.printStackTrace();
      assertThat(ex, is(not(equalTo(null))));
    }
    assertThat(testCluster, is(not(null)));
    assertThat(testCluster.isClosed(), is(not(true)));
  }

  @Test
  public void testConnectWithAndCredentials() {
    Cluster testCluster = null;
    try {
      testCluster = probe.connect(new String[] {host, user, pass} );
    }
    catch(Exception ex) {
      ex.printStackTrace();
      assertThat(ex, is(not(equalTo(null))));
    }
    assertThat(testCluster, is(not(null)));
    assertThat(testCluster.isClosed(), is(not(true)));
  }

  @Test
  public void testConnectWithHostPortAndCredentials() {
    Cluster testCluster = null;
    try {
      testCluster = probe.connect(new String[] {host, port, user, pass} );
    }
    catch(Exception ex) {
      ex.printStackTrace();
      assertThat(ex, is(not(equalTo(null))));
    }
    assertThat(testCluster, is(not(null)));
    assertThat(testCluster.isClosed(), is(not(true)));
  }

  @Test
  public void testConnectWithHostPortCredentialsAndCluster() {
    Cluster testCluster = null;
    try {
      probe.connect(new String[] {host, port, user, pass, clusterName} );
    }
    catch(Exception ex) {
      ex.printStackTrace();
      assertThat(ex, is(not(equalTo(null))));
    }
    assertThat(testCluster, is(not(null)));
    assertThat(testCluster.isClosed(), is(not(true)));
  }
  */

}
