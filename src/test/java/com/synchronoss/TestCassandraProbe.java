package com.synchronoss;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.junit.*;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


/**
 * Unit test for simple CassandraProbe.
 */

public class TestCassandraProbe {

  private static final String host = "127.0.0.1";
  private static final int port = 9042;
  private static final String user = "cassandra";
  private static final String pass = "cassandra";
  private static final String clusterName = "Test Cluster";
  private static final String keyspaceName = "test_keyspace";
  CassandraProbe probe = new CassandraProbe();


  @Rule public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("test.cql", keyspaceName), "cassandra.yaml", host, port);

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
  }

  @Test
  public void testDBCreation() {
    ResultSet result = cassandraCQLUnit.session.execute("select * from test_keyspace.test_table WHERE id='key1'");
    assertThat(result.iterator().next().getString("name"), is("Aristotle"));
  }



  @Test
  public void testConnectWithHostOnly() {
    Session session = null;
    try {
      Cluster testCluster = probe.connect(new String[] {host} );
      session = testCluster.connect(keyspaceName);
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
    ResultSet result = session.execute("select * from test_table WHERE id='key1'");
    assertThat(result.iterator().next().getString("name"), is("Aristotle"));
    session.close();
  }


  @Test
  public void testConnectWithHostAndPort() {
    Session session = null;
    try {
      Cluster cluster = probe.connect(new String[] {host, "" + port} );
      session = cluster.connect(keyspaceName);
    }
    catch(Exception ex) {
      ex.printStackTrace();
      assertThat(ex, is(not(nullValue())));
    }
    ResultSet result = session.execute("select * from test_table WHERE id='key1'");
    assertThat(result.iterator().next().getString("name"), is("Aristotle"));
    session.close();
  }

  @Test
  public void testConnectWithHostAndCredentials() {
    Session session = null;
    try {
      Cluster cluster = probe.connect(new String[] {host, user, pass} );
      session = cluster.connect(keyspaceName);
    }
    catch(Exception ex) {
      ex.printStackTrace();
      assertThat(ex, is(not(nullValue())));
    }
    ResultSet result = session.execute("select * from test_table WHERE id='key1'");
    assertThat(result.iterator().next().getString("name"), is("Aristotle"));
    session.close();
  }

  @Test
  public void testConnectWithHostPortAndCredentials() {
    Session session = null;
    try {
      Cluster cluster = probe.connect(new String[] {host, "" + port, user, pass} );
      session = cluster.connect(keyspaceName);
    }
    catch(Exception ex) {
      ex.printStackTrace();
      assertThat(ex, is(not(nullValue())));
    }
    ResultSet result = session.execute("select * from test_table WHERE id='key1'");
    assertThat(result.iterator().next().getString("name"), is("Aristotle"));
    session.close();
  }

  @Test
  public void testConnectWithHostPortCredentialsAndCluster() {
    Session session = null;
    try {
      Cluster cluster = probe.connect(new String[] {host, "" + port, user, pass, clusterName} );
      session = cluster.connect(keyspaceName);
    }
    catch(Exception ex) {
      ex.printStackTrace();
      assertThat(ex, is(not(nullValue())));
    }
    ResultSet result = session.execute("select * from test_table WHERE id='key1'");
    assertThat(result.iterator().next().getString("name"), is("Aristotle"));
    session.close();
  }

}
