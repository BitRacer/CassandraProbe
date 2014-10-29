package com.synchronoss;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple CassandraProbe.
 */
public class CassandraProbeTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public CassandraProbeTest(String testName) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite( CassandraProbeTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() {
        assertTrue( true );
    }
}
