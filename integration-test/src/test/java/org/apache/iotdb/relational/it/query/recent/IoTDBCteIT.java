/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBCteIT {
  private static final String DATABASE_NAME = "testdb";

  private static final String[] creationSqls =
      new String[] {
        "CREATE DATABASE IF NOT EXISTS testdb",
        "USE testdb",
        "CREATE TABLE IF NOT EXISTS testtb(deviceid STRING TAG, voltage FLOAT FIELD)",
        "INSERT INTO testtb VALUES(1000, 'd1', 100.0)",
        "INSERT INTO testtb VALUES(2000, 'd1', 200.0)",
        "INSERT INTO testtb VALUES(1000, 'd2', 300.0)",
      };

  private static final String dropDbSqls = "DROP DATABASE IF EXISTS testdb";

  @BeforeClass
  public static void setUpClass() {
    Locale.setDefault(Locale.ENGLISH);

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setPartitionInterval(1000)
        .setMemtableSizeThreshold(10000);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Before
  public void setUp() throws SQLException {
    prepareData();
  }

  @After
  public void tearDown() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute(dropDbSqls);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testQuery() {
    String[] expectedHeader = new String[] {"time", "deviceid", "voltage"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:01.000Z,d1,100.0,",
          "1970-01-01T00:00:02.000Z,d1,200.0,",
          "1970-01-01T00:00:01.000Z,d2,300.0,"
        };
    tableResultSetEqualTest(
        "with cte as (select * from testtb) select * from cte order by deviceid",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"deviceid", "voltage"};
    retArray = new String[] {"d1,100.0,", "d1,200.0,", "d2,300.0,"};
    tableResultSetEqualTest(
        "with cte as (select deviceid, voltage from testtb) select * from cte order by deviceid",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"deviceid", "avg_voltage"};
    retArray = new String[] {"d1,150.0,", "d2,300.0,"};
    tableResultSetEqualTest(
        "with cte as (select deviceid, avg(voltage) as avg_voltage from testtb group by deviceid) select * from cte order by deviceid",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testPartialColumn() {
    String[] expectedHeader = new String[] {"id", "v"};
    String[] retArray = new String[] {"d1,100.0,", "d1,200.0,", "d2,300.0,"};
    tableResultSetEqualTest(
        "with cte(id, v) as (select deviceid, voltage from testtb) select * from cte order by id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableAssertTestFail(
        "with cte(v) as (select deviceid, voltage from testtb) select * from cte order by id",
        "701: Column alias list has 1 entries but relation has 2 columns",
        DATABASE_NAME);
  }

  @Test
  public void testExplain() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE testdb");
      // explain
      ResultSet resultSet =
          statement.executeQuery(
              "explain with cte as (select * from testtb) select * from cte order by deviceid");
      ResultSetMetaData metaData = resultSet.getMetaData();
      assertEquals(metaData.getColumnCount(), 1);
      assertEquals(metaData.getColumnName(1), "distribution plan");

      // explain analyze
      resultSet =
          statement.executeQuery(
              "explain analyze with cte as (select * from testtb) select * from cte order by deviceid");
      metaData = resultSet.getMetaData();
      assertEquals(metaData.getColumnCount(), 1);
      assertEquals(metaData.getColumnName(1), "Explain Analyze");
    }
  }

  @Test
  public void testMultiReference() {
    String[] expectedHeader = new String[] {"time", "deviceid", "voltage"};
    String[] retArray = new String[] {"1970-01-01T00:00:01.000Z,d2,300.0,"};
    tableResultSetEqualTest(
        "with cte as (select * from testtb) select * from cte where voltage > (select avg(voltage) from cte)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testCteResultNull() throws SQLException {
    ResultSet resultSet = null;
    String sql =
        "explain analyze with cte1 as materialized (select * from testtb1) select * from testtb,cte1 where testtb.deviceid=cte1.deviceid";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("Use testdb");
      statement.execute(
          "CREATE TABLE IF NOT EXISTS testtb1(deviceid STRING TAG, voltage FLOAT FIELD)");
      resultSet = statement.executeQuery(sql);
      StringBuilder sb = new StringBuilder();
      while (resultSet.next()) {
        sb.append(resultSet.getString(1));
      }
      Assert.assertTrue(sb.toString().contains("CTE Query : 'cte1'Main Query"));
      statement.execute("DROP TABLE testtb1");
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
    }
  }

  @Test
  public void testGetCteResultSuccess() throws SQLException {
    ResultSet resultSet = null;
    String sql =
        "explain analyze with cte1 as materialized (select * from testtb3) select * from testtb where testtb.deviceid in (select deviceid from cte1)";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("Use testdb");
      statement.execute(
          "CREATE TABLE IF NOT EXISTS testtb3(deviceid STRING TAG, voltage FLOAT FIELD)");
      for (int i = 0; i < 101; i++) {
        statement.addBatch("insert into testtb3(deviceid,voltage) values('d" + i + "', " + i + ")");
      }
      statement.executeBatch();
      resultSet = statement.executeQuery(sql);
      StringBuilder sb = new StringBuilder();
      while (resultSet.next()) {
        sb.append(resultSet.getString(1));
      }

      Assert.assertTrue(
          "When the CTE execution success, the main query does not use cte.",
          sb.toString().contains("CteScanNode(CteScanOperator)"));

      List<Integer> counts = new ArrayList<>();
      Pattern pattern = Pattern.compile("Fragment Instances Count:\\s*(\\d+)");
      Matcher matcher = pattern.matcher(sb.toString());
      while (matcher.find()) {
        int count = Integer.parseInt(matcher.group(1));
        counts.add(count);
      }
      Assert.assertEquals(
          "The result output is incorrect after CTE execution success.",
          extractFragmentCount(
              sb.toString(), "CTE Query", "Main Query", "Fragment Instances Count:\\s*(\\d+)"),
          countFragmentInstances(sb.toString(), "CTE Query", "Main Query", "FRAGMENT-INSTANCE\\["));
      Assert.assertEquals(
          "The result output is incorrect after CTE execution fails.",
          extractFragmentCount(
              sb.toString(), "Main Query", null, "Fragment Instances Count:\\s*(\\d+)"),
          countFragmentInstances(sb.toString(), "Main Query", null, "FRAGMENT-INSTANCE\\["));

      statement.execute("DROP TABLE testtb3");
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
    }
  }

  @Test
  public void testGetCteResultFailed() throws SQLException {
    ResultSet resultSet = null;
    String sql =
        "explain analyze with cte1 as materialized (select * from testtb2) select * from testtb where testtb.deviceid in (select deviceid from cte1)";
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = conn.createStatement()) {
      statement.execute("Use testdb");
      statement.execute(
          "CREATE TABLE IF NOT EXISTS testtb2(deviceid STRING TAG, voltage FLOAT FIELD)");
      for (int i = 0; i < 10001; i++) {
        statement.addBatch("insert into testtb2(deviceid,voltage) values('d" + i + "', " + i + ")");
      }
      statement.executeBatch();
      resultSet = statement.executeQuery(sql);
      StringBuilder sb = new StringBuilder();
      while (resultSet.next()) {
        System.out.println(resultSet.getString(1));
        sb.append(resultSet.getString(1));
      }

      Assert.assertFalse(
          "When the CTE execution fails, the main query does not degrade to an inline query.",
          sb.toString().contains("CteScanNode(CteScanOperator)"));
      Assert.assertFalse(
          "When the CTE execution fails, the main query does not degrade to an inline query.",
          sb.toString().contains("ExplainAnalyzeNode"));

      List<Integer> counts = new ArrayList<>();
      Pattern pattern = Pattern.compile("Fragment Instances Count:\\s*(\\d+)");
      Matcher matcher = pattern.matcher(sb.toString());
      while (matcher.find()) {
        int count = Integer.parseInt(matcher.group(1));
        counts.add(count);
      }
      Assert.assertEquals(
          "The result output is incorrect after CTE execution fails.",
          extractFragmentCount(
              sb.toString(), "CTE Query", "Main Query", "Fragment Instances Count:\\s*(\\d+)"),
          countFragmentInstances(sb.toString(), "CTE Query", "Main Query", "FRAGMENT-INSTANCE\\["));
      Assert.assertEquals(
          "The result output is incorrect after CTE execution fails.",
          extractFragmentCount(
              sb.toString(), "Main Query", null, "Fragment Instances Count:\\s*(\\d+)"),
          countFragmentInstances(sb.toString(), "Main Query", null, "FRAGMENT-INSTANCE\\["));

      statement.execute("DROP TABLE testtb2");
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
    }
  }

  private static int countFragmentInstances(
      String result, String startMarker, String endMarker, String regex) {
    int startIndex = result.indexOf(startMarker);
    if (startIndex == -1) {
      return 0;
    }
    int endIndex = (endMarker != null) ? result.indexOf(endMarker, startIndex) : result.length();
    if (endIndex == -1) {
      return 0;
    }
    String section = result.substring(startIndex, endIndex);

    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(section);
    int count = 0;
    while (matcher.find()) {
      count++;
    }
    return count;
  }

  private static int extractFragmentCount(
      String result, String startMarker, String endMarker, String regex) {
    int startIndex = result.indexOf(startMarker);
    if (startIndex == -1) {
      return 0;
    }
    int endIndex = (endMarker != null) ? result.indexOf(endMarker, startIndex) : result.length();
    if (endIndex == -1) {
      return 0;
    }
    String section = result.substring(startIndex, endIndex);
    Pattern r = Pattern.compile(regex);
    Matcher m = r.matcher(section);

    if (m.find()) {
      return Integer.parseInt(m.group(1));
    } else {
      return 0;
    }
  }

  @Test
  public void testDomain() {
    String[] expectedHeader = new String[] {"deviceid", "voltage"};
    String[] retArray = new String[] {"d1,100.0,", "d1,200.0,", "d2,300.0,"};
    tableResultSetEqualTest(
        "with testtb as (select deviceid, voltage from testtb) select * from testtb order by deviceid",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableAssertTestFail(
        "with testtb as (select voltage from testtb) select * from testtb order by deviceid",
        "616: Column 'deviceid' cannot be resolved",
        DATABASE_NAME);
  }

  @Test
  public void testSession() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use testdb");
      SessionDataSet dataSet =
          session.executeQueryStatement("with cte as (select * from testtb) select * from cte");

      assertEquals(dataSet.getColumnNames().size(), 3);
      assertEquals(dataSet.getColumnNames().get(0), "time");
      assertEquals(dataSet.getColumnNames().get(1), "deviceid");
      assertEquals(dataSet.getColumnNames().get(2), "voltage");
      int cnt = 0;
      while (dataSet.hasNext()) {
        dataSet.next();
        cnt++;
      }
      Assert.assertEquals(3, cnt);
    }
  }

  @Test
  public void testJdbc() throws ClassNotFoundException, SQLException {
    BaseEnv env = EnvFactory.getEnv();
    String uri = String.format("jdbc:iotdb://%s:%s?sql_dialect=table", env.getIP(), env.getPort());
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    try (Connection connection =
            DriverManager.getConnection(
                uri, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
        Statement statement = connection.createStatement()) {
      statement.executeUpdate("use testdb");
      ResultSet resultSet =
          statement.executeQuery("with cte as (select * from testtb) select * from cte");

      final ResultSetMetaData metaData = resultSet.getMetaData();
      assertEquals(metaData.getColumnCount(), 3);
      assertEquals(metaData.getColumnLabel(1), "time");
      assertEquals(metaData.getColumnLabel(2), "deviceid");
      assertEquals(metaData.getColumnLabel(3), "voltage");

      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(3, cnt);
    }
  }

  @Test
  public void testNest() {
    String sql1 =
        "WITH"
            + " cte1 AS (select deviceid, voltage from testtb where voltage > 200),"
            + " cte2 AS (SELECT voltage FROM cte1)"
            + " SELECT * FROM cte2";

    String sql2 =
        "WITH"
            + " cte2 AS (SELECT voltage FROM cte1),"
            + " cte1 AS (select deviceid, voltage from testtb where voltage > 200)"
            + " SELECT * FROM cte2";

    String[] expectedHeader = new String[] {"voltage"};
    String[] retArray = new String[] {"300.0,"};
    tableResultSetEqualTest(sql1, expectedHeader, retArray, DATABASE_NAME);

    tableAssertTestFail(sql2, "550: Table 'testdb.cte1' does not exist.", DATABASE_NAME);
  }

  @Test
  public void testRecursive() {
    String sql =
        "WITH RECURSIVE t(n) AS ("
            + " VALUES (1)"
            + " UNION ALL"
            + " SELECT n+1 FROM t WHERE n < 100)"
            + " SELECT sum(n) FROM t";

    tableAssertTestFail(sql, "701: recursive cte is not supported yet", DATABASE_NAME);
  }

  @Test
  public void testPrivileges() throws SQLException {
    Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    Statement adminStmt = adminCon.createStatement();
    try {
      adminStmt.execute("CREATE USER tmpuser 'tmppw123456789'");
      adminStmt.execute("USE testdb");
      adminStmt.execute(
          "CREATE TABLE IF NOT EXISTS testtb1(deviceid STRING TAG, voltage FLOAT FIELD)");
      adminStmt.execute("GRANT SELECT ON testdb.testtb TO USER tmpuser");

      try (Connection connection =
              EnvFactory.getEnv()
                  .getConnection("tmpuser", "tmppw123456789", BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("USE testdb");
        statement.execute("with cte as (select * from testtb) select * from cte");
      }

      try (Connection connection =
              EnvFactory.getEnv()
                  .getConnection("tmpuser", "tmppw123456789", BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("USE testdb");
        statement.execute("with cte as (select * from testtb1) select * from testtb");
        fail("No exception!");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "803: Access Denied: No permissions for this operation, please add privilege SELECT ON testdb.testtb1"));
      }
    } finally {
      adminStmt.execute("DROP USER tmpuser");
      adminStmt.execute("DROP TABLE IF EXISTS testtb1");
    }
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private static void prepareData(String[] sqls) {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
