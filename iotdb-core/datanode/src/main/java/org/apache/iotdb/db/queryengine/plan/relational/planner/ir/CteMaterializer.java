/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceFetchException;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.ExplainType;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.SqlFormatter;
import org.apache.iotdb.db.queryengine.statistics.FragmentInstanceStatisticsDrawer;
import org.apache.iotdb.db.queryengine.statistics.QueryStatisticsFetcher;
import org.apache.iotdb.db.queryengine.statistics.StatisticLine;
import org.apache.iotdb.db.utils.cte.CteDataStore;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.TypeFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CteMaterializer {

  private static final Coordinator coordinator = Coordinator.getInstance();

  private CteMaterializer() {}

  public static void materializeCTE(Analysis analysis, MPPQueryContext context) {
    analysis
        .getNamedQueries()
        .forEach(
            (tableRef, query) -> {
              Table table = tableRef.getNode();
              if (query.isMaterialized()) {
                CteDataStore expalinDataStore = query.getExplainCteDataStore();
                if(isExplainQuery(context)){
                  expalinDataStore = fetchExplainCteQueryResult(table, query, context);
                  if (expalinDataStore != null) {
                    context.addExplainCteDataStore(table, expalinDataStore);
                  }
                }
                CteDataStore dataStore = query.getCteDataStore();
                if (dataStore == null) {
                  dataStore = fetchCteQueryResult(table, query, context);
                  if (dataStore == null) {
                    // CTE query execution failed. Use inline instead of materialization in the
                    // outer query
                    query.setMaterialized(false);
                    return;
                  } else {
                    context.reserveMemoryForFrontEnd(dataStore.getCachedBytes());
                    query.setCteDataStore(dataStore);
                  }
                }
                context.addCteDataStore(table, dataStore);

              }
            });
  }

  public static void cleanUpCTE(MPPQueryContext context) {
    Map<NodeRef<Table>, CteDataStore> cteDataStores = context.getCteDataStores();
    cteDataStores
        .values()
        .forEach(
            dataStore -> {
              context.releaseMemoryReservedForFrontEnd(dataStore.getCachedBytes());
              dataStore.clear();
            });
    cteDataStores.clear();

    Map<NodeRef<Table>, CteDataStore> explainCteDataStores = context.getExplainCteDataStores();
    if(explainCteDataStores != null){
      explainCteDataStores
        .values()
        .forEach(
          dataStore -> {
            context.releaseMemoryReservedForFrontEnd(dataStore.getCachedBytes());
            dataStore.clear();
          });
      explainCteDataStores.clear();
    }
  }

  public static CteDataStore fetchCteQueryResult(
      Table table, Query query, MPPQueryContext context) {
    final long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;
    try {
      final ExecutionResult executionResult =
          coordinator.executeForTableModel(
              query,
              new SqlParser(),
              SessionManager.getInstance().getCurrSession(),
              queryId,
              SessionManager.getInstance()
                  .getSessionInfoOfTableModel(SessionManager.getInstance().getCurrSession()),
              "Materialize common table expression",
              LocalExecutionPlanner.getInstance().metadata,
              context.getTimeOut(),
              false);
      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return null;
      }

      // get table schema
      DatasetHeader datasetHeader = coordinator.getQueryExecution(queryId).getDatasetHeader();
      TableSchema tableSchema = getTableSchema(datasetHeader, table.getName().toString());

      CteDataStore cteDataStore = new CteDataStore(query, tableSchema);
      while (coordinator.getQueryExecution(queryId).hasNextResult()) {
        final Optional<TsBlock> tsBlock;
        try {
          tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
        } catch (final IoTDBException e) {
          t = e;
          throw new IoTDBRuntimeException(
              String.format("Fail to materialize CTE because %s", e.getMessage()),
              e.getErrorCode(),
              e.isUserException());
        }
        if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
          continue;
        }
        if (!cteDataStore.addTsBlock(tsBlock.get())) {
          return null;
        }
      }
      if(isExplainAnalyzeQuery(context)){
        handleExpainAnalyzeResult(queryId,context,table);
      }
      return cteDataStore;
    } catch (final Throwable throwable) {
      t = throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
    return null;
  }

  private static void handleExpainAnalyzeResult(long queryId, MPPQueryContext context,Table table) {
    QueryExecution queryExecution = (QueryExecution) coordinator.getQueryExecution(queryId);
    MPPQueryContext cteContext = queryExecution.getContext();
    DistributedQueryPlan distributedPlan = queryExecution.getDistributedPlan();
    try {
      List<String> results = buildFragmentInstanceStatistics(distributedPlan.getInstances(), true,cteContext);
      results.add(0,"Cte " + table.getName() +" explain analyze :");
      context.setExplainAnalyzeCteResult(results);
    } catch (FragmentInstanceFetchException e) {
      throw new RuntimeException(e);
    }
  }
  private static List<String> buildFragmentInstanceStatistics(
    List<FragmentInstance> instances, boolean verbose, MPPQueryContext context) throws FragmentInstanceFetchException {
    FragmentInstanceStatisticsDrawer fragmentInstanceStatisticsDrawer = new FragmentInstanceStatisticsDrawer();
    fragmentInstanceStatisticsDrawer.renderPlanStatistics(context);
    fragmentInstanceStatisticsDrawer.renderDispatchCost(context);
    IClientManager clientManager = coordinator.getInternalServiceClientManager();
    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStatistics =
      QueryStatisticsFetcher.fetchAllStatistics(instances, clientManager);
    List<StatisticLine> statisticLines =
      fragmentInstanceStatisticsDrawer.renderFragmentInstances(instances, allStatistics, verbose);

    List<String> analyzeResult = new ArrayList<>();
    for (StatisticLine line : statisticLines) {
      StringBuilder sb = new StringBuilder();
      sb.append(line.getValue());
      for (int i = 0;
           i < fragmentInstanceStatisticsDrawer.getMaxLineLength() - line.getValue().length();
           i++) {
        sb.append(" ");
      }
      analyzeResult.add(sb.toString());
    }
    return analyzeResult;
  }

  public static CteDataStore fetchExplainCteQueryResult(
    Table table, Query query, MPPQueryContext context) {
    final long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;
    try {
      return fetchCteExplainPlan(table, query, context, queryId);
    } catch (final Throwable throwable) {
      t = throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
    return null;
  }

  private static boolean isExplainQuery(MPPQueryContext context) {
    return context.getExplainType() == ExplainType.EXPLAIN;
  }

  private static boolean isExplainAnalyzeQuery(MPPQueryContext context) {
    return context.getExplainType() == ExplainType.EXPLAIN_ANALYZE;
  }


  /**
   * 当执行explain查询时，获取WITH子句中SQL的执行计划
   */
  private static CteDataStore fetchCteExplainPlan(
    Table table, Query query, MPPQueryContext context, long queryId) {
    try {
      // 创建一个explain语句来包装CTE查询
      String explainSql = "EXPLAIN " + SqlFormatter.formatSql(query);
      Statement explainStatement = new SqlParser().createStatement(explainSql,context.getZoneId(),SessionManager.getInstance().getCurrSession());

      // 执行explain查询以获取执行计划
      final ExecutionResult executionResult =
        coordinator.executeForTableModel(
          explainStatement,
          new SqlParser(),
          SessionManager.getInstance().getCurrSession(),
          queryId,
          SessionManager.getInstance()
            .getSessionInfoOfTableModel(SessionManager.getInstance().getCurrSession()),
          "Explain common table expression",
          LocalExecutionPlanner.getInstance().metadata,
          context.getTimeOut(),
          false);

      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return null;
      }

      // 获取查询计划的结果
      DatasetHeader datasetHeader = coordinator.getQueryExecution(queryId).getDatasetHeader();
      //TableSchema tableSchema = getTableSchema(datasetHeader, table.getName().toString());
      TableSchema tableSchema = getExplainTableSchema(datasetHeader, table.getName().toString());

      CteDataStore cteDataStore = new CteDataStore(query, tableSchema);
      cteDataStore.setExplain(true);

      // 将CTE的执行计划结果存储起来
      while (coordinator.getQueryExecution(queryId).hasNextResult()) {
        final Optional<TsBlock> tsBlock;
        try {
          tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
        } catch (final IoTDBException e) {
          throw new IoTDBRuntimeException(
            String.format("Fail to get CTE explain plan because %s", e.getMessage()),
            e.getErrorCode(),
            e.isUserException());
        }
        if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
          continue;
        }
        if (!cteDataStore.addTsBlock(tsBlock.get())) {
          return null;
        }
      }

      return cteDataStore;
    } catch (Exception e) {
      // 如果获取执行计划失败，可以记录日志并返回null，这样系统会回退到内联执行模式
      throw new IoTDBRuntimeException(
        String.format("Error occurred when getting CTE explain plan: %s", e.getMessage()),
        TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  private static CteDataStore fetchCteExplainAnalyzePlan(
    Table table, Query query, MPPQueryContext context, long queryId) {
    Throwable t = null;
    try {
      // 非explain查询的原始逻辑
      final ExecutionResult executionResult =
        coordinator.executeForTableModel(
          query,
          new SqlParser(),
          SessionManager.getInstance().getCurrSession(),
          queryId,
          SessionManager.getInstance()
            .getSessionInfoOfTableModel(SessionManager.getInstance().getCurrSession()),
          "Materialize common table expression",
          LocalExecutionPlanner.getInstance().metadata,
          context.getTimeOut(),
          false,
          true);
      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return null;
      }

      // get table schema
      DatasetHeader datasetHeader = coordinator.getQueryExecution(queryId).getDatasetHeader();
      TableSchema tableSchema = getTableSchema(datasetHeader, table.getName().toString());

      CteDataStore cteDataStore = new CteDataStore(query, tableSchema);
      while (coordinator.getQueryExecution(queryId).hasNextResult()) {
        final Optional<TsBlock> tsBlock;
        try {
          tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
        } catch (final IoTDBException e) {
          t = e;
          throw new IoTDBRuntimeException(
            String.format("Fail to materialize CTE because %s", e.getMessage()),
            e.getErrorCode(),
            e.isUserException());
        }
        if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
          continue;
        }
        if (!cteDataStore.addTsBlock(tsBlock.get())) {
          return null;
        }
      }
      return cteDataStore;
    } catch (final Throwable throwable) {
      t = throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
    return null;
  }
  private static TableSchema getTableSchema(DatasetHeader datasetHeader, String cteName) {
    final List<String> columnNames = datasetHeader.getRespColumns();
    final List<TSDataType> columnDataTypes = datasetHeader.getRespDataTypes();
    if (columnNames.size() != columnDataTypes.size()) {
      throw new IoTDBRuntimeException(
          "Size of column names and column data types do not match",
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    final Map<String, Integer> columnNameIndexMap = datasetHeader.getColumnNameIndexMap();
    final List<ColumnSchema> columnSchemaList = new ArrayList<>();

    // build name -> type map
    Map<String, TSDataType> columnNameDataTypeMap =
        IntStream.range(0, columnNames.size())
            .boxed()
            .collect(Collectors.toMap(columnNames::get, columnDataTypes::get));

    // build column schema list of cte table based on columnNameIndexMap
    columnNameIndexMap.entrySet().stream()
        .sorted(Map.Entry.comparingByValue())
        .forEach(
            entry ->
                columnSchemaList.add(
                    new ColumnSchema(
                        entry.getKey(),
                        TypeFactory.getType(columnNameDataTypeMap.get(entry.getKey())),
                        false,
                        TsTableColumnCategory.FIELD)));
    return new TableSchema(cteName, columnSchemaList);
  }

  private static TableSchema getExplainTableSchema(DatasetHeader datasetHeader, String cteName) {
    final List<String> columnNames = datasetHeader.getRespColumns();
    final List<TSDataType> columnDataTypes = datasetHeader.getRespDataTypes();
    if (columnNames.size() != columnDataTypes.size()) {
      throw new IoTDBRuntimeException(
        "Size of column names and column data types do not match",
        TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    final Map<String, Integer> columnNameIndexMap = datasetHeader.getColumnNameIndexMap();
    final List<ColumnSchema> columnSchemaList = new ArrayList<>();

    // build name -> type map
    Map<String, TSDataType> columnNameDataTypeMap =
      IntStream.range(0, columnNames.size())
        .boxed()
        .collect(Collectors.toMap(columnNames::get, columnDataTypes::get));

    // build column schema list of cte table based on columnNameIndexMap
    columnNameDataTypeMap.entrySet().stream()
      .forEach(
        entry ->
          columnSchemaList.add(
            new ColumnSchema(
              entry.getKey(),
              TypeFactory.getType(columnNameDataTypeMap.get(entry.getKey())),
              false,
              TsTableColumnCategory.FIELD)));
    return new TableSchema(cteName, columnSchemaList);
  }
}
