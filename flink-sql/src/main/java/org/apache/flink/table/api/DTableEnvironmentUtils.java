package org.apache.flink.table.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.DStreamPlanner;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.executor.StreamExecutor;

/**
 * @author hanhan.zhang
 */
public class DTableEnvironmentUtils {

  private DTableEnvironmentUtils() {

  }


  public static StreamTableEnvironment create(StreamExecutionEnvironment executionEnvironment) {

    CatalogManager catalogManager = new CatalogManager(
        EnvironmentSettings.DEFAULT_BUILTIN_CATALOG,
        new GenericInMemoryCatalog(EnvironmentSettings.DEFAULT_BUILTIN_CATALOG, EnvironmentSettings.DEFAULT_BUILTIN_DATABASE));

    FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager);

    Executor executor = new StreamExecutor(executionEnvironment);

    TableConfig tableConfig = new TableConfig();

    Planner planner = new DStreamPlanner(executor, tableConfig, functionCatalog, catalogManager);

    return new StreamTableEnvironmentImpl(
        catalogManager,
        functionCatalog,
        tableConfig,
        executionEnvironment,
        planner,
        executor,
        true);
  }

}
