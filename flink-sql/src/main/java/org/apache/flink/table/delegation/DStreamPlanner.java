package org.apache.flink.table.delegation;

import static java.lang.String.format;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.DAppendStreamTableSink;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogManager.ResolvedTable;
import org.apache.flink.table.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.executor.StreamExecutor;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.PlannerExpressionConverter;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OutputConversionModifyOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.UnregisteredSinkModifyOperation;
import org.apache.flink.table.plan.DStreamOptimizer;
import org.apache.flink.table.plan.nodes.datastream.DDataStreamRel;
import org.apache.flink.table.plan.util.UpdatingPlanChecker;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.PartitionableTableSink;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.sqlexec.SqlToOperationConverter;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.types.DStreamRecord;
import org.apache.flink.types.DStreamRecordType;

/**
 * @author hanhan.zhang
 */
public class DStreamPlanner implements Planner {

  private final Executor executor;
  private final TableConfig config;
  private final CatalogManager catalogManager;

  private final PlanningConfigurationBuilder planningConfigurationBuilder;
  private final DStreamOptimizer optimizer;

  public DStreamPlanner(Executor executor, TableConfig config, FunctionCatalog functionCatalog, CatalogManager catalogManager) {
    this.executor = executor;
    this.config = config;
    this.catalogManager = catalogManager;

    CalciteSchema internalSchema = CalciteSchemaBuilder.asRootSchema(new CatalogManagerCalciteSchema(
        catalogManager, true));
    ExpressionBridge<PlannerExpression> expressionBridge = new ExpressionBridge<>(functionCatalog, PlannerExpressionConverter.INSTANCE());

    this.planningConfigurationBuilder = new PlanningConfigurationBuilder(config, functionCatalog, internalSchema, expressionBridge);
    this.optimizer = new DStreamOptimizer(
        () -> config.getPlannerConfig()
            .unwrap(CalciteConfig.class)
            .orElse(CalciteConfig.DEFAULT()),
        planningConfigurationBuilder);
  }

  private FlinkPlannerImpl getFlinkPlanner() {
    String currentCatalogName = catalogManager.getCurrentCatalog();
    String currentDatabase = catalogManager.getCurrentDatabase();

    return planningConfigurationBuilder.createFlinkPlanner(currentCatalogName, currentDatabase);
  }

  private FlinkRelBuilder getRelBuilder() {
    String currentCatalogName = catalogManager.getCurrentCatalog();
    String currentDatabase = catalogManager.getCurrentDatabase();

    return planningConfigurationBuilder.createRelBuilder(currentCatalogName, currentDatabase);
  }

  @Override
  public List<Operation> parse(String stmt) {
    // 解析SQL语句
    FlinkPlannerImpl planner = getFlinkPlanner();
    // parse the sql query
    SqlNode parsed = planner.parse(stmt);

    if (parsed instanceof RichSqlInsert) {
      RichSqlInsert sqlInsert = (RichSqlInsert) parsed;
      SqlNodeList targetColumnList = sqlInsert.getTargetColumnList();
      if (targetColumnList != null && targetColumnList.size() != 0) {
        throw new ValidationException("Partial inserts are not supported");
      }
      return Collections.singletonList(SqlToOperationConverter.convert(planner, sqlInsert));
    }

    else if (parsed.getKind().belongsTo(SqlKind.QUERY) || parsed.getKind().belongsTo(SqlKind.DDL)) {
      return Collections.singletonList(SqlToOperationConverter.convert(planner, parsed));
    }

    else {
      throw new TableException(
          "Unsupported SQL query! parse() only accepts SQL queries of type " +
              "SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY or INSERT;" +
              "and SQL DDLs of type CREATE TABLE");
    }
  }

  @Override
  public List<Transformation<?>> translate(List<ModifyOperation> tableOperations) {
    return tableOperations.stream()
        .map(this::translate)
        .collect(Collectors.toList());
  }

  @Override
  public String explain(List<Operation> operations, boolean extended) {
    return null;
  }

  @Override
  public String[] getCompletionHints(String statement, int position) {
    return new String[0];
  }

  public StreamExecutionEnvironment getExecutionEnvironment() {
    StreamExecutor streamExecutor = (StreamExecutor) executor;
    return streamExecutor.getExecutionEnvironment();
  }

  @SuppressWarnings("unchecked")
  private Optional<TableSink<?>> getTableSink(List<String> tablePath) {
    Optional<ResolvedTable> s = catalogManager.resolveTable(tablePath.toArray(new String[tablePath.size()]));
    if (s.isPresent()) {
      ResolvedTable resolvedTable = s.get();
      if (resolvedTable.getExternalCatalogTable().isPresent()) {
        return Optional.of(TableFactoryUtil.findAndCreateTableSink(resolvedTable.getExternalCatalogTable().get()));
      }

      Optional<CatalogBaseTable> op = resolvedTable.getCatalogTable();
      if (op.isPresent() && op.get() instanceof ConnectorCatalogTable) {
        ConnectorCatalogTable table = (ConnectorCatalogTable) op.get();
        return table.getTableSink();
      }

      if (op.isPresent() && op.get() instanceof CatalogTable) {
        Optional<Catalog> catalog = catalogManager.getCatalog(resolvedTable.getTablePath().get(0));
        CatalogTable catalogTable = (CatalogTable) resolvedTable.getCatalogTable().get();
        if (catalog.isPresent() && catalog.get().getTableFactory().isPresent()) {
          String dbName = resolvedTable.getTablePath().get(1);
          String tableName = resolvedTable.getTablePath().get(2);
          Optional<TableSink> sink = TableFactoryUtil.createTableSinkForCatalogTable(
              catalog.get(), catalogTable, new ObjectPath(dbName, tableName));
          if (sink.isPresent()) {
            return Optional.of(sink.get());
          }
        }

        Map<String, String> sinkProperties = catalogTable.toProperties();
        return Optional.of(TableFactoryService.find(TableSinkFactory.class, sinkProperties)
            .createTableSink(sinkProperties));
      }
    }

    return Optional.empty();
  }

  private Transformation<?> translate(ModifyOperation tableOperation) {

    if (tableOperation instanceof UnregisteredSinkModifyOperation) {
      UnregisteredSinkModifyOperation<?> s = (UnregisteredSinkModifyOperation<?>) tableOperation;
      return writeToSink(s.getChild(), s.getSink(), unwrapQueryConfig());
    }

    else if (tableOperation instanceof CatalogSinkModifyOperation) {
      CatalogSinkModifyOperation catalogSink = (CatalogSinkModifyOperation) tableOperation;

      return getTableSink(catalogSink.getTablePath())
          .map(sink -> {
            // TODO: 不校验
//            TableSinkUtils.validateSink(
//                catalogSink.getStaticPartitions(),
//                catalogSink.getChild(),
//                catalogSink.getTablePath(),
//                sink);

            if (sink instanceof PartitionableTableSink) {
              PartitionableTableSink partitionTableSink = (PartitionableTableSink) sink;
              if (partitionTableSink.getPartitionFieldNames() != null && !partitionTableSink.getPartitionFieldNames().isEmpty()) {
                partitionTableSink.setStaticPartition(catalogSink.getStaticPartitions());
              }
            }

            return writeToSink(catalogSink.getChild(), sink, unwrapQueryConfig());
          })
          .orElseThrow(() -> new TableException(format("Sink %s does not exists", catalogSink.getTablePath())));
    }

    else if (tableOperation instanceof OutputConversionModifyOperation) {
      return null;
    }

    else {
      throw new TableException("Unsupported ModifyOperation: " + tableOperation);
    }
  }

  private StreamQueryConfig unwrapQueryConfig() {
    return new StreamQueryConfig(
        config.getMinIdleStateRetentionTime(), config.getMaxIdleStateRetentionTime());
  }

  private <T> Transformation<?> writeToSink(QueryOperation tableOperation,
      TableSink<T> sink, StreamQueryConfig config) {

    DataStreamSink<?> resultSink;
    if (sink instanceof RetractStreamTableSink<?>) {
      // TODO: 2019-09-27
      resultSink = null;
    }

    else if (sink instanceof UpsertStreamTableSink<?>) {
      // TODO: 2019-09-27
      resultSink = null;
    }

    else if (sink instanceof DAppendStreamTableSink) {
      DAppendStreamTableSink appendSink = (DAppendStreamTableSink) sink;
      resultSink = writeToAppendSink(appendSink, tableOperation, config);
    }

    else {
      throw new ValidationException("Stream Tables can only be emitted by AppendStreamTableSink, "
          + "RetractStreamTableSink, or UpsertStreamTableSink.");
    }

    if (resultSink != null) {
      return resultSink.getTransformation();
    } else {
      return null;
    }

  }

  private DataStreamSink<?> writeToAppendSink(
      AppendStreamTableSink<DRecordTuple> sink, QueryOperation tableOperation, StreamQueryConfig streamQueryConfig) {

    // step1: optimize plan
    RelNode relNode = getRelBuilder().tableOperation(tableOperation).build();
    RelNode optimizedPlan = optimizer.optimize(relNode, false, getRelBuilder());
    if (!UpdatingPlanChecker.isAppendOnly(optimizedPlan)) {
      throw new TableException(
          "AppendStreamTableSink requires that Table has only insert changes.");
    }

    // step2: convert DataStream
    DataStream<DRecordTuple> dataStream = translateToSqlElement(optimizedPlan, streamQueryConfig);
    return sink.consumeDataStream(dataStream);
  }


  private DataStream<DRecordTuple> translateToSqlElement(RelNode logicalPlan, StreamQueryConfig queryConfig) {
    if (logicalPlan instanceof DDataStreamRel) {
      DDataStreamRel node = (DDataStreamRel) logicalPlan;
      return node.translateToSqlElement(this, queryConfig)
          .filter((DStreamRecord streamRecord) -> streamRecord.getRecordType() == DStreamRecordType.RECORD)
          .returns(DStreamRecord.class)
          .map(DStreamRecord::recordTuple)
          .returns(DRecordTuple.class);
    }
    else {
      //
      throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.");
    }
  }

}
