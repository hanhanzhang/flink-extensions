package org.apache.flink.table.updater;

import static org.apache.flink.types.DTypeUtils.javaTypeToDataType;

import com.sdu.flink.utils.SqlUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkRelBuilderFactory;
import org.apache.flink.table.calcite.FlinkRelOptClusterFactory;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.catalog.CatalogReader;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.PlannerExpressionConverter;
import org.apache.flink.table.plan.DStreamOptimizer;
import org.apache.flink.table.plan.nodes.datastream.DDataStreamCalc;
import org.apache.flink.table.plan.schema.TableSourceTable;
import org.apache.flink.table.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;
import org.apache.flink.table.sources.DStreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.types.DSchemaTuple;

public class DSqlParer {

  private FlinkPlannerImpl planner;

  private final FlinkRelBuilder relBuilder;

  private final DStreamOptimizer optimizer;

  public DSqlParer(List<TableStatement> tableStatements) {

    SchemaPlus schemaPlus = CalciteSchema.createRootSchema(true).plus();
    for (final TableStatement tableStatement : tableStatements) {
      TableSourceTable<DRecordTuple> table = new TableSourceTable<>(
          new EmptyDStreamTableSource(tableStatement.getColumns()), true, FlinkStatistic.UNKNOWN());
      schemaPlus.add(tableStatement.getTableName(), table);
    }
    CalciteSchema rootSchema = CalciteSchema.from(schemaPlus);

    // TODO: UDF
    ExpressionBridge<PlannerExpression> expressionBridge = new ExpressionBridge<>(null,
        PlannerExpressionConverter.INSTANCE());

    TableConfig config = new TableConfig();

    PlanningConfigurationBuilder pcb = new PlanningConfigurationBuilder(config,
        null, rootSchema, expressionBridge);
    RelOptPlanner relOptPlanner = pcb.getPlanner();
    FlinkTypeFactory typeFactory = pcb.getTypeFactory();
    SqlParser.Config sqlParseConfig = pcb.getSqlParserConfig();

    optimizer = new DStreamOptimizer(() -> config.getPlannerConfig().unwrap(CalciteConfig.class).orElse(CalciteConfig.DEFAULT()),
        pcb);

    relBuilder = createRelBuilder(rootSchema, relOptPlanner, typeFactory, sqlParseConfig, pcb.getContext());
    planner = createFlinkPlanner(rootSchema, typeFactory, sqlParseConfig, relOptPlanner);
  }


  public DDataStreamCalc parseSql(String sql) {
    SqlNode parsed = planner.parse(sql);
    SqlNode validated = planner.validate(parsed);
    RelRoot relRoot = planner.rel(validated);
    RelNode relational = relRoot.project();
    RelNode optimizedRelNode = optimizer.optimize(relational, false, relBuilder);

    if (optimizedRelNode instanceof DDataStreamCalc) {
      return (DDataStreamCalc) optimizedRelNode;
    }

    throw new UnsupportedOperationException("Unsupported sql: " + sql);
  }

  private static FlinkRelBuilder createRelBuilder(CalciteSchema rootSchema, RelOptPlanner planner,
      RelDataTypeFactory typeFactory, SqlParser.Config sqlParseConfig, Context context) {
    RelOptCluster cluster = FlinkRelOptClusterFactory.create(
        planner,
        new RexBuilder(typeFactory));

    RelOptSchema relOptSchema = createCatalogReader(rootSchema, typeFactory, sqlParseConfig);

    return new FlinkRelBuilder(context, cluster, relOptSchema, null);
  }

  private static FlinkPlannerImpl createFlinkPlanner(CalciteSchema rootSchema, FlinkTypeFactory typeFactory,
      SqlParser.Config config, RelOptPlanner planner) {
    return new FlinkPlannerImpl(
        createFrameworkConfig(config, typeFactory),
        isLenient -> createCatalogReader(rootSchema, typeFactory, config),
        planner,
        typeFactory);
  }

  private static FrameworkConfig createFrameworkConfig(SqlParser.Config sqlParseConfig, FlinkTypeFactory typeFactory) {
    return Frameworks
        .newConfigBuilder()
        .parserConfig(sqlParseConfig)
//        .costFactory(costFactory)
        .typeSystem(typeFactory.getTypeSystem())
//        .operatorTable(getSqlOperatorTable(calciteConfig(tableConfig), functionCatalog))
        .sqlToRelConverterConfig(getSqlToRelConverterConfig())
        .build();
  }


  private static CatalogReader createCatalogReader(CalciteSchema rootSchema, RelDataTypeFactory typeFactory,
      SqlParser.Config parserConfig) {
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
        String.valueOf(parserConfig.caseSensitive()));

    return new CatalogReader(
        rootSchema,
        Collections.singletonList(rootSchema.path(null)),
        typeFactory,
        new CalciteConnectionConfigImpl(props));
  }

  private static SqlToRelConverter.Config getSqlToRelConverterConfig() {
    return SqlToRelConverter.configBuilder()
        .withTrimUnusedFields(false)
        .withConvertTableAccess(false)
        .withInSubQueryThreshold(Integer.MAX_VALUE)
        .withRelBuilderFactory(new FlinkRelBuilderFactory(null))
        .build();
  }

  public static class EmptyDStreamTableSource implements DStreamTableSource {

    private TableSchema tableSchema;
    private Map<String, DataType> fieldDataTypes;

    EmptyDStreamTableSource(List<ColumnStatement> columnStatements) {
      fieldDataTypes = new HashMap<>();
      TableSchema.Builder builder = TableSchema.builder();
      for (ColumnStatement columnStatement : columnStatements) {
        DataType dataType = javaTypeToDataType(columnStatement.getType());
        builder.field(columnStatement.getName(), dataType);
        fieldDataTypes.put(columnStatement.getName(), dataType);
      }
      tableSchema = builder.build();
    }

    @Override
    public BroadcastStream<DSchemaTuple> getBroadcastStream(StreamExecutionEnvironment execEnv) {
      return null;
    }

    @Override
    public DataStream<DRecordTuple> getDataStream(StreamExecutionEnvironment execEnv) {
      return null;
    }

    @Override
    public TableSchema getTableSchema() {
      return tableSchema;
    }

    @Override
    public DataType getProducedDataType() {
      return SqlUtils.fromTableSchema(tableSchema);
    }
  }

}
