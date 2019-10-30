package org.apache.flink.table.updater;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkRelBuilderFactory;
import org.apache.flink.table.calcite.FlinkRelOptClusterFactory;
import org.apache.flink.table.catalog.CatalogReader;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.PlannerExpressionConverter;
import org.apache.flink.table.plan.DStreamOptimizer;
import org.apache.flink.table.plan.nodes.datastream.DDataStreamCalc;
import org.apache.flink.table.plan.schema.TableSourceTable;
import org.apache.flink.table.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;
import org.apache.flink.types.DRecordTuple;

public class DSqlUpdater {

  private DSqlUpdater() {

  }

  private static CalciteSchema registerSchema(List<DTableStatement> tableStatements) {
    SchemaPlus schemaPlus = CalciteSchema.createRootSchema(true).plus();
    for (final DTableStatement tableStatement : tableStatements) {
      TableSourceTable<DRecordTuple> table = new TableSourceTable<>(
          new DFakeStreamTableTableSource(tableStatement.getColumns()), true, FlinkStatistic.UNKNOWN());
      schemaPlus.add(tableStatement.getTableName(), table);
    }

    return CalciteSchema.from(schemaPlus);
  }

  private static PlanningConfigurationBuilder buildPlanConfigureBuilder(TableConfig config, CalciteSchema rootSchema, FunctionCatalog functionCatalog) {
    ExpressionBridge<PlannerExpression> expressionBridge = new ExpressionBridge<>(functionCatalog,
        PlannerExpressionConverter.INSTANCE());

    return new PlanningConfigurationBuilder(config, functionCatalog, rootSchema, expressionBridge);
  }

  private static FlinkRelBuilder createRelBuilder(CalciteSchema rootSchema, PlanningConfigurationBuilder pcb) {
    RelOptCluster cluster = FlinkRelOptClusterFactory.create(
        pcb.getPlanner(),
        new RexBuilder(pcb.getTypeFactory()));

    RelOptSchema relOptSchema = createCatalogReader(rootSchema, pcb);

    return new FlinkRelBuilder(pcb.getContext(), cluster, relOptSchema, null);
  }

  private static FlinkPlannerImpl createFlinkPlanner(CalciteSchema rootSchema, PlanningConfigurationBuilder pcb) {
    return new FlinkPlannerImpl(
        createFrameworkConfig(pcb),
        isLenient -> createCatalogReader(rootSchema, pcb),
        pcb.getPlanner(),
        pcb.getTypeFactory());
  }

  private static FrameworkConfig createFrameworkConfig(PlanningConfigurationBuilder pcb) {
    return Frameworks
        .newConfigBuilder()
        .parserConfig(pcb.getSqlParserConfig())
//        .costFactory(costFactory)
        .typeSystem(pcb.getTypeFactory().getTypeSystem())
//        .operatorTable(getSqlOperatorTable(calciteConfig(tableConfig), functionCatalog))
        .sqlToRelConverterConfig(getSqlToRelConverterConfig())
        .build();
  }


  private static SqlToRelConverter.Config getSqlToRelConverterConfig() {
    return SqlToRelConverter.configBuilder()
        .withTrimUnusedFields(false)
        .withConvertTableAccess(false)
        .withInSubQueryThreshold(Integer.MAX_VALUE)
        .withRelBuilderFactory(new FlinkRelBuilderFactory(null))
        .build();
  }

  /**
   * CatalogReader 读取元数据信息
   * */
  private static CatalogReader createCatalogReader(CalciteSchema rootSchema, PlanningConfigurationBuilder pcb) {
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
        String.valueOf(pcb.getSqlParserConfig().caseSensitive()));

    return new CatalogReader(
        rootSchema,
        Collections.singletonList(rootSchema.path(null)),
        pcb.getTypeFactory(),
        new CalciteConnectionConfigImpl(props));
  }

  public static DDataStreamCalc parseSql(List<DTableStatement> tableStatements, String sql) {
    TableConfig config = new TableConfig();

    // step1: 注册元数据
    CalciteSchema rootSchema = registerSchema(tableStatements);

    // step2: PlanningConfigurationBuilder(TODO: UDF)
    PlanningConfigurationBuilder pcb = buildPlanConfigureBuilder(config, rootSchema, null);

    // step3: FlinkRelBuilder
    FlinkRelBuilder relBuilder = createRelBuilder(rootSchema, pcb);

    // step4: FlinkPlannerImpl
    FlinkPlannerImpl planner = createFlinkPlanner(rootSchema, pcb);

    // step5: RelOptimizer
    DStreamOptimizer optimizer = new DStreamOptimizer(() -> config.getPlannerConfig().unwrap(CalciteConfig.class).orElse(CalciteConfig.DEFAULT()),
        pcb);

    // step6: 转为 DDataStreamRel
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

}
