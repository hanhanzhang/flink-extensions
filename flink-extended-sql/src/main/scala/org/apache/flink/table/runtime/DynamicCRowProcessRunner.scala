package org.apache.flink.table.runtime

import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, RowTypeInfo}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.types.schema.SqlCalcSchema
import org.apache.flink.table.types.{RowDataType, SqlSchemaTuple}
import org.apache.flink.table.util.Logging
import org.apache.flink.table.utils.JsonUtils
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

class DynamicCRowProcessRunner(
    streamUniqueName: String,
    name: String,
    code: String)
  extends ProcessFunction[CRow, CRow]
  with ResultTypeQueryable[CRow]
  with Compiler[ProcessFunction[Row, Row]]
  with Logging{

  private var parameters: Configuration = _

  private var function: ProcessFunction[Row, Row] = _
  private var cRowWrapper: DynamicCRowWrappingCollector = _


  override def open(parameters: Configuration): Unit = {
    this.parameters = parameters
    LOG.debug(s"Compiling ProcessFunction: $name \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    LOG.debug("Instantiating ProcessFunction.")
    function = clazz.newInstance()
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, parameters)

    this.cRowWrapper = new DynamicCRowWrappingCollector()
  }

  override def processElement(
      in: CRow,
      ctx: ProcessFunction[CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {
    cRowWrapper.out = out
    cRowWrapper.setChange(in.change)

    val value = in.row
    val rowType = value.getField(0).asInstanceOf
    cRowWrapper.setRowType(rowType)

    if (rowType.equals(RowDataType.SCHEMA.name())) {
      val schema: String = value.getField(2).asInstanceOf
      FunctionUtils.closeFunction(function)
      function = generateFunction(schema)
      cRowWrapper.collect(schema)
    } else if (rowType.equals(RowDataType.DATA.name())) {
      val data = value.getField(2).asInstanceOf
      val columns = JsonUtils.fromJson(data, classOf[Row])
      function.processElement(
        columns,
        ctx.asInstanceOf[ProcessFunction[Row, Row]#Context],
        cRowWrapper)
    } else {
      throw new RuntimeException(s"Unsupported row type: $rowType" )
    }
  }

  override def getProducedType: TypeInformation[CRow] = {
    val fieldNames = Array[String]("type", "data")
    val fieldTypes = Array[TypeInformation[_]](Types.STRING, Types.STRING)
    val rowType = new RowTypeInfo(fieldTypes, fieldNames)
    CRowTypeInfo(rowType)
  }

  override def close(): Unit = {
    FunctionUtils.closeFunction(function)
  }

  private def generateFunction(schema: String) : ProcessFunction[Row, Row] = {
    val schemaTuple = JsonUtils.fromJson(schema, classOf[SqlSchemaTuple])
    val calc = schemaTuple.getStreamNodeSchema(streamUniqueName, classOf[SqlCalcSchema])
    val name = calc.getName
    val code = calc.getCode
    LOG.info(s"Compiling ProcessFunction: $calc \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    LOG.info("Instantiating ProcessFunction.")
    val function = clazz.newInstance()
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, parameters)
    function
  }

}
