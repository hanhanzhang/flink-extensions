package org.apache.flink.table;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;

public interface DynamicDataTypeConverter {

  String SCHEMA_NAME_TEMPLATE = "schema.%d.name";
  String SCHEMA_EXPR_TEMPLATE = "schema.%d.expr";
  String SCHEMA_TYPE_TEMPLATE = "schema.%d.data-type";

  default Tuple2<String[], TypeInformation<?>[]> toNameAndTypes(Map<String, String> properties) {
    List<String> names = new ArrayList<>();
    List<TypeInformation<?>> types = new ArrayList<>();
    for (int i = 0;; ++i) {
      String name = properties.getOrDefault(format(SCHEMA_NAME_TEMPLATE, i), null);
      if (name == null) {
        break;
      }

      // computed column
      String expr = properties.getOrDefault(format(SCHEMA_EXPR_TEMPLATE, i), null);
      if (expr != null && !expr.isEmpty()) {
        continue;
      }
      names.add(name);

      //
      String type = properties.getOrDefault(format(SCHEMA_TYPE_TEMPLATE, i), null);
      DataType dataType = TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(type));
      types.add(TypeConversions.fromDataTypeToLegacyInfo(dataType));
    }

    return Tuple2.of(
        names.toArray(new String[0]),
        types.toArray(new TypeInformation<?>[0])
    );
  }

  default TableSchema toTableSchema(Map<String, String> properties) {
    // @See DescriptorProperties
    TableSchema.Builder builder = TableSchema.builder();
    for (int i = 0;;i++) {
      String name = properties.getOrDefault(format(SCHEMA_NAME_TEMPLATE, i), null);
      if (name == null) {
        break;
      }

      String type = properties.getOrDefault(format(SCHEMA_TYPE_TEMPLATE, i), null);
      DataType dataType = TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(type));

      String expr = properties.getOrDefault(format(SCHEMA_EXPR_TEMPLATE, i), null);
      if (expr != null) {
        builder.field(name, dataType, expr);
      } else {
        builder.field(name, dataType);
      }
    }

    return builder.build();
  }

}
