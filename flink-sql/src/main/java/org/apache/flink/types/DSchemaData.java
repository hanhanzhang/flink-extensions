package org.apache.flink.types;

import java.io.Serializable;
import org.apache.flink.annotation.Internal;

@Internal
public interface DSchemaData extends Serializable {

  DSchemaType schemaType();

}
