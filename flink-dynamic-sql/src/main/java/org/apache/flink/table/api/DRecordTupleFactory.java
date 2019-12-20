package org.apache.flink.table.api;

import java.io.Serializable;
import org.apache.flink.types.DRecordTuple;

public interface DRecordTupleFactory<IN> extends Serializable {

  DRecordTuple buildRecordTuple(IN input);

}
