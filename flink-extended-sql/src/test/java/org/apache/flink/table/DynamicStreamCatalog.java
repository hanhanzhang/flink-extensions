package org.apache.flink.table;

import java.util.Optional;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.TableFactory;

public class DynamicStreamCatalog extends GenericInMemoryCatalog {

  DynamicStreamCatalog(String name, String database) {
    super(name, database);
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
    return super.getTable(tablePath);
  }

  @Override
  public Optional<TableFactory> getTableFactory() {
    return Optional.of(new DynamicStreamTableFactory());
  }

}

