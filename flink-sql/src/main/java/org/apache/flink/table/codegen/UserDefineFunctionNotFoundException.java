package org.apache.flink.table.codegen;

import java.util.List;

public class UserDefineFunctionNotFoundException extends RuntimeException {

  public UserDefineFunctionNotFoundException(List<Class<?>> args) {
    super("User define function can't find 'eval' method with argument: " + args);
  }

}
