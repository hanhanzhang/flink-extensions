package org.apache.flink.table.codegen;

import java.util.List;

class UserDefineFunctionNotFoundException extends RuntimeException {

  UserDefineFunctionNotFoundException(List<Class<?>> args) {
    super("User define function can't find 'eval' method with argument: " + args);
  }

}
