package org.apache.flink.table.exec;

import java.util.List;

class UserDefineFunctionNotFoundException extends RuntimeException {

  UserDefineFunctionNotFoundException(List<Class<?>> args) {
    super("User define function can't find 'eval' method with argument: " + args);
  }

}
