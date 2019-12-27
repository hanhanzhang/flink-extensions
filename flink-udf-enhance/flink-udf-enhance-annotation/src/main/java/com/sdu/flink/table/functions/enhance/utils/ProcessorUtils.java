package com.sdu.flink.table.functions.enhance.utils;

import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.JCTree.JCExpression;
import com.sun.tools.javac.tree.JCTree.JCModifiers;
import com.sun.tools.javac.tree.JCTree.JCVariableDecl;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.util.Name;
import com.sun.tools.javac.util.Names;

public class ProcessorUtils {

  private ProcessorUtils() {

  }


  public static Name getNameFromString(Names names, String s) {
    return names.fromString(s);
  }

  /**
   * 声明变量
   * */
  public static JCVariableDecl createVariableLabel(TreeMaker treeMaker, Names names,
      JCModifiers modifiers, String name, JCExpression varType, JCExpression init) {
    return treeMaker.VarDef(
        modifiers,
        getNameFromString(names, name),
        // 类型
        varType,
        // 初始化语句
        init);
  }


  /**
   * 访问类 / 方法
   * */
  public static JCTree.JCExpression memberAccess(TreeMaker treeMaker, Names names, String expressionStatement) {
    String[] expressionStatementArray = expressionStatement.split("\\.");
    JCTree.JCExpression expr = treeMaker.Ident(getNameFromString(names, expressionStatementArray[0]));
    for (int i = 1; i < expressionStatementArray.length; i++) {
      expr = treeMaker.Select(expr, getNameFromString(names, expressionStatementArray[i]));
    }
    return expr;
  }

}
