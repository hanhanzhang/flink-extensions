package com.sdu.flink.table.functions.enhance.utils;

import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.JCTree.JCExpression;
import com.sun.tools.javac.tree.JCTree.JCIdent;
import com.sun.tools.javac.tree.JCTree.JCModifiers;
import com.sun.tools.javac.tree.JCTree.JCTypeApply;
import com.sun.tools.javac.tree.JCTree.JCVariableDecl;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.util.Name;
import com.sun.tools.javac.util.Names;
import javax.lang.model.type.TypeKind;

public class ProcessorUtils {

  private ProcessorUtils() {

  }

  public static String getDefaultValueMethodName(JCExpression restType) {
    TypeKind typeKind = restType.type.getKind();
    switch (typeKind) {
      case BOOLEAN:
        return "getDefaultBooleanValue";
      case DOUBLE:
        return "getDefaultDoubleValue";
      case FLOAT:
        return "getDefaultFloatValue";
      case INT:
        return "getDefaultIntValue";
      case SHORT:
        return "getDefaultShortValue";
      case LONG:
        return "getDefaultLongValue";
      case BYTE:
        return "getDefaultByteValue";
      case ARRAY:
        return "getDefaultArrayValue";
      case DECLARED:
        return getDefaultObjectMethodName(restType);
      default:
        throw new UnsupportedOperationException("Unsupported TypeKind: " + typeKind);
    }
  }

  private static String getDefaultObjectMethodName(JCExpression restType) {
    if (restType instanceof JCIdent) {
      JCIdent jcIdent = (JCIdent) restType;
      String name = jcIdent.getName().toString();
      switch (name) {
        case "String":
          return "getDefaultStringValue";
        default:
          throw new UnsupportedOperationException("Unsupported JCIdent: " + name);
      }
    }
    else if (restType instanceof JCTypeApply) {
      JCTypeApply jcTypeApply = (JCTypeApply) restType;
      switch (jcTypeApply.clazz.toString()) {
        case "Map":
          return "getDefaultMapValue";
        default:
          throw new UnsupportedOperationException("Unsupported JCTypeApply: " + jcTypeApply.clazz.toString());
      }
    }
    throw new UnsupportedOperationException("Unsupported JCExpression: " + restType.getClass().getSimpleName());
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
