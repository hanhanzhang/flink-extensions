package com.sdu.flink.udf.enhance;

import com.sun.tools.javac.api.JavacTrees;
import com.sun.tools.javac.code.Flags;
import com.sun.tools.javac.processing.JavacProcessingEnvironment;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.JCTree.JCMethodDecl;
import com.sun.tools.javac.tree.JCTree.Tag;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.tree.TreeTranslator;
import com.sun.tools.javac.util.List;
import com.sun.tools.javac.util.Name;
import com.sun.tools.javac.util.Names;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;

public class FunctionEnhanceAnnotationProcessor extends AbstractProcessor {

  // 编译时期打日志
  private Messager messager;
  // 抽象语法树AST
  private JavacTrees javacTrees;
  // 创建AST节点
  private TreeMaker treeMaker;
  // 创建标识符
  private Names names;

  @Override
  public synchronized void init(ProcessingEnvironment processingEnv) {
    super.init(processingEnv);

    this.messager = processingEnv.getMessager();
    this.javacTrees = JavacTrees.instance(processingEnv);
    this.treeMaker = TreeMaker.instance(((JavacProcessingEnvironment) processingEnv).getContext());
    this.names = Names.instance(((JavacProcessingEnvironment) processingEnv).getContext());
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    // 获取标记FunctionEnhance的元素
    Set<? extends Element> elements = roundEnv.getElementsAnnotatedWith(FunctionEnhance.class);
    messager.printMessage(Kind.NOTE, "Start process 'FunctionEnhance' annotation ...");

    elements.forEach((Element e) -> {
      // 获取当前元素的JCTree对象
      JCTree jcTree = javacTrees.getTree(e);

      // 遍历JCTree节点
      jcTree.accept(new JCTreeNodeTranslator());

    });

    return true;
  }

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    Set<String> supportedAnnotationTypes = new HashSet<>();
    supportedAnnotationTypes.add(FunctionEnhance.class.getName());
    return supportedAnnotationTypes;
  }

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.RELEASE_8;
  }

  private class JCTreeNodeTranslator extends TreeTranslator {

    @Override
    public void visitMethodDef(JCMethodDecl jcMethodDecl) {
      String methodName = jcMethodDecl.name.toString();
      if (methodName.equals("eval")) {
        messager.printMessage(Kind.NOTE, "Start enhance 'eval' method ...");

        // 生成表达式: System.currentTimeMillis()
        JCTree.JCExpressionStatement time = treeMaker.Exec(
            treeMaker.Apply(
                // 参数类型(传入方法的参数的类型), 如果是无参的不能设置为null, 使用 List.nil()
                List.nil(),
                memberAccess("java.lang.System.currentTimeMillis"),
                // 因为不需要传递参数, 所以直接设置为List.nil(), 不能设置null
                List.nil()
                // 参数集合[集合中每一项的类型需要跟第一个参数对照]
            )
        );

        // 创建startTime变量, 编译期间为其赋值
        JCTree.JCVariableDecl startTime = createVariableLabel(treeMaker.Modifiers(0),
            "startTime", memberAccess("java.lang.Long"), treeMaker.Literal(System.currentTimeMillis()));

        // 计算耗时
        JCTree.JCExpressionStatement costStatement = treeMaker.Exec(
            treeMaker.Apply(
                // 参数类型
                List.of(memberAccess("java.lang.Long"), memberAccess("java.lang.Long")),
                // 方法体
                memberAccess("java.lang.Math.subtractExact"),
                // 参数
                List.of(time.expr, treeMaker.Ident(startTime.name))
            )
        );

        // 打印耗时
        JCTree.JCExpressionStatement printTimeCostStatement = treeMaker.Exec(
            treeMaker.Apply(
                List.of(memberAccess("java.lang.String"), memberAccess("java.lang.Long")),
                memberAccess("java.lang.System.out.printf"),
                List.of(treeMaker.Literal(">>>>>>>>>Method: %s, execute cost %d ms."), treeMaker.Literal(methodName),
                    costStatement.getExpression())
            )
        );

        // 异常处理模块
        JCTree.JCBlock catchBlock = treeMaker.Block(0, List.of(
            // 判断
            treeMaker.If(treeMaker.Binary(Tag.EQ, memberAccess("com.sdu.flink.table.functions.isThrowException"), treeMaker.Literal(true)),
                treeMaker.Throw(treeMaker.Ident(getNameFromString("e"))),
                null)
        ));

        // 方法体Finally
        JCTree.JCBlock finallyBlock = treeMaker.Block(0, List.of(printTimeCostStatement));

        // 更改方法执行体
        jcMethodDecl.body = treeMaker.Block(0, List.of(
            // 定义局部变量, 并赋值
            startTime,
            treeMaker.Exec(treeMaker.Assign(
                treeMaker.Ident(startTime.name), time.expr)),
            // 添加try{...} catch() {...} finally {...}
            treeMaker.Try(jcMethodDecl.body,
                List.of(treeMaker.Catch(createVariableLabel(treeMaker.Modifiers(0),
                    "e", memberAccess("java.lang.Exception"), null), catchBlock)),
                finallyBlock
        )));


      }
      super.visitMethodDef(jcMethodDecl);
    }

    private Name getNameFromString(String s) {
      return names.fromString(s);
    }

    private JCTree.JCVariableDecl createVariableLabel(JCTree.JCModifiers modifiers, String name,
        JCTree.JCExpression varType, JCTree.JCExpression init) {
      return treeMaker.VarDef(
          modifiers,
          getNameFromString(name),
          // 类型
          varType,
          // 初始化语句
          init);
    }

    private JCTree.JCExpression memberAccess(String expressionStatement) {
      String[] expressionStatementArray = expressionStatement.split("\\.");
      JCTree.JCExpression expr = treeMaker.Ident(getNameFromString(expressionStatementArray[0]));
      for (int i = 1; i < expressionStatementArray.length; i++) {
        expr = treeMaker.Select(expr, getNameFromString(expressionStatementArray[i]));
      }
      return expr;
    }

  }

}
