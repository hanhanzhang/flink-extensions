package com.sdu.flink.table.functions.enhance;

import static com.sdu.flink.table.functions.enhance.utils.ProcessorUtils.createVariableLabel;
import static com.sdu.flink.table.functions.enhance.utils.ProcessorUtils.getDefaultValueMethodName;
import static com.sdu.flink.table.functions.enhance.utils.ProcessorUtils.getNameFromString;
import static com.sdu.flink.table.functions.enhance.utils.ProcessorUtils.memberAccess;

import com.sun.tools.javac.api.JavacTrees;
import com.sun.tools.javac.processing.JavacProcessingEnvironment;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.JCTree.JCBinary;
import com.sun.tools.javac.tree.JCTree.JCExpressionStatement;
import com.sun.tools.javac.tree.JCTree.JCIf;
import com.sun.tools.javac.tree.JCTree.JCMethodDecl;
import com.sun.tools.javac.tree.JCTree.JCReturn;
import com.sun.tools.javac.tree.JCTree.JCVariableDecl;
import com.sun.tools.javac.tree.JCTree.Tag;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.tree.TreeTranslator;
import com.sun.tools.javac.util.List;
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

public class ScalarFunctionAnnotationProcessor extends AbstractProcessor {

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
    Set<? extends Element> elements = roundEnv.getElementsAnnotatedWith(ScalarFunctionEnhance.class);
    messager.printMessage(Kind.NOTE, "Start process 'ScalarFunctionEnhance' annotation ...");

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
    supportedAnnotationTypes.add(ScalarFunctionEnhance.class.getName());
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

        // 执行before()方法
        String beforeMethod = "this.before";
        JCExpressionStatement beforeStatement = treeMaker.Exec(
            treeMaker.Apply(
                // 输入参数类型
                List.nil(),
                // 方法名
                memberAccess(treeMaker, names, beforeMethod),
                // 输入参数
                List.nil()
            ));

        // 执行after()方法
        String afterMethod = "this.after";
        JCExpressionStatement afterStatement = treeMaker.Exec(
            treeMaker.Apply(
                // 输入参数类型
                List.nil(),
                // 方法名
                memberAccess(treeMaker, names, afterMethod),
                // 输入参数
                List.nil()
            ));

        // 执行异常处理
        String onExceptionMethod = "this.onException";
        JCExpressionStatement onExceptionStatement = treeMaker.Exec(
            treeMaker.Apply(
                // 输入参数类型
                List.of(memberAccess(treeMaker, names, "java.lang.Exception")),
                // 方法名
                memberAccess(treeMaker, names, onExceptionMethod),
                // 输入参数
                List.of(treeMaker.Ident(getNameFromString(names, "e")))
            ));
        // 异常处理模块
        JCBinary condition = treeMaker.Binary(Tag.EQ, memberAccess(treeMaker, names, "this.throwException"), treeMaker.Literal(true));
        // 选择默认值方法名
        System.out.println(">>>>>>>>>>>>>>>> RESULT TYPE <<<<<<<<<<<<<<\n" + jcMethodDecl.restype.toString());
        String defaultValueMethodName = getDefaultValueMethodName(jcMethodDecl.restype);
        JCExpressionStatement defaultValueMethodStatement = treeMaker.Exec(
            treeMaker.Apply(
                // 输入参数类型
                List.nil(),
                // 方法名
                memberAccess(treeMaker, names, "this." + defaultValueMethodName),
                // 输入参数
                List.nil()));
        // 定义默认值变量
        JCVariableDecl nullValue = createVariableLabel(treeMaker, names,
            treeMaker.Modifiers(0), "nullValue", jcMethodDecl.restype, defaultValueMethodStatement.expr);
        JCReturn returnExceptionValue = treeMaker.Return(treeMaker.Ident(nullValue.name));

        JCIf ifBlock = treeMaker.If(condition,
            treeMaker.Throw(treeMaker.Ident(getNameFromString(names, "e"))),
            returnExceptionValue);
        JCTree.JCBlock catchBlock = treeMaker.Block(0, List.of(
            // 异常计数及打印日志
            onExceptionStatement,
            nullValue,
            // 判断是否抛出异常
            ifBlock
            ));

        // 方法体Finally
        JCTree.JCBlock finallyBlock = treeMaker.Block(0, List.of(afterStatement));

        // 更改方法执行体
        jcMethodDecl.body = treeMaker.Block(0, List.of(
            beforeStatement,
            // 添加try{...} catch() {...} finally {...}
            treeMaker.Try(jcMethodDecl.body,
                List.of(treeMaker.Catch(createVariableLabel(treeMaker, names, treeMaker.Modifiers(0),
                    "e", memberAccess(treeMaker, names,"java.lang.Exception"), null), catchBlock)),
                finallyBlock
        )));
        System.out.println(">>>>>>>>>>>>>>>Code<<<<<<<<<<<<<<\n" + jcMethodDecl.getBody().toString());
      }
      super.visitMethodDef(jcMethodDecl);
    }



  }

}
