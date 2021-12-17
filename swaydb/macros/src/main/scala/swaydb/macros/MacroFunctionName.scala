package swaydb.macros

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

object MacroFunctionName {

  final val useFullyQualifiedName = false

  def functionName(param: Any): String = macro functionName_Impl

  def functionName_Impl(c: blackbox.Context)(param: c.Expr[Any]): c.Expr[String] = {
    import c.universe._

    def getName(select: c.universe.Select) =
      select match {
        case Select(t, TermName(methodName)) =>
          val baseClass = t.tpe.resultType.baseClasses.head
          val className = if (useFullyQualifiedName) baseClass.fullName else baseClass.name
          c.Expr[String](Literal(Constant(className + "." + methodName)))

        case _ =>
          c.abort(c.enclosingPosition, "Not a function: " + show(param.tree))
      }

    param.tree match {
      case Apply(select @ Select(_, _), _) =>
        getName(select)

      case Apply(Apply(select @ Select(_, _), _), _) =>
        getName(select)

      case Function(_, Apply(select @ Select(_, _), _)) =>
        getName(select)

      case tree =>
        c.abort(c.enclosingPosition, s"Not a function: ${show(tree)}")
    }
  }
}
