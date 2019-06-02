package swaydb.macros

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

object SealedList {

  def list[A]: List[A] = macro list_impl[A]

  def list_impl[A: c.WeakTypeTag](c: blackbox.Context): c.Expr[List[A]] = {
    import c.universe._

    val symbol = weakTypeOf[A].typeSymbol

    if (!symbol.isClass)
      c.abort(
        c.enclosingPosition,
        "Can only enumerate values of a sealed trait or class."
      )
    else if (!symbol.asClass.isSealed)
      c.abort(
        c.enclosingPosition,
        "Can only enumerate values of a sealed trait or class."
      )
    else
      c.Expr[List[A]](
        Apply(
          Select(
            reify(List).tree,
            TermName("apply")
          ),
          symbol.asClass.knownDirectSubclasses.toList.filter(_.isModuleClass) map {
            symbol =>
              Ident(
                symbol.asInstanceOf[scala.reflect.internal.Symbols#Symbol].sourceModule.asInstanceOf[Symbol]
              )
          }
        )
      )
  }
}