/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
