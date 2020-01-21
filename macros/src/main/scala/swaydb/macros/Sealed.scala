/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.macros

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

object Sealed {

  def list[A]: List[A] = macro list_impl[A]

  def array[A]: Array[A] = macro array_impl[A]

  def list_impl[A: c.WeakTypeTag](c: blackbox.Context): c.Expr[List[A]] = {
    import c.universe._

    val symbol = weakTypeOf[A].typeSymbol

    if (!symbol.isClass || !symbol.asClass.isSealed)
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

  def array_impl[A: c.WeakTypeTag](c: blackbox.Context): c.Expr[Array[A]] = {
    import c.universe._

    val symbol = weakTypeOf[A].typeSymbol

    if (!symbol.isClass || !symbol.asClass.isSealed)
      c.abort(
        c.enclosingPosition,
        "Can only enumerate values of a sealed trait or class."
      )
    else
      c.Expr[Array[A]](
        Apply(
          Select(
            reify(Array).tree,
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
