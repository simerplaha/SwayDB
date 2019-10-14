/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.java

import java.util.Optional

import swaydb.java.data.slice.Slice
import swaydb.java.data.util.Java._
import swaydb.{Map, PureFunction, Apply => ScalaApply}

import scala.compat.java8.OptionConverters._

sealed trait PureFunction[+K, +V] {
  /**
   * This unique [[id]] of this function.
   *
   * It is stored in the database and should be unique to each function.
   *
   * This can simply be the full class name if your application does not
   * have conflict package names and class names.
   *
   * For example:
   * {{{
   *   Slice.writeString(this.getClass.getCanonicalName)
   * }}}
   *
   * @return a unique id for each function.
   */
  def id: Slice[java.lang.Byte] =
    Slice.writeString(this.getClass.getName)
}

/**
 * Function types for SwayDB.
 *
 * Your registered functions ([[Map.registerFunction]]) should implement one of the these functions that
 * informs SwayDB of target data for the on the applied key should be read to execute the function.
 */
object PureFunction {
  def asScala[K, V](function: PureFunction[K, V]): swaydb.PureFunction[K, V] =
    function match {
      case function: PureFunction.OnValue[V] =>
        new swaydb.PureFunction.OnValue[V] {
          override def id: ScalaSlice[Byte] =
            function.id.asInstanceOf[ScalaSlice[Byte]]

          override def apply(value: V): ScalaApply.Map[V] =
            function.apply(value)
        }

      case function: PureFunction.OnKey[K, V] =>
        new swaydb.PureFunction.OnKey[K, V] {
          override def id: ScalaSlice[Byte] =
            function.id.asScala.asInstanceOf[ScalaSlice[Byte]]

          override def apply(key: K, deadline: Option[scala.concurrent.duration.Deadline]): ScalaApply.Map[V] =
            function.apply(key, deadline.map(_.asJava).asJava)
        }

      case function: PureFunction.OnKeyValue[K, V] =>
        new swaydb.PureFunction.OnKeyValue[K, V] {
          override def id: ScalaSlice[Byte] =
            function.id.asScala.asInstanceOf[ScalaSlice[Byte]]

          override def apply(key: K, value: V, deadline: Option[scala.concurrent.duration.Deadline]): ScalaApply.Map[V] =
            function.apply(key, value, deadline.map(_.asJava).asJava)
        }
    }

  def asScala[K](function: PureFunction.OnKey[K, java.lang.Void]): swaydb.PureFunction.OnKey[K, Nothing] =
    new swaydb.PureFunction.OnKey[K, Nothing] {
      override def id: ScalaSlice[Byte] =
        function.id.asScala.asInstanceOf[ScalaSlice[Byte]]

      override def apply(key: K, deadline: Option[scala.concurrent.duration.Deadline]): ScalaApply.Map[Nothing] =
        function.apply(key, deadline.map(_.asJava).asJava).asInstanceOf[ScalaApply.Map[Nothing]]
    }

  @FunctionalInterface
  trait OnValue[V] extends PureFunction[scala.Nothing, V] { self =>
    def apply(value: V): ScalaApply.Map[V]
  }

  @FunctionalInterface
  trait OnKey[K, V] extends PureFunction[K, V] { self =>
    def apply(key: K, deadline: Optional[swaydb.java.Deadline]): ScalaApply.Map[V]
  }

  @FunctionalInterface
  trait OnKeyValue[K, V] extends PureFunction[K, V] { self =>
    def apply(key: K, value: V, deadline: Optional[Deadline]): ScalaApply.Map[V]
  }
}
