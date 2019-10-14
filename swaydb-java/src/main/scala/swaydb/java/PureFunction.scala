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
import swaydb.{Map, Apply => ScalaApply}

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

  protected[swaydb] def asScala: swaydb.PureFunction[K, V] =
    this match {
      case function: PureFunction.GetValue[V] =>
        function.asScala

      case function: PureFunction.GetKey[K, V] =>
        function.asScala

      case function: PureFunction.GetKeyValue[K, V] =>
        function.asScala
    }
}

/**
 * Function types for SwayDB.
 *
 * Your registered functions ([[Map.registerFunction]]) should implement one of the these functions that
 * informs SwayDB of target data for the on the applied key should be read to execute the function.
 */
object PureFunction {

  @FunctionalInterface
  trait GetValue[V] extends PureFunction[scala.Nothing, V] { self =>
    def apply(value: V): ScalaApply.Map[V]

    protected[swaydb] final override def asScala: swaydb.PureFunction.GetValue[V] =
      new swaydb.PureFunction.GetValue[V] {
        override def id: ScalaSlice[Byte] =
          self.id.asInstanceOf[ScalaSlice[Byte]]

        override def apply(value: V): ScalaApply.Map[V] =
          self.apply(value)
      }
  }

  @FunctionalInterface
  trait GetKey[K, V] extends PureFunction[K, V] { self =>
    def apply(key: K, deadline: Optional[swaydb.java.Deadline]): ScalaApply.Map[V]

    protected[swaydb] final override def asScala: swaydb.PureFunction.GetKey[K, V] =
      new swaydb.PureFunction.GetKey[K, V] {
        override def id: ScalaSlice[Byte] =
          self.id.asScala.asInstanceOf[ScalaSlice[Byte]]

        override def apply(key: K, deadline: Option[scala.concurrent.duration.Deadline]): ScalaApply.Map[V] =
          self.apply(key, deadline.map(_.asJava).asJava)
      }
  }

  @FunctionalInterface
  trait GetKeyValue[K, V] extends PureFunction[K, V] { self =>
    def apply(key: K, value: V, deadline: Optional[Deadline]): ScalaApply.Map[V]

    protected[swaydb] final override def asScala: swaydb.PureFunction[K, V] =
      new swaydb.PureFunction.GetKeyValue[K, V] {
        override def id: ScalaSlice[Byte] =
          self.id.asScala.asInstanceOf[ScalaSlice[Byte]]

        override def apply(key: K, value: V, deadline: Option[scala.concurrent.duration.Deadline]): ScalaApply.Map[V] =
          self.apply(key, value, deadline.map(_.asJava).asJava)
      }
  }
}
