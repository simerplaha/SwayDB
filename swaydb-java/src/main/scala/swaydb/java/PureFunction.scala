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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.java

import java.util.Optional

import swaydb.Apply
import swaydb.data.util.Java._
import swaydb.java.data.util.Java._

import scala.concurrent.duration

trait PureFunction[+K, +V, +R <: Return[V]] {
  /**
   * This unique [[id]] of this function.
   *
   * It is stored in the database and should be unique to each function.
   *
   * This can simply be the full class name if your application does not
   * have conflict package names and class names.
   *
   * NOTE - Missing functions are reported/logged along with this [[id]] to
   * make debugging easier.
   *
   * @return a unique id for each function.
   */
  def id: String =
    this.getClass.getName
}

/**
 * Function types for SwayDB.
 *
 * Your registered functions should implement one of the these functions that
 * informs SwayDB of target data for the on the applied key should be read to execute the function.
 */
object PureFunction {

  def asScala[K, V, R <: Return.Map[V]](function: PureFunction[K, V, R]): swaydb.PureFunction[K, V, Apply.Map[V]] =
    function match {
      case function: PureFunction.OnValue[K, V, Return.Map[V]] =>
        new swaydb.PureFunction.OnValue[V, Apply.Map[V]] {
          override def id: String =
            function.id

          override def apply(value: V): Apply.Map[V] =
            Return.toScalaMap(function.apply(value))
        }

      case function: PureFunction.OnKey[K, V, Return.Map[V]] =>
        new swaydb.PureFunction.OnKey[K, V, Apply.Map[V]] {
          override def id: String =
            function.id

          override def apply(key: K, deadline: Option[scala.concurrent.duration.Deadline]): Apply.Map[V] =
            Return.toScalaMap(function.apply(key, deadline.asJavaMap(_.asJava)))
        }

      case function: PureFunction.OnKeyValue[K, V, Return.Map[V]] =>
        new swaydb.PureFunction.OnKeyValue[K, V, Apply.Map[V]] {
          override def id: String =
            function.id

          override def apply(key: K, value: V, deadline: Option[scala.concurrent.duration.Deadline]): Apply.Map[V] =
            Return.toScalaMap(function.apply(key, value, deadline.asJavaMap(_.asJava)))
        }
    }

  def asScala[K, R <: Return.Set[Void]](function: PureFunction.OnKey[K, Void, R]): swaydb.PureFunction.OnKey[K, Nothing, Apply.Set] =
    new swaydb.PureFunction.OnKey[K, Nothing, Apply.Set] {
      override def id: String =
        function.id

      override def apply(key: K, deadline: Option[duration.Deadline]): Apply.Set =
        Return.toScalaSet(function.apply(key, deadline.asJavaMap(_.asJava)))
    }

  @FunctionalInterface
  trait OnValue[K, V, R <: Return[V]] extends PureFunction[K, V, R] { self =>
    def apply(value: V): R
  }

  @FunctionalInterface
  trait OnKey[K, V, R <: Return[V]] extends PureFunction[K, V, R] { self =>
    def apply(key: K, deadline: Optional[swaydb.java.Deadline]): R
  }

  @FunctionalInterface
  trait OnKeyValue[K, V, R <: Return[V]] extends PureFunction[K, V, R] { self =>
    def apply(key: K, value: V, deadline: Optional[Deadline]): R
  }
}
