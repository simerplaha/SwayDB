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

import swaydb.java.data.slice.Slice
import swaydb.{Apply, Map}

import scala.concurrent.duration.Deadline

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
  def id: Slice[java.lang.Byte]
}

/**
 * Function types for SwayDB.
 *
 * Your registered functions ([[Map.registerFunction]]) should implement one of the these functions that
 * informs SwayDB of target data for the on the applied key should be read to execute the function.
 */
object PureFunction {

  @FunctionalInterface
  trait GetValue[V] extends PureFunction[scala.Nothing, V] {
    def apply(value: V): Apply.Map[V]
  }

  @FunctionalInterface
  trait GetKey[K, V] extends PureFunction[K, V] {
    def apply(key: K, deadline: Option[Deadline]): Apply.Map[V]
  }

  @FunctionalInterface
  trait GetKeyValue[K, V] extends PureFunction[K, V] {
    def apply(key: K, value: V, deadline: Option[Deadline]): Apply.Map[V]
  }
}
