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

package swaydb

import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

sealed trait Function[+K, +V] {
  /**
   * This unique id is stored in the database when an update is submitted.
   *
   * This can simply be an increment integer. For example:
   * {{{
   *   val id = new AtomicInteger(0)
   *   Slice.writeInt(id.incrementAndGet())
   * }}}
   *
   * @return a unique id for each function.
   */
  def id: Slice[Byte]
}

object Function {

  trait GetValue[V] extends (V => Apply.Map[V]) with Function[Nothing, V] {
    override def apply(value: V): Apply.Map[V]
  }

  trait GetKey[K, +V] extends ((K, Option[Deadline]) => Apply.Map[V]) with Function[K, V] {
    override def apply(key: K, deadline: Option[Deadline]): Apply.Map[V]
  }

  trait GetKeyValue[K, V] extends ((K, V, Option[Deadline]) => Apply.Map[V]) with Function[K, V] {
    override def apply(key: K, value: V, deadline: Option[Deadline]): Apply.Map[V]
  }
}