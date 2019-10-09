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

package swaydb.java.data.util

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.Deadline

object KeyVal {
  def apply[K, V](keyVal: (K, V)): KeyVal[K, V] = new KeyVal(keyVal._1, keyVal._2)

  implicit class KeyValueImplicit[K](keyVal: KeyVal[K, java.time.Duration]) {
    def toScala: (K, Deadline) =
      (keyVal.key, keyVal.value.toScala.fromNow)
  }
}

case class KeyVal[+K, +V](key: K, value: V) {
  def toTuple: (K, V) =
    (key, value)
}
