/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.data

import swaydb.core.data.Persistent.Created
import swaydb.data.slice.Slice

import scala.util.{Success, Try}

private[core] sealed trait TransientType

private[core] sealed trait TransientReadOnly extends TransientType with KeyValueReadOnly {
  val value: Option[Slice[Byte]]
}

private[core] sealed trait Transient extends TransientType with KeyValue {
  val value: Option[Slice[Byte]]
}

private[core] object Transient {

  object Create {
    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]],
              falsePositiveRate: Double,
              previousMayBe: Option[KeyValue]): Create =
      new Create(key, value, Stats(key, value, false, falsePositiveRate, previousMayBe))
  }

  case class Create(key: Slice[Byte],
                    value: Option[Slice[Byte]],
                    stats: Stats) extends Transient {

    override def isDelete: Boolean = false

    override def id: Int = Created.id

    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue]): KeyValue =
      this.copy(stats = Stats(key, value, false, falsePositiveRate, keyValue))

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = Success(value)
  }

  object Delete {
    val id: Int = 0

    def apply(key: Slice[Byte]): Delete =
      new Delete(key, Stats(key, None, true, 0.1, None))

    def apply(key: Slice[Byte], falsePositiveRate: Double): Delete =
      new Delete(key, Stats(key, None, true, falsePositiveRate, None))

    def apply(key: Slice[Byte],
              falsePositiveRate: Double,
              previous: Option[KeyValue]): Delete =
      new Delete(key, Stats(key, None, true, falsePositiveRate, previous))
  }

  case class Delete(key: Slice[Byte],
                    stats: Stats) extends Transient {
    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue]): KeyValue =
      this.copy(stats = Stats(key, None, true, falsePositiveRate, keyValue))

    override def id: Int = Delete.id

    override def isDelete: Boolean =
      true

    override val value: Option[Slice[Byte]] = None

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = Success(None)
  }

}