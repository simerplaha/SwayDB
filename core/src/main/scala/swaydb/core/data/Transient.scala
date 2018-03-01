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

  object Put {
    val id = 1

    def apply(key: Slice[Byte],
              value: Option[Slice[Byte]],
              falsePositiveRate: Double,
              previousMayBe: Option[KeyValue]): Put =
      new Put(key, value, Stats(key, value, false, falsePositiveRate, previousMayBe))

    def apply(key: Slice[Byte]): Put =
      Put(key, None, Stats(key, isDelete = false, falsePositiveRate = 0.1))

    def apply(key: Slice[Byte], value: Slice[Byte]): Put =
      Put(key, Some(value), Stats(key, value, isDelete = false, falsePositiveRate = 0.1))

    def apply(key: Slice[Byte], value: Slice[Byte], falsePositiveRate: Double): Put =
      Put(key, Some(value), Stats(key, value, isDelete = false, falsePositiveRate = falsePositiveRate))

    def apply(key: Slice[Byte], falsePositiveRate: Double, previous: Option[KeyValue]): Put =
      Put(key, None, falsePositiveRate, previous)

    def apply(key: Slice[Byte], value: Slice[Byte], falsePositiveRate: Double, previous: Option[KeyValue]): Put =
      Put(key, Some(value), Stats(key, value, isDelete = false, falsePositiveRate = falsePositiveRate, previous))
  }

  case class Put(key: Slice[Byte],
                 value: Option[Slice[Byte]],
                 stats: Stats) extends Transient {

    override def isRemove: Boolean = false

    override def id: Int = Put.id

    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue]): KeyValue =
      this.copy(stats = Stats(key, value, false, falsePositiveRate, keyValue))

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = Success(value)
  }

  object Remove {
    val id: Int = 0

    def apply(key: Slice[Byte]): Remove =
      new Remove(key, Stats(key, None, true, 0.1, None))

    def apply(key: Slice[Byte], falsePositiveRate: Double): Remove =
      new Remove(key, Stats(key, None, true, falsePositiveRate, None))

    def apply(key: Slice[Byte],
              falsePositiveRate: Double,
              previous: Option[KeyValue]): Remove =
      new Remove(key, Stats(key, None, true, falsePositiveRate, previous))
  }

  case class Remove(key: Slice[Byte],
                    stats: Stats) extends Transient {
    override def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue]): KeyValue =
      this.copy(stats = Stats(key, None, true, falsePositiveRate, keyValue))

    override def id: Int = Remove.id

    override def isRemove: Boolean =
      true

    override val value: Option[Slice[Byte]] = None

    override def getOrFetchValue: Try[Option[Slice[Byte]]] = Success(None)
  }

}