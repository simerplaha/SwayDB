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

import swaydb.core.data.KeyValue._
import swaydb.core.data.SegmentEntry.Range
import swaydb.data.slice.Slice

import scala.util.{Failure, Success, Try}

private[core] sealed trait KeyValue {
  def key: Slice[Byte]

  def id: Int

  def isRemove: Boolean

  def notRemove: Boolean = !isRemove

  def keyLength =
    key.size

  def getOrFetchValue: Try[Option[Slice[Byte]]]

  def toKeyValuePair: Try[Option[KeyValueInternal]] =
    getOrFetchValue map {
      valueBytes =>
        Some(
          key,
          if (isRemove) Value.Remove else Value.Put(valueBytes)
        )
    }

  def toTuple: Try[Option[KeyValueTuple]] =
    getOrFetchValue map {
      value =>
        Some(key, value)
    }
}

private[core] object KeyValue {

  trait ReadOnly extends KeyValue {
    val valueLength: Int
  }

  trait WriteOnly extends KeyValue {
    val stats: Stats

    def updateStats(falsePositiveRate: Double, keyValue: Option[KeyValue.WriteOnly]): KeyValue.WriteOnly

    val isRemoveRange: Boolean

    val isRange: Boolean
  }

  trait FixedWriteOnly extends KeyValue.WriteOnly

  implicit class FixedWriteOnlyImplicit(fixed: FixedWriteOnly) {
    def toValue: Try[Value.Fixed] =
      fixed match {
        case _: Transient.Remove =>
          Success(Value.Remove)
        case _: SegmentEntry.Remove =>
          Success(Value.Remove)
        case put: Transient.Put =>
          Success(Value.Put(put.value))
        case put: SegmentEntry.Put =>
          put.getOrFetchValue.map(Value.Put(_))
      }
  }

  trait RangeWriteOnly extends KeyValue.WriteOnly {
    def fromKey: Slice[Byte]

    def toKey: Slice[Byte]

    def fullKey: Slice[Byte]

    def fetchRangeValue: Try[Value.Fixed]

    def fetchFromValue: Try[Option[Value.Fixed]]

    def fetchFromAndRangeValue: Try[(Option[Value.Fixed], Value.Fixed)]

    val isRange: Boolean = true
  }

  implicit class RangeWriteOnlyImplicit(range: RangeWriteOnly) {
    def toValue: Try[(Slice[Byte], Value.Range)] =
      range match {
        case range: Range =>
          range.fetchFromAndRangeValue match {
            case Success((fromValue, rangeValue)) =>
              Success(range.fromKey, Value.Range(range.toKey, fromValue, rangeValue))

            case Failure(exception) =>
              Failure(exception)
          }
        case range: Transient.Range =>
          Success(range.fromKey, Value.Range(range.toKey, range.fromValue, range.rangeValue))
      }
  }

  type KeyValueTuple = (Slice[Byte], Option[Slice[Byte]])

  implicit class ToKeyValueTypeFromInternal(keyVal: KeyValueInternal) {
    def toKeyValueType: KeyValue = new KeyValue {
      override def isRemove: Boolean =
        keyVal._2.isRemove

      override def getOrFetchValue: Try[Option[Slice[Byte]]] =
        keyVal._2 match {
          case _: Value.Remove =>
            Success(None)
          case Value.Put(value) =>
            Success(value)
        }

      override def id: Int =
        keyVal._2.id

      override def key: Slice[Byte] =
        keyVal._1
    }
  }
  type KeyValueInternal = (Slice[Byte], Value)

}