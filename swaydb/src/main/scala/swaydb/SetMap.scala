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

package swaydb

import java.nio.file.Path

import swaydb.core.util.Bytes
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

import scala.collection.mutable
import scala.concurrent.duration.{Deadline, DurationInt, FiniteDuration}

object SetMap {
  /**
   * Combines two serialisers into a single Serialiser.
   */
  def serialiser[A, B](aSerializer: Serializer[A],
                       bSerializer: Serializer[B]): Serializer[(A, B)] =
    new Serializer[(A, B)] {
      override def write(data: (A, B)): Slice[Byte] = {
        val keyBytes = aSerializer.write(data._1)

        val valueBytes =
          if (data._2 == null)
            Slice.emptyBytes //value can be null when
          else
            bSerializer.write(data._2)

        Slice
          .create[Byte](Bytes.sizeOfUnsignedInt(keyBytes.size) + keyBytes.size + Bytes.sizeOfUnsignedInt(valueBytes.size) + valueBytes.size)
          .addUnsignedInt(keyBytes.size)
          .addAll(keyBytes)
          .addUnsignedInt(valueBytes.size)
          .addAll(valueBytes)
      }

      override def read(data: Slice[Byte]): (A, B) = {
        val reader = data.createReader()

        val keyBytes = reader.read(reader.readUnsignedInt())
        val valuesBytes = reader.read(reader.readUnsignedInt())

        val key = aSerializer.read(keyBytes)
        val value = bSerializer.read(valuesBytes)
        (key, value)
      }
    }

  /**
   * Partial ordering based on [[SetMap.serialiser]].
   */
  def ordering[K](defaultOrdering: Either[KeyOrder[Slice[Byte]], KeyOrder[K]])(implicit keySerializer: Serializer[K]): KeyOrder[Slice[Byte]] =
    defaultOrdering match {
      case Left(untypedOrdering) =>
        new KeyOrder[Slice[Byte]] {
          override def compare(left: Slice[Byte], right: Slice[Byte]): Int = {
            val readerLeft = left.createReader()
            val readerRight = right.createReader()

            val leftKey = readerLeft.read(readerLeft.readUnsignedInt())
            val rightKey = readerRight.read(readerRight.readUnsignedInt())

            untypedOrdering.compare(leftKey, rightKey)
          }

          private[swaydb] override def comparableKey(key: Slice[Byte]): Slice[Byte] = {
            val reader = key.createReader()
            reader.read(reader.readUnsignedInt())
          }
        }

      case Right(typedOrdering) =>
        new KeyOrder[Slice[Byte]] {
          override def compare(left: Slice[Byte], right: Slice[Byte]): Int = {
            val readerLeft = left.createReader()
            val readerRight = right.createReader()

            val leftUntypedKey = readerLeft.read(readerLeft.readUnsignedInt())
            val rightUntypedKey = readerRight.read(readerRight.readUnsignedInt())

            val leftTypedKey = keySerializer.read(leftUntypedKey)
            val rightTypedKey = keySerializer.read(rightUntypedKey)

            typedOrdering.compare(leftTypedKey, rightTypedKey)
          }

          private[swaydb] override def comparableKey(key: Slice[Byte]): Slice[Byte] = {
            val reader = key.createReader()
            reader.read(reader.readUnsignedInt())
          }
        }
    }
}

/**
 * A [[SetMap]] is a simple wrapper around [[Set]] to provide
 * [[Map]] like API on [[Set]] storage format.
 *
 * [[SetMap]] has limited write APIs as compared to [[swaydb.Map]]
 * and range & update operations are not supported.
 */
case class SetMap[K, V, F, BAG[_]] private(set: Set[(K, V), F, BAG])(implicit bag: Bag[BAG]) extends SetMapT[K, V, F, BAG] { self =>

  private final val nullValue: V = null.asInstanceOf[V]

  def path: Path =
    set.path

  def put(key: K, value: V): BAG[OK] =
    set.add((key, value))

  def put(key: K, value: V, expireAfter: FiniteDuration): BAG[OK] =
    set.add((key, value), expireAfter.fromNow)

  def put(key: K, value: V, expireAt: Deadline): BAG[OK] =
    set.add((key, value), expireAt)

  def put(keyValues: (K, V)*): BAG[OK] =
    put(keyValues)

  def put(keyValues: Stream[(K, V)]): BAG[OK] =
    set.add(keyValues)

  def put(keyValues: Iterable[(K, V)]): BAG[OK] =
    set.add(keyValues)

  def put(keyValues: Iterator[(K, V)]): BAG[OK] =
    set.add(keyValues)

  def remove(key: K): BAG[OK] =
    set.remove((key, nullValue))

  def remove(keys: K*): BAG[OK] =
    set.remove(keys.map(key => (key, nullValue)))

  def remove(keys: Stream[K]): BAG[OK] =
    set.remove(keys.map((_, nullValue)))

  def remove(keys: Iterable[K]): BAG[OK] =
    remove(keys.iterator)

  def remove(keys: Iterator[K]): BAG[OK] =
    set.remove(keys.map((_, nullValue)))

  def expire(key: K, after: FiniteDuration): BAG[OK] =
    set.expire((key, nullValue), after)

  def expire(key: K, at: Deadline): BAG[OK] =
    set.expire((key, nullValue), at)

  def clearKeyValues(): BAG[OK] =
    set.clear()

  /**
   * Returns target value for the input key.
   */
  def get(key: K): BAG[Option[V]] =
    bag.map(set.get((key, nullValue)))(_.map(_._2))

  /**
   * Returns target full key for the input partial key.
   *
   * This function is mostly used for Set databases where partial ordering on the Key is provided.
   */
  def getKey(key: K): BAG[Option[K]] =
    bag.map(set.get((key, nullValue)))(_.map(_._1))

  def getKeyValue(key: K): BAG[Option[(K, V)]] =
    set.get((key, nullValue))

  def contains(key: K): BAG[Boolean] =
    set.contains((key, nullValue))

  def mightContain(key: K): BAG[Boolean] =
    set.mightContain((key, nullValue))

  def levelZeroMeter: LevelZeroMeter =
    set.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    set.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    set.sizeOfSegments

  def expiration(key: K): BAG[Option[Deadline]] =
    set.expiration((key, nullValue))

  def timeLeft(key: K): BAG[Option[FiniteDuration]] =
    bag.map(expiration(key))(_.map(_.timeLeft))

  def from(key: K): SetMap[K, V, F, BAG] =
    SetMap(set = set.from((key, nullValue)))

  def before(key: K): SetMap[K, V, F, BAG] =
    SetMap(set = set.before((key, nullValue)))

  def fromOrBefore(key: K): SetMap[K, V, F, BAG] =
    SetMap(set = set.fromOrBefore((key, nullValue)))

  def after(key: K): SetMap[K, V, F, BAG] =
    SetMap(set = set.after((key, nullValue)))

  def fromOrAfter(key: K): SetMap[K, V, F, BAG] =
    SetMap(set = set.fromOrAfter((key, nullValue)))

  def headOption: BAG[Option[(K, V)]] =
    set.headOption

  def headOrNull: BAG[(K, V)] =
    set.headOrNull

  def stream: Stream[(K, V)] =
    set.stream

  def iterator[BAG[_]](implicit bag: Bag.Sync[BAG]): Iterator[BAG[(K, V)]] =
    set.iterator

  def sizeOfBloomFilterEntries: BAG[Int] =
    set.sizeOfBloomFilterEntries

  def isEmpty: BAG[Boolean] =
    set.isEmpty

  def nonEmpty: BAG[Boolean] =
    set.nonEmpty

  def lastOption: BAG[Option[(K, V)]] =
    set.lastOption

  def reverse: SetMap[K, V, F, BAG] =
    SetMap(set.reverse)

  private def copy(): Unit = ()

  override def asScala: mutable.Map[K, V] =
    ScalaMap[K, V, F](toBag[Bag.Less](Bag.less))

  override private[swaydb] def keySet: mutable.Set[K] =
    ScalaSet[K, V, F](toBag[Bag.Less](Bag.less), nullValue)

  /**
   * Returns an Async API of type O where the [[Bag]] is known.
   */
  def toBag[X[_]](implicit bag: Bag[X]): SetMap[K, V, F, X] =
    SetMap(set = set.toBag[X])

  def close(): BAG[Unit] =
    set.close()

  def delete(): BAG[Unit] =
    set.delete()

  override def toString(): String =
    classOf[SetMap[_, _, _, BAG]].getSimpleName
}
