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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb

import java.nio.file.Path

import swaydb.PrepareImplicits._
import swaydb.core.Core
import swaydb.core.segment.ThreadReadState
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.stream.{From, SourceFree}
import swaydb.data.util.TupleOrNone
import swaydb.serializers.{Serializer, _}

import scala.collection.mutable
import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * Map database API.
 *
 * For documentation check - http://swaydb.io/
 */
case class Map[K, V, F, BAG[_]] private(private val core: Core[BAG])(implicit val keySerializer: Serializer[K],
                                                                     val valueSerializer: Serializer[V],
                                                                     override val bag: Bag[BAG]) extends MapT[K, V, F, BAG] { self =>

  def path: Path =
    core.zeroPath.getParent

  def put(key: K, value: V): BAG[OK] =
    bag.suspend(core.put(key = key, value = value))

  def put(key: K, value: V, expireAfter: FiniteDuration): BAG[OK] =
    bag.suspend(core.put(key, value, expireAfter.fromNow))

  def put(key: K, value: V, expireAt: Deadline): BAG[OK] =
    bag.suspend(core.put(key, value, expireAt))

  def put(keyValues: (K, V)*): BAG[OK] =
    bag.suspend(put(keyValues))

  def put(keyValues: Stream[(K, V), BAG]): BAG[OK] =
    bag.flatMap(keyValues.materialize)(put)

  def put(keyValues: Iterable[(K, V)]): BAG[OK] =
    put(keyValues.iterator)

  def put(keyValues: Iterator[(K, V)]): BAG[OK] =
    bag.suspend {
      core.commit {
        keyValues map {
          case (key, value) =>
            Prepare.Put(keySerializer.write(key), valueSerializer.write(value), None)
        }
      }
    }

  def remove(key: K): BAG[OK] =
    bag.suspend(core.remove(key))

  def remove(from: K, to: K): BAG[OK] =
    bag.suspend(core.remove(from, to))

  def remove(keys: K*): BAG[OK] =
    bag.suspend(remove(keys))

  def remove(keys: Stream[K, BAG]): BAG[OK] =
    bag.flatMap(keys.materialize)(remove)

  def remove(keys: Iterable[K]): BAG[OK] =
    remove(keys.iterator)

  def remove(keys: Iterator[K]): BAG[OK] =
    bag.suspend(core.commit(keys.map(key => Prepare.Remove(keySerializer.write(key)))))

  def expire(key: K, after: FiniteDuration): BAG[OK] =
    bag.suspend(core.expire(key, after.fromNow))

  def expire(key: K, at: Deadline): BAG[OK] =
    bag.suspend(core.expire(key, at))

  def expire(from: K, to: K, after: FiniteDuration): BAG[OK] =
    bag.suspend(core.expire(from, to, after.fromNow))

  def expire(from: K, to: K, at: Deadline): BAG[OK] =
    bag.suspend(core.expire(from, to, at))

  def expire(keys: (K, Deadline)*): BAG[OK] =
    bag.suspend(expire(keys))

  def expire(keys: Stream[(K, Deadline), BAG]): BAG[OK] =
    bag.flatMap(keys.materialize)(expire)

  def expire(keys: Iterable[(K, Deadline)]): BAG[OK] =
    expire(keys.iterator)

  def expire(keys: Iterator[(K, Deadline)]): BAG[OK] =
    bag.suspend {
      core.commit {
        keys map {
          keyDeadline =>
            Prepare.Remove(
              from = keySerializer.write(keyDeadline._1),
              to = None,
              deadline = Some(keyDeadline._2)
            )
        }
      }
    }

  def update(key: K, value: V): BAG[OK] =
    bag.suspend(core.update(key, value))

  def update(from: K, to: K, value: V): BAG[OK] =
    bag.suspend(core.update(from, to, value))

  def update(keyValues: (K, V)*): BAG[OK] =
    bag.suspend(update(keyValues))

  def update(keyValues: Stream[(K, V), BAG]): BAG[OK] =
    bag.flatMap(keyValues.materialize)(update)

  def update(keyValues: Iterable[(K, V)]): BAG[OK] =
    update(keyValues.iterator)

  def update(keyValues: Iterator[(K, V)]): BAG[OK] =
    bag.suspend {
      core.commit {
        keyValues map {
          case (key, value) =>
            Prepare.Update(keySerializer.write(key), valueSerializer.write(value))
        }
      }
    }

  def clearKeyValues(): BAG[OK] =
    bag.suspend(core.clear(core.readStates.get()))

  def applyFunction(key: K, function: F)(implicit evd: F <:< PureFunction.Map[K, V]): BAG[OK] =
    bag.suspend(core.applyFunction(key, Slice.writeString[Byte](function.id)))

  def applyFunction(from: K, to: K, function: F)(implicit evd: F <:< PureFunction.Map[K, V]): BAG[OK] =
    bag.suspend(core.applyFunction(from, to, Slice.writeString[Byte](function.id)))

  def commit(prepare: Prepare[K, V, F]*): BAG[OK] =
    bag.suspend(core.commit(preparesToUntyped(prepare).iterator))

  def commit(prepare: Stream[Prepare[K, V, F], BAG]): BAG[OK] =
    bag.flatMap(prepare.materialize) {
      prepares =>
        commit(prepares)
    }

  def commit(prepare: Iterable[Prepare[K, V, F]]): BAG[OK] =
    bag.suspend(core.commit(preparesToUntyped(prepare).iterator))

  def commit(prepare: Iterator[Prepare[K, V, F]]): BAG[OK] =
    bag.suspend(core.commit(preparesToUntyped(prepare)))

  /**
   * Returns target value for the input key.
   */
  def get(key: K): BAG[Option[V]] =
    bag.map(core.get(key, core.readStates.get()))(_.map(_.read[V]))

  /**
   * Returns target full key for the input partial key.
   *
   * This function is mostly used for Set databases where partial ordering on the Key is provided.
   */
  def getKey(key: K): BAG[Option[K]] =
    bag.map(core.getKey(key, core.readStates.get()))(_.mapC(_.read[K]))

  def getKeyValue(key: K): BAG[Option[(K, V)]] =
    bag.map(core.getKeyValue(key, core.readStates.get()))(_.mapC {
      keyValue =>
        (keyValue.left.read[K], keyValue.right.read[V])
    })

  def getKeyDeadline(key: K): BAG[Option[(K, Option[Deadline])]] =
    bag.flatMap(core.getKeyDeadline[BAG](key, core.readStates.get())(bag)) {
      case TupleOrNone.None =>
        bag.none[(K, Option[Deadline])]

      case TupleOrNone.Some(left, right) =>
        bag.success(Some((left.read[K], right)))
    }

  override def getKeyValueDeadline(key: K): BAG[Option[((K, V), Option[Deadline])]] =
    bag.flatMap(core.getKeyValueDeadline[BAG](key, core.readStates.get())(bag)) {
      case TupleOrNone.None =>
        bag.none[((K, V), Option[Deadline])]

      case TupleOrNone.Some((key, value), deadline) =>
        bag.success(Some(((key.read[K], value.read[V]), deadline)))
    }

  def contains(key: K): BAG[Boolean] =
    bag.suspend(core.contains(key, core.readStates.get()))

  def mightContain(key: K): BAG[Boolean] =
    bag.suspend(core mightContainKey key)

  def mightContainFunction(function: F)(implicit evd: F <:< PureFunction.Map[K, V]): BAG[Boolean] =
    bag.suspend(core mightContainFunction Slice.writeString[Byte](function.id))

  def keys: Stream[K, BAG] =
    map(_._1)

  def values: Stream[V, BAG] =
    map(_._2)

  def toSet: Set[K, Nothing, BAG] =
    Set[K, Nothing, BAG](core)

  private[swaydb] def keySet: mutable.Set[K] =
    toSet.asScala

  def levelZeroMeter: LevelZeroMeter =
    core.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    core.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    core.sizeOfSegments

  def keySize(key: K): Int =
    (key: Slice[Byte]).size

  def valueSize(value: V): Int =
    (value: Slice[Byte]).size

  def expiration(key: K): BAG[Option[Deadline]] =
    bag.suspend(core.deadline(key, core.readStates.get()))

  def timeLeft(key: K): BAG[Option[FiniteDuration]] =
    bag.map(expiration(key))(_.map(_.timeLeft))

  override def head: BAG[Option[(K, V)]] =
    bag.transform(
      headOrNull(
        from = None,
        reverseIteration = false,
        readState = core.readStates.get())
    )(Option(_))

  private def headOrNull[BAG[_]](from: Option[From[K]],
                                 reverseIteration: Boolean,
                                 readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[(K, V)] =
    bag.map(headOptionTupleOrNull(from, reverseIteration, readState)) {
      case TupleOrNone.None =>
        null

      case TupleOrNone.Some(left, right) =>
        val key = left.read[K]
        val value = right.read[V]
        (key, value)
    }

  private def headOptionTupleOrNull[BAG[_]](from: Option[From[K]],
                                            reverseIteration: Boolean,
                                            readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[Slice[Byte], SliceOption[Byte]]] =
    from match {
      case Some(from) =>
        val fromKeyBytes: Slice[Byte] = from.key

        if (from.before)
          core.before(fromKeyBytes, readState)
        else if (from.after)
          core.after(fromKeyBytes, readState)
        else
          bag.flatMap(core.getKeyValue(fromKeyBytes, readState)) {
            case some @ TupleOrNone.Some(_, _) =>
              bag.success(some)

            case TupleOrNone.None =>
              if (from.orAfter)
                core.after(fromKeyBytes, readState)
              else if (from.orBefore)
                core.before(fromKeyBytes, readState)
              else
                bag.success(TupleOrNone.None)
          }

      case None =>
        if (reverseIteration)
          core.last(readState)
        else
          core.head(readState)
    }

  private def nextTupleOrNone[BAG[_]](previousKey: K,
                                      reverseIteration: Boolean,
                                      readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[TupleOrNone[Slice[Byte], SliceOption[Byte]]] =
    if (reverseIteration)
      core.before(keySerializer.write(previousKey), readState)
    else
      core.after(keySerializer.write(previousKey), readState)

  override private[swaydb] def free: SourceFree[K, (K, V)] =
    new SourceFree[K, (K, V)](from = None, reverse = false) {
      val readState = core.readStates.get()

      override private[swaydb] def headOrNull[BAG[_]](from: Option[From[K]], reverse: Boolean)(implicit bag: Bag[BAG]) =
        self.headOrNull(
          from = from,
          reverseIteration = reverse,
          readState = readState
        )

      override private[swaydb] def nextOrNull[BAG[_]](previous: (K, V), reverse: Boolean)(implicit bag: Bag[BAG]) =
        bag.map(nextTupleOrNone(previousKey = previous._1, reverseIteration = reverse, readState = readState)) {
          case TupleOrNone.None =>
            null

          case TupleOrNone.Some(left, right) =>
            val key = left.read[K]
            val value = right.read[V]
            (key, value)
        }
    }

  def sizeOfBloomFilterEntries: BAG[Int] =
    bag.suspend(core.bloomFilterKeyValueCount)

  def isEmpty: BAG[Boolean] =
    bag.transform(core.headKey(core.readStates.get()))(_.isNoneC)

  def nonEmpty: BAG[Boolean] =
    bag.transform(isEmpty)(!_)

  override def last: BAG[Option[(K, V)]] =
    bag.map(core.last(core.readStates.get())) {
      case TupleOrNone.Some(key, value) =>
        Some((key.read[K], value.read[V]))

      case TupleOrNone.None =>
        None
    }

  override def clearAppliedFunctions(): BAG[Iterable[String]] =
    bag.suspend(core.clearAppliedFunctions())

  override def clearAppliedAndRegisteredFunctions(): BAG[Iterable[String]] =
    bag.suspend(core.clearAppliedAndRegisteredFunctions())

  override def isFunctionApplied(function: F)(implicit evd: F <:< PureFunction.Map[K, V]): Boolean =
    core.isFunctionApplied(Slice.writeString[Byte](function.asInstanceOf[PureFunction.Map[K, V]].id))

  /**
   * Returns an Async API of type O where the [[Bag]] is known.
   */
  override def toBag[X[_]](implicit bag: Bag[X]): Map[K, V, F, X] =
    copy(core = core.toBag[X])

  def asScala: scala.collection.mutable.Map[K, V] =
    ScalaMap[K, V](toBag[Glass](Bag.glass))

  def close(): BAG[Unit] =
    bag.suspend(core.close())

  def delete(): BAG[Unit] =
    bag.suspend(core.delete())

  /**
   * The private modifier restricts access in Scala.
   * But does not stop access from Java.
   *
   * Do not access this Actor.
   *
   * TODO - Make private.
   */
  private[swaydb] def protectedSweeper =
    core.bufferSweeper

  override def equals(other: Any): Boolean =
    other match {
      case other: Map[_, _, _, _] =>
        other.path == this.path

      case _ =>
        false
    }

  override def hashCode(): Int =
    path.hashCode()

  override def toString(): String =
    s"Map(path = $path)"
}
