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

import swaydb.PrepareImplicits._
import swaydb.core.Core
import swaydb.core.function.{FunctionStore => CoreFunctionStore}
import swaydb.core.segment.ThreadReadState
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.stream.{From, SourceFree}
import swaydb.data.util.TupleOrNone
import swaydb.serializers.{Serializer, _}

import scala.collection.mutable
import scala.concurrent.duration.{Deadline, FiniteDuration}

object Map {

  implicit def nothing[K, V]: Functions[K, V, Nothing] =
    new Functions[K, V, Nothing]()(null, null)

  implicit def void[K, V]: Functions[K, V, Void] =
    new Functions[K, V, Void]()(null, null)

  object Functions {
    def apply[K, V, F](functions: F*)(implicit keySerializer: Serializer[K],
                                      valueSerializer: Serializer[V],
                                      ev: F <:< swaydb.PureFunction[K, V, Apply.Map[V]]) = {
      val f = new Functions[K, V, F]()
      functions.foreach(f.register(_))
      f
    }

    def apply[K, V, F](functions: Iterable[F])(implicit keySerializer: Serializer[K],
                                               valueSerializer: Serializer[V],
                                               ev: F <:< swaydb.PureFunction[K, V, Apply.Map[V]]) = {
      val f = new Functions[K, V, F]()
      functions.foreach(f.register(_))
      f
    }
  }

  final case class Functions[K, V, F]()(implicit keySerializer: Serializer[K],
                                        valueSerializer: Serializer[V]) {

    private[swaydb] val core = CoreFunctionStore.memory()

    def register[PF <: F](functions: PF*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Unit =
      functions.foreach(register(_))

    def register[PF <: F](function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Unit =
      (function: swaydb.PureFunction[K, V, Apply.Map[V]]) match {
        case function: swaydb.PureFunction.OnValue[V, Apply.Map[V]] =>
          core.put(Slice.writeString(function.id), SwayDB.toCoreFunction(function))

        case function: swaydb.PureFunction.OnKey[K, V, Apply.Map[V]] =>
          core.put(Slice.writeString(function.id), SwayDB.toCoreFunction(function))

        case function: swaydb.PureFunction.OnKeyValue[K, V, Apply.Map[V]] =>
          core.put(Slice.writeString(function.id), SwayDB.toCoreFunction(function))
      }
  }
}

/**
 * Map database API.
 *
 * For documentation check - http://swaydb.io/
 */
case class Map[K, V, F, BAG[_]] private(private[swaydb] val core: Core[BAG])(implicit val keySerializer: Serializer[K],
                                                                             val valueSerializer: Serializer[V],
                                                                             val bag: Bag[BAG]) extends MapT[K, V, F, BAG] { self =>

  def path: Path =
    core.zero.path.getParent

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

  def applyFunction[PF <: F](key: K, function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    bag.suspend(core.applyFunction(key, Slice.writeString(function.id)))

  def applyFunction[PF <: F](from: K, to: K, function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    bag.suspend(core.applyFunction(from, to, Slice.writeString(function.id)))

  def commit[PF <: F](prepare: Prepare[K, V, PF]*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    bag.suspend(core.commit(preparesToUntyped(prepare).iterator))

  def commit[PF <: F](prepare: Stream[Prepare[K, V, PF], BAG])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    bag.flatMap(prepare.materialize) {
      prepares =>
        commit(prepares)
    }

  def commit[PF <: F](prepare: Iterable[Prepare[K, V, PF]])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    bag.suspend(core.commit(preparesToUntyped(prepare).iterator))

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
    getKeyDeadline(key, bag)

  def getKeyDeadline[BAG[_]](key: K, bag: Bag[BAG]): BAG[Option[(K, Option[Deadline])]] =
    bag.flatMap(core.getKeyDeadline[BAG](key, core.readStates.get())(bag)) {
      case TupleOrNone.None =>
        bag.none[(K, Option[Deadline])]

      case TupleOrNone.Some(left, right) =>
        bag.success(Some((left.read[K], right)))
    }

  override def getKeyValueDeadline[BAG[_]](key: K, bag: Bag[BAG]): BAG[Option[((K, Option[Deadline]), V)]] =
    bag.flatMap(core.getKeyValueDeadline[BAG](key, core.readStates.get())(bag)) {
      case TupleOrNone.None =>
        bag.none[((K, Option[Deadline]), V)]

      case TupleOrNone.Some((key, deadline), right) =>
        bag.success(Some(((key.read[K], deadline), right.read[V])))
    }

  def contains(key: K): BAG[Boolean] =
    bag.suspend(core.contains(key, core.readStates.get()))

  def mightContain(key: K): BAG[Boolean] =
    bag.suspend(core mightContainKey key)

  def mightContainFunction[PF <: F](function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[Boolean] =
    bag.suspend(core mightContainFunction Slice.writeString(function.id))

  def keys: Set[K, F, BAG] =
    Set[K, F, BAG](core)(keySerializer, bag)

  private[swaydb] def keySet: mutable.Set[K] =
    keys.asScala

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

  def headOption: BAG[Option[(K, V)]] =
    bag.transform(
      headOrNull(
        from = None,
        reverseIteration = false,
        readState = core.readStates.get())
    )(Option(_))

  def headOrNull: BAG[(K, V)] =
    headOrNull(
      from = None,
      reverseIteration = false,
      readState = core.readStates.get()
    )

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

  private def sourceFree(): SourceFree[K, (K, V)] =
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


  def stream: Source[K, (K, V), BAG] =
    new Source(sourceFree())

  def iterator[BAG[_]](implicit bag: Bag.Sync[BAG]): Iterator[BAG[(K, V)]] =
    stream.iterator(bag)

  def sizeOfBloomFilterEntries: BAG[Int] =
    bag.suspend(core.bloomFilterKeyValueCount)

  def isEmpty: BAG[Boolean] =
    bag.transform(core.headKey(core.readStates.get()))(_.isNoneC)

  def nonEmpty: BAG[Boolean] =
    bag.transform(isEmpty)(!_)

  def lastOption: BAG[Option[(K, V)]] =
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

  override def isFunctionStoredAsApplied[PF <: F](functionId: PF)(implicit ev: PF <:< PureFunction[K, V, Apply.Map[V]]): Boolean =
    core.isFunctionApplied(Slice.writeString(functionId.id))

  /**
   * Returns an Async API of type O where the [[Bag]] is known.
   */
  def toBag[X[_]](implicit bag: Bag[X]): Map[K, V, F, X] =
    copy(core = core.toBag[X])

  def asScala: scala.collection.mutable.Map[K, V] =
    ScalaMap[K, V, F](toBag[Bag.Less](Bag.less))

  def close(): BAG[Unit] =
    bag.suspend(core.close())

  def delete(): BAG[Unit] =
    bag.suspend(core.delete())

  override def toString(): String =
    classOf[Map[_, _, _, BAG]].getSimpleName
}
