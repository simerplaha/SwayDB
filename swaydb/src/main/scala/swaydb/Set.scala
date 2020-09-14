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
import swaydb.core.segment.ThreadReadState
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.stream.{From, SourceFree}
import swaydb.serializers.{Serializer, _}

import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * Set database API.
 *
 * For documentation check - http://swaydb.io/
 */
case class Set[A, F, BAG[_]] private(private val core: Core[BAG])(implicit serializer: Serializer[A],
                                                                  bag: Bag[BAG]) { self =>

  def path: Path =
    core.zeroPath.getParent

  def get(elem: A): BAG[Option[A]] =
    bag.map(core.getKey(elem, core.readStates.get()))(_.mapC(_.read[A]))

  def contains(elem: A): BAG[Boolean] =
    bag.suspend(core.contains(elem, core.readStates.get()))

  def mightContain(elem: A): BAG[Boolean] =
    bag.suspend(core mightContainKey elem)

  def mightContainFunction(function: F)(implicit evd: F <:< PureFunction.Set[A]) =
    bag.suspend(core mightContainFunction Slice.writeString[Byte](function.id))

  def add(elem: A): BAG[OK] =
    bag.suspend(core.put(key = elem))

  def add(elem: A, expireAt: Deadline): BAG[OK] =
    bag.suspend(core.put(elem, None, expireAt))

  def add(elem: A, expireAfter: FiniteDuration): BAG[OK] =
    bag.suspend(core.put(elem, None, expireAfter.fromNow))

  def add(elems: A*): BAG[OK] =
    add(elems)

  def add(elems: Stream[A, BAG]): BAG[OK] =
    bag.flatMap(elems.materialize)(add)

  def add(elems: Iterable[A]): BAG[OK] =
    add(elems.iterator)

  def add(elems: Iterator[A]): BAG[OK] =
    bag.suspend(core.commit(elems.map(elem => Prepare.Put(key = serializer.write(elem), value = Slice.Null, deadline = None))))

  def remove(elem: A): BAG[OK] =
    bag.suspend(core.remove(elem))

  def remove(from: A, to: A): BAG[OK] =
    bag.suspend(core.remove(from, to))

  def remove(elems: A*): BAG[OK] =
    remove(elems)

  def remove(elems: Stream[A, BAG]): BAG[OK] =
    bag.flatMap(elems.materialize)(remove)

  def remove(elems: Iterable[A]): BAG[OK] =
    remove(elems.iterator)

  def remove(elems: Iterator[A]): BAG[OK] =
    bag.suspend(core.commit(elems.map(elem => Prepare.Remove(serializer.write(elem)))))

  def expire(elem: A, after: FiniteDuration): BAG[OK] =
    bag.suspend(core.expire(elem, after.fromNow))

  def expire(elem: A, at: Deadline): BAG[OK] =
    bag.suspend(core.expire(elem, at))

  def expire(from: A, to: A, after: FiniteDuration): BAG[OK] =
    bag.suspend(core.expire(from, to, after.fromNow))

  def expire(from: A, to: A, at: Deadline): BAG[OK] =
    bag.suspend(core.expire(from, to, at))

  def expire(elems: (A, Deadline)*): BAG[OK] =
    expire(elems)

  def expire(elems: Stream[(A, Deadline), BAG]): BAG[OK] =
    bag.flatMap(elems.materialize)(expire)

  def expire(elems: Iterable[(A, Deadline)]): BAG[OK] =
    expire(elems.iterator)

  def expire(elems: Iterator[(A, Deadline)]): BAG[OK] =
    bag.suspend {
      core.commit {
        elems map {
          elemWithExpire =>
            Prepare.Remove(
              from = serializer.write(elemWithExpire._1),
              to = None,
              deadline = Some(elemWithExpire._2)
            )
        }
      }
    }

  def clear(): BAG[OK] =
    bag.suspend(core.clear(core.readStates.get()))

  def applyFunction(from: A, to: A, function: F)(implicit evd: F <:< PureFunction.Set[A]): BAG[OK] =
    bag.suspend(core.applyFunction(from, to, Slice.writeString[Byte](function.id)))

  def applyFunction(elem: A, function: F)(implicit evd: F <:< PureFunction.Set[A]): BAG[OK] =
    bag.suspend(core.applyFunction(elem, Slice.writeString[Byte](function.id)))

  def commit(prepare: Prepare[A, Nothing, F]*): BAG[OK] =
    bag.suspend(core.commit(preparesToUntyped(prepare).iterator))

  def commit(prepare: Stream[Prepare[A, Nothing, F], BAG]): BAG[OK] =
    bag.flatMap(prepare.materialize) {
      statements =>
        commit(statements)
    }

  def commit(prepare: Iterable[Prepare[A, Nothing, F]]): BAG[OK] =
    bag.suspend(core.commit(preparesToUntyped(prepare).iterator))

  def levelZeroMeter: LevelZeroMeter =
    core.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    core.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    core.sizeOfSegments

  def elemSize(elem: A): Int =
    (elem: Slice[Byte]).size

  def expiration(elem: A): BAG[Option[Deadline]] =
    bag.suspend(core.deadline(elem, core.readStates.get()))

  def timeLeft(elem: A): BAG[Option[FiniteDuration]] =
    bag.map(expiration(elem))(_.map(_.timeLeft))

  def head: BAG[Option[A]] =
    headOption(core.readStates.get())

  private[swaydb] def headOrNull: BAG[A] =
    headOrNull(
      from = None,
      reverseIteration = false,
      readState = core.readStates.get()
    )

  protected def headOption(readState: ThreadReadState): BAG[Option[A]] =
    bag.transform(
      headOrNull(
        from = None,
        reverseIteration = false,
        readState = readState
      )
    )(Option(_))

  private def headOrNull[BAG[_]](from: Option[From[A]],
                                 reverseIteration: Boolean,
                                 readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[A] =
    bag.map(headSliceOption(from, reverseIteration, readState)) {
      case Slice.Null =>
        null.asInstanceOf[A]

      case slice: Slice[Byte] =>
        serializer.read(slice)
    }

  private def headSliceOption[BAG[_]](from: Option[From[A]],
                                      reverseIteration: Boolean,
                                      readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[SliceOption[Byte]] =
    from match {
      case Some(from) =>
        val fromKeyBytes: Slice[Byte] = from.key
        if (from.before)
          core.beforeKey(fromKeyBytes, readState)
        else if (from.after)
          core.afterKey(fromKeyBytes, readState)
        else
          bag.flatMap(core.getKey(fromKeyBytes, readState)) {
            case Slice.Null =>
              if (from.orAfter)
                core.afterKey(fromKeyBytes, readState)
              else if (from.orBefore)
                core.beforeKey(fromKeyBytes, readState)
              else
                bag.success(Slice.Null)

            case slice: Slice[Byte] =>
              bag.success(slice)
          }

      case None =>
        if (reverseIteration)
          core.lastKey(readState)
        else
          core.headKey(readState)
    }

  private def nextSliceOption[BAG[_]](previous: A,
                                      reverseIteration: Boolean,
                                      readState: ThreadReadState)(implicit bag: Bag[BAG]): BAG[SliceOption[Byte]] =
    if (reverseIteration)
      core.beforeKey(serializer.write(previous), readState)
    else
      core.afterKey(serializer.write(previous), readState)

  private def sourceFree(): SourceFree[A, A] =
    new SourceFree[A, A](from = None, reverse = false) {
      val readState = core.readStates.get()

      override private[swaydb] def headOrNull[BAG[_]](from: Option[From[A]], reverse: Boolean)(implicit bag: Bag[BAG]) =
        self.headOrNull(
          from = from,
          reverseIteration = reverse,
          readState = readState
        )

      override private[swaydb] def nextOrNull[BAG[_]](previous: A, reverse: Boolean)(implicit bag: Bag[BAG]) =
        bag.map(nextSliceOption(previous, reverse, readState)) {
          case Slice.Null =>
            null.asInstanceOf[A]

          case slice: Slice[Byte] =>
            serializer.read(slice)
        }
    }

  def clearAppliedFunctions(): BAG[Iterable[String]] =
    bag.suspend(core.clearAppliedFunctions())

  def clearAppliedAndRegisteredFunctions(): BAG[Iterable[String]] =
    bag.suspend(core.clearAppliedAndRegisteredFunctions())

  def isFunctionApplied(function: F)(implicit evd: F <:< PureFunction.Set[A]): Boolean =
    core.isFunctionApplied(Slice.writeString[Byte](function.id))

  def stream: Source[A, A, BAG] =
    new swaydb.Source(sourceFree())

  def iterator[BAG[_]](implicit bag: Bag.Sync[BAG]): Iterator[BAG[A]] =
    stream.iterator(bag)

  def sizeOfBloomFilterEntries: BAG[Int] =
    bag.suspend(core.bloomFilterKeyValueCount)

  def isEmpty: BAG[Boolean] =
    bag.map(core.headKey(core.readStates.get()))(_.isNoneC)

  def nonEmpty: BAG[Boolean] =
    bag.map(isEmpty)(!_)

  def last: BAG[Option[A]] =
    bag.map(core.lastKey(core.readStates.get()))(_.mapC(_.read[A]))

  def toBag[X[_]](implicit bag: Bag[X]): Set[A, F, X] =
    copy(core = core.toBag[X])

  def asScala: scala.collection.mutable.Set[A] =
    ScalaSet[A, F](toBag[Bag.Less](Bag.less))

  def close(): BAG[Unit] =
    bag.suspend(core.close())

  def delete(): BAG[Unit] =
    bag.suspend(core.delete())

  override def equals(other: Any): Boolean =
    other match {
      case other: Set[_, _, _] =>
        other.path == this.path

      case _ =>
        false
    }

  override def hashCode(): Int =
    path.hashCode()

  override def toString(): String =
    s"Set(path = $path)"
}
