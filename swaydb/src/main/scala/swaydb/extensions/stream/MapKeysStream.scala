/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.extensions.stream

import swaydb.Error.API.ExceptionHandler
import swaydb.data.slice.Slice
import swaydb.extensions.Key
import swaydb.serializers.Serializer
import swaydb.{IO, Streamable}

import scala.annotation.tailrec

/**
 * TODO - [[MapStream]] and [[MapKeysStream]] are similar and need a higher type - tagless final.
 *
 * Sample order
 *
 * Key.MapStart(1),
 *   MapKey.EntriesStart(1)
 *     MapKey.Entry(1, 1)
 *   MapKey.EntriesEnd(1)
 *   MapKey.SubMapsStart(1)
 *     MapKey.SubMap(1, 1000)
 *   MapKey.SubMapsEnd(1)
 * MapKey.End(1)
 **/

object MapKeysStream {

  private def toK[K](keyValue: Key[K]): K = {
    keyValue match {
      case key: Key.MapEntry[K] =>
        key.dataKey

      case key: Key.SubMap[K] =>
        key.dataKey

      //FIXME: make this is not type-safe. This should never occur.
      case keyValue =>
        throw IO.throwable(s"Can serialise toKV: $keyValue")
    }
  }

  @tailrec
  private def step[K](stream: swaydb.Stream[Key[K], IO.ApiIO],
                      previous: Key[K],
                      isReverse: Boolean,
                      mapsOnly: Boolean,
                      thisMapKeyBytes: Slice[Byte])(implicit keySerializer: Serializer[K]): IO.ApiIO[Option[Key[K]]] =
    stream.next(previous) match {
      case IO.Right(some @ Some(key)) =>
        MapStream.checkStep(key = key, isReverse = isReverse, mapsOnly = mapsOnly, thisMapKeyBytes = thisMapKeyBytes) match {
          case Step.Stop =>
            IO.none

          case Step.Next =>
            stream.next(key) match {
              case IO.Right(Some(keyValue)) =>
                step(
                  stream = stream,
                  previous = keyValue,
                  isReverse = isReverse,
                  mapsOnly = mapsOnly,
                  thisMapKeyBytes = thisMapKeyBytes
                )

              case IO.Right(None) =>
                IO.none

              case IO.Left(error) =>
                IO.Left(error)
            }

          case Step.Success =>
            IO.Right(some)
        }

      case IO.Right(None) =>
        IO.none

      case IO.Left(error) =>
        IO.Left(error)
    }
}

case class MapKeysStream[K](mapKey: Seq[K],
                            mapsOnly: Boolean = false,
                            userDefinedFrom: Boolean = false,
                            set: swaydb.Set[Key[K], Nothing, IO.ApiIO])(implicit keySerializer: Serializer[K],
                                                                        mapKeySerializer: Serializer[Key[K]]) extends Streamable[K, IO.ApiIO] { self =>

  private val endEntriesKey = Key.MapEntriesEnd(mapKey)
  private val endSubMapsKey = Key.SubMapsEnd(mapKey)

  private val thisMapKeyBytes = Key.writeKeys(mapKey, keySerializer)

  def from(key: K): MapKeysStream[K] =
    if (mapsOnly)
      copy(set = set.from(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(set = set.from(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def before(key: K): MapKeysStream[K] =
    if (mapsOnly)
      copy(set = set.before(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(set = set.before(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def fromOrBefore(key: K): MapKeysStream[K] =
    if (mapsOnly)
      copy(set = set.fromOrBefore(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(set = set.fromOrBefore(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def after(key: K): MapKeysStream[K] =
    if (mapsOnly)
      copy(set = set.after(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(set = set.after(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def fromOrAfter(key: K): MapKeysStream[K] =
    if (mapsOnly)
      copy(set = set.fromOrAfter(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(set = set.fromOrAfter(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  private def before(key: Key[K], reverse: Boolean): MapKeysStream[K] =
    copy(set = set.before(key).copy(reverseIteration = reverse))

  private def reverse(reverse: Boolean): MapKeysStream[K] =
    copy(set = set.copy(reverseIteration = reverse))

  def isReverse: Boolean =
    self.set.reverseIteration

  private def headOptionInner: IO.ApiIO[Option[Key[K]]] = {
    val stream = set.stream
    set.headOption match {
      case IO.Right(someKeyValue @ Some(key)) =>
        MapStream.checkStep(
          key = key,
          isReverse = set.reverseIteration,
          mapsOnly = mapsOnly,
          thisMapKeyBytes = thisMapKeyBytes
        ) match {
          case Step.Stop =>
            IO.none

          case Step.Next =>
            MapKeysStream.step(
              stream = stream,
              previous = key,
              isReverse = set.reverseIteration,
              mapsOnly = mapsOnly,
              thisMapKeyBytes = thisMapKeyBytes
            )

          case Step.Success =>
            IO.Right(someKeyValue)
        }

      case IO.Right(None) =>
        IO.none

      case IO.Left(error) =>
        IO.Left(error)
    }
  }

  override def headOption: IO.ApiIO[Option[K]] =
    headOptionInner.map(_.map(MapKeysStream.toK))

  override def drop(count: Int): swaydb.Stream[K, IO.ApiIO] =
    stream drop count

  override def dropWhile(f: K => Boolean): swaydb.Stream[K, IO.ApiIO] =
    stream dropWhile f

  override def take(count: Int): swaydb.Stream[K, IO.ApiIO] =
    stream take count

  override def takeWhile(f: K => Boolean): swaydb.Stream[K, IO.ApiIO] =
    stream takeWhile f

  override def map[B](f: K => B): swaydb.Stream[B, IO.ApiIO] =
    stream map f

  override def flatMap[B](f: K => swaydb.Stream[B, IO.ApiIO]): swaydb.Stream[B, IO.ApiIO] =
    stream flatMap f

  override def foreach[U](f: K => U): swaydb.Stream[Unit, IO.ApiIO] =
    stream foreach f

  override def filter(f: K => Boolean): swaydb.Stream[K, IO.ApiIO] =
    stream filter f

  override def filterNot(f: K => Boolean): swaydb.Stream[K, IO.ApiIO] =
    stream filterNot f

  override def size: IO.ApiIO[Int] =
    stream.size

  override def foldLeft[B](initial: B)(f: (B, K) => B): IO.ApiIO[B] =
    stream.foldLeft(initial)(f)

  def stream: swaydb.Stream[K, IO.ApiIO] =
    new swaydb.Stream[K, IO.ApiIO] {
      /**
       * Stores raw key-value from previous read. This is a temporary solution because
       * this class extends Stream[K] and the types are being lost on stream.next here since previous
       * Key[K] is not known.
       */
      private var previousRaw: Key[K] = _

      override def headOption: IO.ApiIO[Option[K]] =
        self.headOptionInner.map(_.map {
          raw =>
            previousRaw = raw
            MapKeysStream.toK(raw)
        })

      override private[swaydb] def next(previous: K): IO.ApiIO[Option[K]] =
        MapKeysStream.step(
          stream = self.set.stream,
          previous = previousRaw,
          isReverse = isReverse,
          mapsOnly = mapsOnly,
          thisMapKeyBytes = thisMapKeyBytes
        ).map(_.map {
          raw =>
            previousRaw = raw
            MapKeysStream.toK(raw)
        })
    }

  /**
   * Returns the start key when doing reverse iteration.
   *
   * If subMaps are included then it will return the starting point to be [[Key.SubMapsEnd]]
   * which will iterate backward until [[Key.MapEntriesStart]]
   * else returns the starting point to be [[Key.MapEntriesEnd]] to fetch entries only.
   */
  def reverse: MapKeysStream[K] =
    if (userDefinedFrom) //if user has defined from then do not override it and just set reverse to true.
      reverse(reverse = true)
    else if (mapsOnly) //if from is not already set & map are included in the iteration then start from subMap's last key
      before(key = endSubMapsKey, reverse = true)
    else //if subMaps are excluded, start from key's last key.
      before(key = endEntriesKey, reverse = true)

  /**
   * lastOption should always force formKey to be the [[endSubMapsKey]]
   * because from is always set in [[swaydb.extensions.Maps]] and regardless from where the iteration starts the
   * most efficient way to fetch the last is from the key [[endSubMapsKey]].
   */
  override def lastOption: IO.ApiIO[Option[K]] =
    reverse.headOption

  override def toString(): String =
    classOf[MapKeysStream[_]].getClass.getSimpleName
}
