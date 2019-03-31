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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.extensions.stream

import scala.annotation.tailrec
import swaydb.Stream
import swaydb.data.IO
import swaydb.data.order.KeyOrder
import swaydb.extensions.Key
import swaydb.serializers.Serializer

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

case class MapKeysStream[K](mapKey: Seq[K],
                            mapsOnly: Boolean = false,
                            userDefinedFrom: Boolean = false,
                            set: swaydb.Set[Key[K], IO],
                            till: K => Boolean = (_: K) => true,
                            skip: Int = 0,
                            count: Option[Int] = None)(implicit keySerializer: Serializer[K],
                                                       mapKeySerializer: Serializer[Key[K]]) extends Stream[K, IO](skip, count) {

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

  def take(count: Int): MapKeysStream[K] =
    copy(count = Some(count))

  def drop(count: Int): MapKeysStream[K] =
    copy(skip = count)

  def takeWhile(condition: K => Boolean) =
    copy(till = condition)

  private def validate(mapKey: Key[K]): Step[K] = {
    val mapKeyBytes = Key.writeKeys(mapKey.parentMapKeys, keySerializer)
    if (KeyOrder.default.compare(mapKeyBytes, thisMapKeyBytes) != 0) //Exit if it's moved onto another map
      Step.Stop
    else
      mapKey match {
        case Key.MapStart(_) =>
          if (set.reverseIteration) //exit iteration if it's going backward since Start is the head key
            Step.Stop
          else
            Step.Next

        case Key.MapEntriesStart(_) =>
          if (set.reverseIteration) //exit iteration if it's going backward as previous entry is pointer entry Start
            Step.Stop
          else
            Step.Next

        case Key.MapEntry(_, dataKey) =>
          if (mapsOnly) {
            if (set.reverseIteration) //Exit if it's fetching subMaps only it's already reached an entry means it's has already crossed subMaps block
              Step.Stop
            else
              Step.Next
          } else {
            if (till(dataKey))
              Step.Success(dataKey)
            else
              Step.Stop
          }

        case Key.MapEntriesEnd(_) =>
          //if it's not going backwards and it's trying to fetch subMaps only then move forward
          if (!set.reverseIteration && !mapsOnly)
            Step.Stop
          else
            Step.Next

        case Key.SubMapsStart(_) =>
          //if it's not going backward and it's trying to fetch subMaps only then move forward
          if (!set.reverseIteration && !mapsOnly)
            Step.Stop
          else
            Step.Next

        case Key.SubMap(_, dataKey) =>
          if (!mapsOnly) //if subMaps are excluded
            if (set.reverseIteration) //if subMaps are excluded & it's going in reverse continue iteration.
              Step.Next
            else //if it's going forward with subMaps excluded then end iteration as it's already iterated all key-value entries.
              Step.Stop
          else if (till(dataKey))
            Step.Success(dataKey)
          else
            Step.Stop

        case Key.SubMapsEnd(_) =>
          //Exit if it's not going forward.
          if (!set.reverseIteration)
            Step.Stop
          else
            Step.Next

        case Key.MapEnd(_) =>
          //Exit if it's not going in reverse.
          if (!set.reverseIteration)
            Step.Stop
          else
            Step.Next
      }
  }

  /**
    * Stores raw key from previous read. This is a temporary solution because
    * this class extends Stream[K] and the types are being lost on stream.next here since previous
    * Key[K] is not known.
    */
  private var previousRaw = Option.empty[Key[K]]

  @tailrec
  private def step(previous: Key[K]): IO[Option[K]] =
    set.next(previous) match {
      case IO.Success(some @ Some(key)) =>
        previousRaw = some
        validate(key) match {
          case Step.Stop =>
            IO.none

          case Step.Next =>
            set.next(key) match {
              case IO.Success(Some(keyValue)) =>
                step(keyValue)

              case IO.Success(None) =>
                IO.none

              case IO.Failure(error) =>
                IO.Failure(error)
            }

          case Step.Success(keyValue) =>
            IO.Success(Some(keyValue))
        }

      case IO.Success(None) =>
        IO.none

      case IO.Failure(error) =>
        IO.Failure(error)
    }

  override def headOption: IO[Option[K]] =
    set.headOption match {
      case IO.Success(some @ Some((key))) =>
        previousRaw = some

        validate(key) match {
          case Step.Stop =>
            IO.none

          case Step.Next =>
            step((key))

          case Step.Success(keyValue) =>
            IO.Success(Some(keyValue))
        }

      case IO.Success(None) =>
        IO.none

      case IO.Failure(error) =>
        IO.Failure(error)
    }

  override def next(previous: K): IO[Option[K]] = {
    //ignore the input previous and use previousRaw instead. Temporary solution.
    previousRaw.map(step) getOrElse IO.Failure(new Exception("Previous raw not defined."))
  }

  def size: IO[Int] =
    run.map(_.size)

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
  def lastOption: IO[Option[K]] =
    reverse.headOption

  override def toString(): String =
    classOf[MapKeysStream[_]].getClass.getSimpleName
}
