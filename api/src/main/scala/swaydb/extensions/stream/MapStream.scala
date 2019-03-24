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
import swaydb.data.slice.Slice
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

case class MapStream[K, V](mapKey: Seq[K],
                           mapsOnly: Boolean = false,
                           userDefinedFrom: Boolean = false,
                           map: swaydb.Map[Key[K], Option[V], IO],
                           till: (K, V) => Boolean = (_: K, _: V) => true,
                           skip: Int = 0,
                           count: Option[Int] = None)(implicit keySerializer: Serializer[K],
                                                      mapKeySerializer: Serializer[Key[K]],
                                                      optionValueSerializer: Serializer[Option[V]]) extends Stream[(K, V), IO] {

  private val endEntriesKey = Key.MapEntriesEnd(mapKey)
  private val endSubMapsKey = Key.SubMapsEnd(mapKey)

  private val thisMapKeyBytes = Key.writeKeys(mapKey, keySerializer)

  def from(key: K): MapStream[K, V] =
    if (mapsOnly)
      copy(map = map.from(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(map = map.from(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def before(key: K): MapStream[K, V] =
    if (mapsOnly)
      copy(map = map.before(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(map = map.before(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def fromOrBefore(key: K): MapStream[K, V] =
    if (mapsOnly)
      copy(map = map.fromOrBefore(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(map = map.fromOrBefore(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def after(key: K): MapStream[K, V] =
    if (mapsOnly)
      copy(map = map.after(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(map = map.after(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def fromOrAfter(key: K): MapStream[K, V] =
    if (mapsOnly)
      copy(map = map.fromOrAfter(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(map = map.fromOrAfter(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  private def before(key: Key[K], reverse: Boolean): MapStream[K, V] =
    copy(map = map.before(key).copy(reverseIteration = reverse))

  private def reverse(reverse: Boolean): MapStream[K, V] =
    copy(map = map.copy(reverseIteration = reverse))

  def till(condition: (K, V) => Boolean) =
    copy(till = condition)

  def tillKey(condition: K => Boolean) =
    copy(
      till =
        (key: K, _: V) =>
          condition(key)
    )

  def tillValue(condition: V => Boolean) =
    copy(
      till =
        (_: K, value: V) =>
          condition(value)
    )

  def take(count: Int): MapStream[K, V] =
    copy(count = Some(count))

  def drop(count: Int): MapStream[K, V] =
    copy(skip = count)

  private def validate(mapKey: Key[K], valueOption: Option[V]): Step[(K, V)] = {
    //type casting here because a value set by the User will/should always have a serializer. If the inner value if
    //Option[V] then the full value would be Option[Option[V]] and the serializer is expected to handle serialization for
    //the inner Option[V] which would be of the User's type V.
    val value = valueOption.getOrElse(optionValueSerializer.read(Slice.emptyBytes).asInstanceOf[V])
    val mapKeyBytes = Key.writeKeys(mapKey.parentMapKeys, keySerializer)
    if (KeyOrder.default.compare(mapKeyBytes, thisMapKeyBytes) != 0) //Exit if it's moved onto another map
      Step.Stop
    else
      mapKey match {
        case Key.MapStart(_) =>
          if (map.reverseIteration) //exit iteration if it's going backward since Start is the head key
            Step.Stop
          else
            Step.Next

        case Key.MapEntriesStart(_) =>
          if (map.reverseIteration) //exit iteration if it's going backward as previous entry is pointer entry Start
            Step.Stop
          else
            Step.Next

        case Key.MapEntry(_, dataKey) =>
          if (mapsOnly) {
            if (map.reverseIteration) //Exit if it's fetching subMaps only it's already reached an entry means it's has already crossed subMaps block
              Step.Stop
            else
              Step.Next
          } else {
            if (till(dataKey, value))
              Step.Success(dataKey, value)
            else
              Step.Stop
          }

        case Key.MapEntriesEnd(_) =>
          //if it's not going backwards and it's trying to fetch subMaps only then move forward
          if (!map.reverseIteration && !mapsOnly)
            Step.Stop
          else
            Step.Next

        case Key.SubMapsStart(_) =>
          //if it's not going backward and it's trying to fetch subMaps only then move forward
          if (!map.reverseIteration && !mapsOnly)
            Step.Stop
          else
            Step.Next

        case Key.SubMap(_, dataKey) =>
          if (!mapsOnly) //if subMaps are excluded
            if (map.reverseIteration) //if subMaps are excluded & it's going in reverse continue iteration.
              Step.Next
            else //if it's going forward with subMaps excluded then end iteration as it's already iterated all key-value entries.
              Step.Stop
          else if (till(dataKey, value))
            Step.Success(dataKey, value)
          else
            Step.Stop

        case Key.SubMapsEnd(_) =>
          //Exit if it's not going forward.
          if (!map.reverseIteration)
            Step.Stop
          else
            Step.Next

        case Key.MapEnd(_) =>
          //Exit if it's not going in reverse.
          if (!map.reverseIteration)
            Step.Stop
          else
            Step.Next
      }
  }

  /**
    * Stores raw key-value from previous read. This is a temporary solution because
    * this class extends Stream[(K, V)] and the types are being lost on stream.next here since previous
    * (Key[K], Option[V]) is not known.
    */
  private var previousRaw = Option.empty[(Key[K], Option[V])]

  @tailrec
  private def step(previous: (Key[K], Option[V])): IO[Option[(K, V)]] =
    map.next(previous) match {
      case IO.Success(some @ Some((key, value))) =>
        previousRaw = some
        validate(key, value) match {
          case Step.Stop =>
            IO.none

          case Step.Next =>
            map.next(key, value) match {
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

  override def headOption: IO[Option[(K, V)]] =
    map.headOption match {
      case IO.Success(some @ Some((key, value))) =>
        previousRaw = some

        validate(key, value) match {
          case Step.Stop =>
            IO.none

          case Step.Next =>
            step((key, value))

          case Step.Success(keyValue) =>
            IO.Success(Some(keyValue))
        }

      case IO.Success(None) =>
        IO.none

      case IO.Failure(error) =>
        IO.Failure(error)
    }

  override def next(previous: (K, V)): IO[Option[(K, V)]] = {
    //ignore the input previous and use previousRaw instead. Temporary solution.
    previousRaw.map(step) getOrElse IO.Failure(new Exception("Previous raw not defined."))
  }

  override def restart: Stream[(K, V), IO] =
    copy()

  /**
    * Returns the start key when doing reverse iteration.
    *
    * If subMaps are included then it will return the starting point to be [[Key.SubMapsEnd]]
    * which will iterate backward until [[Key.MapEntriesStart]]
    * else returns the starting point to be [[Key.MapEntriesEnd]] to fetch entries only.
    */
  def reverse: MapStream[K, V] =
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
  def lastOption: IO[Option[(K, V)]] =
    reverse.headOption

  override def toString(): String =
    classOf[MapStream[_, _]].getClass.getSimpleName
}
