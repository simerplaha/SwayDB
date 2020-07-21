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

import swaydb.MultiMapKey.{MapEnd, MapStart}
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * Provides APIs to manage children/nested maps/child maps of [[MultiMap]].
 */
class Children[K, V, F, BAG[_]](map: Map[MultiMapKey[K], Option[V], PureFunction[MultiMapKey[K], Option[V], Apply.Map[Option[V]]], BAG],
                                mapKey: Iterable[K],
                                defaultExpiration: Option[Deadline])(implicit keySerializer: Serializer[K],
                                                                     valueSerializer: Serializer[V],
                                                                     bag: Bag[BAG]) {

  /**
   * Creates new or initialises the existing map.
   */
  def init(key: K): BAG[MultiMap[K, V, F, BAG]] =
    initOrPut(key = key, expireAt = None)

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[initOrPut]] if clearing existing entries is not required.
   */
  def put(key: K): BAG[MultiMap[K, V, F, BAG]] =
    initOrPut(key, None, clear = true)

  /**
   * Creates new or initialises the existing map.
   */
  def init(key: K, expireAfter: FiniteDuration): BAG[MultiMap[K, V, F, BAG]] =
    initOrPut(key, Some(expireAfter.fromNow))

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[initOrPut]] if clearing existing entries is not required.
   */
  def put(key: K, expireAfter: FiniteDuration): BAG[MultiMap[K, V, F, BAG]] =
    initOrPut(key, Some(expireAfter.fromNow), clear = true)

  /**
   * Creates new or initialises the existing map.
   */

  def init(key: K, expireAt: Deadline): BAG[MultiMap[K, V, F, BAG]] =
    initOrPut(key, Some(expireAt))

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[initOrPut]] if clearing existing entries is not required.
   */
  def put(key: K, expireAt: Deadline): BAG[MultiMap[K, V, F, BAG]] =
    initOrPut(key = key, expireAt = Some(expireAt), clear = true)

  /**
   * Inserts a child map to this [[MultiMap]].
   *
   * @param key      key assign to the child
   * @param expireAt expiration
   * @param clear    if true, removes all existing entries before initialising the child Map.
   *                 Clear uses a range entry to clear existing key-values and inserts to [[Range]]
   *                 in [[swaydb.core.level.zero.LevelZero]]'s [[Map]] entries can be slower because it
   *                 requires skipList to be cloned on each insert. As the compaction progresses the
   *                 range entries will get applied the performance goes back to normal. But try to avoid
   *                 using clear.
   */
  def initOrPut(key: K, expireAt: Option[Deadline], clear: Boolean = false): BAG[MultiMap[K, V, F, BAG]] = {
    val childMapKey = mapKey.toBuffer :+ key

    val expiry = expireAt.orElse(defaultExpiration)

    val buffer = Slice.create[Prepare[MultiMapKey[K], Option[V], Nothing]](8)

    if (clear)
      buffer add Prepare.Remove(MapStart(childMapKey), MapEnd(childMapKey))

    buffer add Prepare.Put(MultiMapKey.SubMap(mapKey, key), None, expiry)
    buffer add Prepare.Put(MultiMapKey.MapStart(childMapKey), None, expiry)
    buffer add Prepare.Put(MultiMapKey.MapEntriesStart(childMapKey), None, expiry)
    buffer add Prepare.Put(MultiMapKey.MapEntriesEnd(childMapKey), None, expiry)
    buffer add Prepare.Put(MultiMapKey.SubMapsStart(childMapKey), None, expiry)
    buffer add Prepare.Put(MultiMapKey.SubMapsEnd(childMapKey), None, expiry)
    buffer add Prepare.Put(MultiMapKey.MapEnd(childMapKey), None, expiry)

    bag.map(map.commit(buffer)) {
      _ =>
        MultiMap(
          map = map,
          mapKey = childMapKey,
          defaultExpiration = expireAt orElse defaultExpiration
        )
    }
  }

  def remove(key: K): BAG[OK] = {
    val childMapKey = mapKey.toBuffer :+ key
    map.remove(MapStart(childMapKey), MapEnd(childMapKey))
  }

  /**
   * Returns the child Map
   */
  def get[K2 <: K](key: K2): BAG[Option[MultiMap[K2, V, F, BAG]]] = {
    val mapPrefix = mapKey.toBuffer :+ key

    bag.map(map.contains(MapStart(mapPrefix))) {
      contains =>
        if (contains)
          Some(
            MultiMap(
              map = map,
              mapKey = mapPrefix,
              defaultExpiration = defaultExpiration
            ).asInstanceOf[MultiMap[K2, V, F, BAG]]
          )
        else
          None
    }
  }

  /**
   * Streams all child Maps.
   */
  def stream: Stream[MultiMap[K, V, F, BAG]] =
    map
      .after(MultiMapKey.SubMapsStart(mapKey))
      .keys
      .stream
      .takeWhile {
        case MultiMapKey.SubMap(parentKey, _) =>
          parentKey == mapKey

        case _ =>
          false
      }
      .map {
        case MultiMapKey.SubMap(key, dataKey) =>
          val mapKey = key.toBuffer += dataKey
          MultiMap(map, mapKey)

        case entry =>
          MultiMap.failure(classOf[MultiMapKey.SubMap[_]], entry.getClass)
      }

  def isEmpty: BAG[Boolean] =
    bag.transform(stream.headOrNull) {
      head =>
        head == null
    }

  def nonEmpty: BAG[Boolean] =
    bag.transform(isEmpty)(!_)
}