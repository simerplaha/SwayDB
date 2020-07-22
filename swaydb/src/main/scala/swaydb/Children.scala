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
import swaydb.serializers._
import swaydb.core.util.Times._

import scala.collection.mutable.ListBuffer
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
  def getOrPut(key: K): BAG[MultiMap[K, V, F, BAG]] =
    getOrPut(key = key, expireAt = None, clear = false)

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[getOrPut]] if clearing existing entries is not required.
   */
  def put(key: K): BAG[MultiMap[K, V, F, BAG]] =
    getOrPut(key, None, clear = true)

  /**
   * Creates new or initialises the existing map.
   */
  def getOrPut(key: K, expireAfter: FiniteDuration): BAG[MultiMap[K, V, F, BAG]] =
    getOrPut(key, Some(expireAfter.fromNow), clear = false)

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[getOrPut]] if clearing existing entries is not required.
   */
  def put(key: K, expireAfter: FiniteDuration): BAG[MultiMap[K, V, F, BAG]] =
    getOrPut(key, Some(expireAfter.fromNow), clear = true)

  /**
   * Creates new or initialises the existing map.
   */

  def getOrPut(key: K, expireAt: Deadline): BAG[MultiMap[K, V, F, BAG]] =
    getOrPut(key, Some(expireAt), clear = false)

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[getOrPut]] if clearing existing entries is not required.
   */
  def put(key: K, expireAt: Deadline): BAG[MultiMap[K, V, F, BAG]] =
    getOrPut(key = key, expireAt = Some(expireAt), clear = true)

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
  private def getOrPut(key: K, expireAt: Option[Deadline], clear: Boolean): BAG[MultiMap[K, V, F, BAG]] =
    bag.flatMap(get(key)) {
      case Some(map) =>
        expireAt match {
          case Some(existing) =>
            val newExpiration = existing.earlier(expireAt)
            if (newExpiration == existing)
              bag.success(map)
            else
              create(key = key, expireAt = Some(newExpiration), clear = clear, expire = true)

          case None =>
            bag.success(map)
        }

      case None =>
        create(key = key, expireAt = expireAt, clear = false, expire = false)
    }

  private def create(key: K, expireAt: Option[Deadline], clear: Boolean, expire: Boolean): BAG[MultiMap[K, V, F, BAG]] = {

    val childMapKey = mapKey.toBuffer += key

    val expiry = expireAt.earlier(defaultExpiration)

    val buffer = Slice.create[Prepare[MultiMapKey[K], Option[V], Nothing]](9)

    if (clear)
      buffer add Prepare.Remove(MapStart(childMapKey), MapEnd(childMapKey))
    else if (expire)
      buffer add Prepare.Remove(from = MapStart(childMapKey), to = Some(MapEnd(childMapKey)), deadline = expiry)

    buffer add Prepare.Put(MultiMapKey.SubMap(mapKey, key), None, expiry)
    buffer add Prepare.Put(MultiMapKey.MapStart(childMapKey), None, expiry)
    buffer add Prepare.Put(MultiMapKey.MapEntriesStart(childMapKey), None, expiry)
    buffer add Prepare.Put(MultiMapKey.MapEntriesEnd(childMapKey), None, expiry)
    buffer add Prepare.Put(MultiMapKey.SubMapsStart(childMapKey), None, expiry)
    buffer add Prepare.Put(MultiMapKey.SubMapsEnd(childMapKey), None, expiry)
    buffer add Prepare.Put(MultiMapKey.MapEnd(childMapKey), None, expiry)

    bag.transform(map.commit(buffer)) {
      _ =>
        MultiMap(
          map = map,
          mapKey = childMapKey,
          defaultExpiration = expiry
        )
    }
  }

  def remove(key: K): BAG[OK] = {
    val childMapKey = mapKey.toBuffer += key
    map.remove(MapStart(childMapKey), MapEnd(childMapKey))
  }

  /**
   * Returns the child Map
   */
  def get[K2 <: K](key: K2): BAG[Option[MultiMap[K2, V, F, BAG]]] = {
    val mapPrefix = mapKey.toBuffer += key

    bag.map(map.getKeyDeadline(MapStart(mapPrefix))) {
      case Some((_, deadline)) =>
        Some(
          MultiMap(
            map = map,
            mapKey = mapPrefix,
            defaultExpiration = deadline
          ).asInstanceOf[MultiMap[K2, V, F, BAG]]
        )

      case None =>
        None
    }
  }

  /**
   * Keys of all child Maps.
   */
  def keys: Stream[K] =
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
      .collect {
        case MultiMapKey.SubMap(_, dataKey) =>
          dataKey
      }

  //todo - flatten Options and BAG.
  def stream: Stream[BAG[Option[MultiMap[K, V, F, BAG]]]] =
    keys.map(get)

  def isEmpty: BAG[Boolean] =
    bag.transform(keys.headOrNull) {
      head =>
        head == null
    }

  def nonEmpty: BAG[Boolean] =
    bag.transform(isEmpty)(!_)
}