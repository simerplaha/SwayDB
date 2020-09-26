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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.multimap

import swaydb.core.map.counter.CounterMap
import swaydb.core.util.Times._
import swaydb.multimap.MultiKey.Child
import swaydb.serializers._
import swaydb.{Apply, Bag, Map, MultiMap, Prepare, PureFunction, Source, Stream}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * Provides APIs to manage children/nested maps/child maps of [[MultiMap]].
 */
abstract class Schema[M, K, V, F, BAG[_]](multiMap: Map[MultiKey[M, K], MultiValue[V], PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]], BAG],
                                          mapId: Long,
                                          defaultExpiration: Option[Deadline])(implicit keySerializer: Serializer[K],
                                                                               childKeySerializer: Serializer[M],
                                                                               valueSerializer: Serializer[V],
                                                                               counter: CounterMap,
                                                                               bag: Bag[BAG]) extends Source[K, (K, V), BAG] {

  /**
   * Creates new or initialises the existing map.
   */
  def child(mapKey: M): BAG[MultiMap[M, K, V, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = None, forceClear = false)

  def child[K2 <: K](mapKey: M, keyType: Class[K2]): BAG[MultiMap[M, K2, V, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = None, forceClear = false)

  def child[K2 <: K, V2 <: V](mapKey: M, keyType: Class[K2], valueType: Class[V2]): BAG[MultiMap[M, K2, V2, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = None, forceClear = false)

  /**
   * Creates new or initialises the existing map.
   */
  def child(mapKey: M, expireAfter: FiniteDuration): BAG[MultiMap[M, K, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = false)

  def child[K2 <: K](mapKey: M, keyType: Class[K2], expireAfter: FiniteDuration): BAG[MultiMap[M, K2, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = false)

  def child[K2 <: K, V2 <: V](mapKey: M, keyType: Class[K2], valueType: Class[V2], expireAfter: FiniteDuration): BAG[MultiMap[M, K2, V2, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = false)


  /**
   * Creates new or initialises the existing map.
   */
  def child(mapKey: M, expireAt: Deadline): BAG[MultiMap[M, K, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAt), forceClear = false)

  def child[K2 <: K](mapKey: M, keyType: Class[K2], expireAt: Deadline): BAG[MultiMap[M, K2, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAt), forceClear = false)

  def child[K2 <: K, V2 <: V](mapKey: M, keyType: Class[K2], valueType: Class[V2], expireAt: Deadline): BAG[MultiMap[M, K2, V2, F, BAG]] =
    getOrPut(mapKey, Some(expireAt), forceClear = false)


  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[child]] if clearing existing entries is not required.
   */
  def replaceChild(mapKey: M): BAG[MultiMap[M, K, V, F, BAG]] =
    getOrPut(mapKey, None, forceClear = true)

  def replaceChild[K2 <: K](mapKey: M, keyType: Class[K2]): BAG[MultiMap[M, K2, V, F, BAG]] =
    getOrPut(mapKey, None, forceClear = true)

  def replaceChild[K2 <: K, V2 <: V](mapKey: M, keyType: Class[K2], valueType: Class[V2]): BAG[MultiMap[M, K2, V2, F, BAG]] =
    getOrPut(mapKey, None, forceClear = true)

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[child]] if clearing existing entries is not required.
   */
  def replaceChild(mapKey: M, expireAfter: FiniteDuration): BAG[MultiMap[M, K, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = true)

  def replaceChild[K2 <: K](mapKey: M, keyType: Class[K2], expireAfter: FiniteDuration): BAG[MultiMap[M, K2, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = true)

  def replaceChild[K2 <: K, V2 <: V](mapKey: M, keyType: Class[K2], valueType: Class[V2], expireAfter: FiniteDuration): BAG[MultiMap[M, K2, V2, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = true)

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[child]] if clearing existing entries is not required.
   */
  def replaceChild(mapKey: M, expireAt: Deadline): BAG[MultiMap[M, K, V, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = Some(expireAt), forceClear = true)

  def replaceChild[K2 <: K](mapKey: M, keyType: Class[K2], expireAt: Deadline): BAG[MultiMap[M, K2, V, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = Some(expireAt), forceClear = true)

  def replaceChild[K2 <: K, V2 <: V](mapKey: M, keyType: Class[K2], valueType: Class[V2], expireAt: Deadline): BAG[MultiMap[M, K2, V2, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = Some(expireAt), forceClear = true)

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[child]] if clearing existing entries is not required.
   */

  def replaceChild(mapKey: M, expireAt: Option[Deadline]): BAG[MultiMap[M, K, V, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = expireAt, forceClear = true)

  def replaceChild[K2 <: K](mapKey: M, keyType: Class[K2], expireAt: Option[Deadline]): BAG[MultiMap[M, K2, V, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = expireAt, forceClear = true)

  def replaceChild[K2 <: K, V2 <: V](mapKey: M, keyType: Class[K2], valueType: Class[V2], expireAt: Option[Deadline]): BAG[MultiMap[M, K2, V2, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = expireAt, forceClear = true)


  /**
   * @return false if the map does not exist else true on successful remove.
   */
  def removeChild(mapKey: M): BAG[Boolean] =
    bag.flatMap(prepareRemove(mapKey = mapKey, expiration = None, forceClear = true, expire = false)) {
      buffer =>
        if (buffer.isEmpty)
          bag.success(false)
        else
          bag.transform(multiMap.commit(buffer)) {
            _ =>
              true
          }
    }

  /**
   * Inserts a child map to this [[MultiMap]].
   *
   * @param childKey   key assign to the child
   * @param expireAt   expiration
   * @param forceClear if true, removes all existing entries before initialising the child Map.
   *                   Clear uses a range entry to clear existing key-values and inserts to [[Range]]
   *                   in [[swaydb.core.level.zero.LevelZero]]'s [[Map]] entries can be slower because it
   *                   requires skipList to be cloned on each insert. As the compaction progresses the
   *                   range entries will get applied the performance goes back to normal. But try to avoid
   *                   using clear.
   */
  private def getOrPut[K2 <: K, V2 <: V, F2 <: F](childKey: M, expireAt: Option[Deadline], forceClear: Boolean): BAG[MultiMap[M, K2, V2, F2, BAG]] =
    bag.flatMap(getChild(childKey)) {
      case Some(_child) =>
        val childMap = _child.asInstanceOf[MultiMap[M, K2, V2, F2, BAG]]

        if (forceClear)
          create(childKey = childKey, Some(childMap.mapId), expireAt = expireAt, forceClear = forceClear, expire = false)
        else
          expireAt match {
            case Some(updatedExpiration) =>
              val newExpiration = defaultExpiration earlier updatedExpiration
              //if the expiration is not updated return the map.
              if (defaultExpiration contains newExpiration)
                bag.success(childMap)
              else // expiration is updated perform create.
                create(childKey = childKey, Some(childMap.mapId), expireAt = Some(newExpiration), forceClear = false, expire = true)

            case None =>
              bag.success(childMap)
          }

      case None =>
        create(childKey = childKey, childId = None, expireAt = expireAt, forceClear = false, expire = false)
    }

  private def create[K2 <: K, V2 <: V, F2 <: F](childKey: M, childId: Option[Long], expireAt: Option[Deadline], forceClear: Boolean, expire: Boolean): BAG[MultiMap[M, K2, V2, F2, BAG]] = {
    val expiration = expireAt earlier defaultExpiration

    val buffer = prepareRemove(mapKey = childKey, expiration = expiration, forceClear = forceClear, expire = expire)

    bag.flatMap(buffer) {
      buffer =>
        val childIdOrNew = childId getOrElse counter.next

        buffer += Prepare.Put(MultiKey.Child[M](mapId, childKey), MultiValue.MapId(childIdOrNew), expiration)
        buffer += Prepare.Put(MultiKey.Start(childIdOrNew), MultiValue.None, expiration)
        buffer += Prepare.Put(MultiKey.KeysStart(childIdOrNew), MultiValue.None, expiration)
        buffer += Prepare.Put(MultiKey.KeysEnd(childIdOrNew), MultiValue.None, expiration)
        buffer += Prepare.Put(MultiKey.ChildrenStart(childIdOrNew), MultiValue.None, expiration)
        buffer += Prepare.Put(MultiKey.ChildrenEnd(childIdOrNew), MultiValue.None, expiration)
        buffer += Prepare.Put(MultiKey.End(childIdOrNew), MultiValue.None, expiration)

        bag.transform(multiMap.commit(buffer)) {
          _ =>
            MultiMap(
              multiMap = multiMap,
              mapKey = childKey,
              mapId = childIdOrNew,
              defaultExpiration = expiration
            ).asInstanceOf[MultiMap[M, K2, V2, F2, BAG]]
        }
    }
  }

  /**
   * Returns a list of [[Prepare.Remove]] statements.
   *
   * @param expiration default expiration to set
   * @param forceClear remove the map
   * @param expire     updates the expiration only. If forceClear is true then this is ignored.
   * @return a list of [[Prepare.Remove]] statements.
   */
  protected def prepareRemove(expiration: Option[Deadline],
                              forceClear: Boolean,
                              expire: Boolean): BAG[ListBuffer[Prepare[MultiKey[M, K], MultiValue[V], Nothing]]] = {
    val buffer = ListBuffer.empty[Prepare[MultiKey[M, K], MultiValue[V], Nothing]]

    if (forceClear || expire) {
      //ignore expiry if forceClear is set to true. ForceClear should remove instead of just setting a new expiry.
      val prepareRemoveExpiry =
        if (!forceClear && expire)
          expiration
        else
          None

      bag.transform(prepareRemove(prepareRemoveExpiry)) {
        removes =>
          buffer ++= removes
      }
    } else {
      bag.success(buffer)
    }
  }

  /**
   * Builds [[Prepare.Remove]] statements to remove the key's map and all that key's children.
   */
  private def prepareRemove(mapKey: M,
                            expiration: Option[Deadline],
                            forceClear: Boolean,
                            expire: Boolean): BAG[ListBuffer[Prepare[MultiKey[M, K], MultiValue[V], Nothing]]] =
    bag.flatMap(getChild(mapKey)) {
      case Some(child) =>
        val buffer = child.prepareRemove(expiration = expiration, forceClear = forceClear, expire = expire)

        bag.transform(buffer) {
          buffer =>
            val deadline =
              if (!forceClear && expire)
                expiration
              else
                None

            buffer ++= buildPrepareRemove(mapKey, child.mapId, deadline)
        }

      case None =>
        bag.success(ListBuffer.empty)
    }

  /**
   * Builds [[Prepare.Remove]] statements for a child with the key.
   */
  private def buildPrepareRemove(subMapKey: M, subMapId: Long, expire: Option[Deadline]): Seq[Prepare.Remove[MultiKey[M, K]]] = {
    Seq(
      Prepare.Remove(MultiKey.Child(mapId, subMapKey), None, expire),
      Prepare.Remove(MultiKey.Start(subMapId), Some(MultiKey.End(subMapId)), expire)
    )
  }

  /**
   * Builds [[Prepare.Remove]] statements for all children of this map.
   */
  protected def prepareRemove(expire: Option[Deadline]): BAG[ListBuffer[Prepare.Remove[MultiKey[M, K]]]] =
    children.foldLeftFlatten(ListBuffer.empty[Prepare.Remove[MultiKey[M, K]]]) {
      case (buffer, child) =>

        buffer ++= buildPrepareRemove(child.mapKey, child.mapId, expire)
        bag.transform(child.prepareRemove(expire)) {
          childPrepares =>
            buffer ++= childPrepares
        }
    }

  /**
   * Returns the child Map
   */

  def getChild(mapKey: M): BAG[Option[MultiMap[M, K, V, F, BAG]]] =
    getNarrow(mapKey)

  def getChild[K2 <: K](mapKey: M, keyType: Class[K2]): BAG[Option[MultiMap[M, K2, V, F, BAG]]] =
    getNarrow(mapKey)

  def getChild[K2 <: K, V2 <: V](mapKey: M, keyType: Class[K2], valueType: Class[V2]): BAG[Option[MultiMap[M, K2, V2, F, BAG]]] =
    getNarrow(mapKey)

  def getChild[K2 <: K, V2 <: V, F2 <: F](mapKey: M, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2]): BAG[Option[MultiMap[M, K2, V2, F2, BAG]]] =
    getNarrow(mapKey)

  private def getNarrow[K2 <: K, V2 <: V, F2 <: F](mapKey: M): BAG[Option[MultiMap[M, K2, V2, F2, BAG]]] = {
    bag.map(multiMap.getKeyValueDeadline(Child(mapId, mapKey), bag)) {
      case Some(((key: Child[M], value: MultiValue.MapId), deadline)) =>
        val map =
          MultiMap[M, K, V, F, BAG](
            multiMap = multiMap,
            mapKey = key.childKey,
            mapId = value.id,
            defaultExpiration = deadline
          )

        Some(map.asInstanceOf[MultiMap[M, K2, V2, F2, BAG]])

      case Some(((key, _), value)) =>
        throw new Exception(
          s"Expected key ${classOf[Child[_]].getSimpleName}. Got ${key.getClass.getSimpleName}. " +
            s"Expected value ${classOf[MultiValue.MapId].getSimpleName}. Got ${value.getClass.getSimpleName}. "
        )

      case None =>
        None
    }
  }

  /**
   * Flatten all nest children of this map.
   *
   * Requires a [[Bag.Sync]] instead of [[Bag.Async]].
   */
  def childrenFlatten: Stream[MultiMap[M, K, V, F, BAG], BAG] =
    children flatMap {
      child =>
        Stream.join(child, child.childrenFlatten)
    }

  /**
   * Keys of all child Maps.
   */
  def childrenKeys: Stream[M, BAG] =
    multiMap
      .toSet
      .after(MultiKey.ChildrenStart(mapId))
      .takeWhile {
        case MultiKey.Child(parentMap, _) =>
          parentMap == mapId

        case _ =>
          false
      }
      .collect {
        case MultiKey.Child(_, dataKey) =>
          dataKey
      }

  def children: Stream[MultiMap[M, K, V, F, BAG], BAG] =
    childrenKeys
      .map(key => getChild(key))
      .flatten
      .collect {
        case Some(map) => map
      }

  def hasChildren: BAG[Boolean] =
    bag.transform(childrenKeys.headOrNull) {
      head =>
        head != null
    }
}
