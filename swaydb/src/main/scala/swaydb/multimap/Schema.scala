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

import swaydb.MultiMapKey.{MapStart, SubMap}
import swaydb.core.util.Times._
import swaydb.serializers._
import swaydb.Stream
import swaydb.core.map.counter.Counter
import swaydb.{Apply, Bag, IO, Map, MultiMapKey, MultiMap_Experimental, Prepare, PureFunction}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * Provides APIs to manage children/nested maps/child maps of [[MultiMap_Experimental]].
 */
class Schema[M, K, V, F, BAG[_]](innerMap: Map[MultiMapKey[M, K], MultiValue[V], PureFunction[MultiMapKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]], BAG],
                                 val mapId: Long,
                                 val defaultExpiration: Option[Deadline])(implicit keySerializer: Serializer[K],
                                                                          tableSerializer: Serializer[M],
                                                                          valueSerializer: Serializer[V],
                                                                          counter: Counter,
                                                                          bag: Bag[BAG]) {

  /**
   * Creates new or initialises the existing map.
   */
  def init[M2](mapKey: M2)(implicit evT: M2 <:< M): BAG[MultiMap_Experimental[M2, K, V, F, BAG]] =
    getOrPut(mapKey = mapKey, expireAt = None, forceClear = false)

  def init[M2, K2](mapKey: M2, keyType: Class[K2])(implicit evT: M2 <:< M,
                                                   evK: K2 <:< K): BAG[MultiMap_Experimental[M2, K2, V, F, BAG]] =
    getOrPut(mapKey = mapKey, expireAt = None, forceClear = false)

  def init[M2, K2, V2](mapKey: M2, keyType: Class[K2], valueType: Class[V2])(implicit evT: M2 <:< M,
                                                                             evK: K2 <:< K,
                                                                             evV: V2 <:< V): BAG[MultiMap_Experimental[M2, K2, V2, F, BAG]] =
    getOrPut(mapKey = mapKey, expireAt = None, forceClear = false)

  def init[M2, K2, V2, F2](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2])(implicit evT: M2 <:< M,
                                                                                                          evK: K2 <:< K,
                                                                                                          evV: V2 <:< V,
                                                                                                          evF: F2 <:< F): BAG[MultiMap_Experimental[M2, K2, V2, F2, BAG]] =
    getOrPut(mapKey = mapKey, expireAt = None, forceClear = false)

  /**
   * Creates new or initialises the existing map.
   */
  def init[M2](mapKey: M2, expireAfter: FiniteDuration)(implicit evT: M2 <:< M): BAG[MultiMap_Experimental[M2, K, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = false)

  def init[M2, K2](mapKey: M2, keyType: Class[K2], expireAfter: FiniteDuration)(implicit evT: M2 <:< M,
                                                                                evK: K2 <:< K): BAG[MultiMap_Experimental[M2, K2, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = false)

  def init[M2, K2, V2](mapKey: M2, keyType: Class[K2], valueType: Class[V2], expireAfter: FiniteDuration)(implicit evT: M2 <:< M,
                                                                                                          evK: K2 <:< K,
                                                                                                          evV: V2 <:< V): BAG[MultiMap_Experimental[M2, K2, V2, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = false)

  def init[M2, K2, V2, F2](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2], expireAfter: FiniteDuration)(implicit evT: M2 <:< M,
                                                                                                                                       evK: K2 <:< K,
                                                                                                                                       evV: V2 <:< V,
                                                                                                                                       evF: F2 <:< F): BAG[MultiMap_Experimental[M2, K2, V2, F2, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = false)


  /**
   * Creates new or initialises the existing map.
   */
  def init[M2](mapKey: M2, expireAt: Deadline)(implicit evT: M2 <:< M): BAG[MultiMap_Experimental[M2, K, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAt), forceClear = false)


  def init[M2, K2](mapKey: M2, keyType: Class[K2], expireAt: Deadline)(implicit evT: M2 <:< M,
                                                                       evK: K2 <:< K): BAG[MultiMap_Experimental[M2, K2, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAt), forceClear = false)

  def init[M2, K2, V2](mapKey: M2, keyType: Class[K2], valueType: Class[V2], expireAt: Deadline)(implicit evT: M2 <:< M,
                                                                                                 evK: K2 <:< K,
                                                                                                 evV: V2 <:< V): BAG[MultiMap_Experimental[M2, K2, V2, F, BAG]] =
    getOrPut(mapKey, Some(expireAt), forceClear = false)

  def init[M2, K2, V2, F2](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2], expireAt: Deadline)(implicit evT: M2 <:< M,
                                                                                                                              evK: K2 <:< K,
                                                                                                                              evV: V2 <:< V,
                                                                                                                              evF: F2 <:< F): BAG[MultiMap_Experimental[M2, K2, V2, F2, BAG]] =
    getOrPut(mapKey, Some(expireAt), forceClear = false)


  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[init]] if clearing existing entries is not required.
   */
  def replace[M2](mapKey: M2)(implicit evT: M2 <:< M): BAG[MultiMap_Experimental[M2, K, V, F, BAG]] =
    getOrPut(mapKey, None, forceClear = true)

  def replace[M2, K2](mapKey: M2, keyType: Class[K2])(implicit evT: M2 <:< M,
                                                      evK: K2 <:< K): BAG[MultiMap_Experimental[M2, K2, V, F, BAG]] =
    getOrPut(mapKey, None, forceClear = true)

  def replace[M2, K2, V2](mapKey: M2, keyType: Class[K2], valueType: Class[V2])(implicit evT: M2 <:< M,
                                                                                evK: K2 <:< K,
                                                                                evV: V2 <:< V): BAG[MultiMap_Experimental[M2, K2, V2, F, BAG]] =
    getOrPut(mapKey, None, forceClear = true)

  def replace[M2, K2, V2, F2](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2])(implicit evT: M2 <:< M,
                                                                                                             evK: K2 <:< K,
                                                                                                             evV: V2 <:< V,
                                                                                                             evF: F2 <:< F): BAG[MultiMap_Experimental[M2, K2, V2, F2, BAG]] =
    getOrPut(mapKey, None, forceClear = true)

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[init]] if clearing existing entries is not required.
   */
  def replace[M2](mapKey: M2, expireAfter: FiniteDuration)(implicit evT: M2 <:< M): BAG[MultiMap_Experimental[M2, K, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = true)

  def replace[M2, K2](mapKey: M2, keyType: Class[K2], expireAfter: FiniteDuration)(implicit evT: M2 <:< M,
                                                                                   evK: K2 <:< K): BAG[MultiMap_Experimental[M2, K2, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = true)

  def replace[M2, K2, V2](mapKey: M2, keyType: Class[K2], valueType: Class[V2], expireAfter: FiniteDuration)(implicit evT: M2 <:< M,
                                                                                                             evK: K2 <:< K,
                                                                                                             evV: V2 <:< V): BAG[MultiMap_Experimental[M2, K2, V2, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = true)

  def replace[M2, K2, V2, F2](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2], expireAfter: FiniteDuration)(implicit evT: M2 <:< M,
                                                                                                                                          evK: K2 <:< K,
                                                                                                                                          evV: V2 <:< V,
                                                                                                                                          evF: F2 <:< F): BAG[MultiMap_Experimental[M2, K2, V2, F2, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = true)

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[init]] if clearing existing entries is not required.
   */
  def replace[M2](mapKey: M2, expireAt: Deadline)(implicit evT: M2 <:< M): BAG[MultiMap_Experimental[M2, K, V, F, BAG]] =
    getOrPut(mapKey = mapKey, expireAt = Some(expireAt), forceClear = true)

  def replace[M2, K2](mapKey: M2, keyType: Class[K2], expireAt: Deadline)(implicit evT: M2 <:< M,
                                                                          evK: K2 <:< K): BAG[MultiMap_Experimental[M2, K2, V, F, BAG]] =
    getOrPut(mapKey = mapKey, expireAt = Some(expireAt), forceClear = true)

  def replace[M2, K2, V2](mapKey: M2, keyType: Class[K2], valueType: Class[V2], expireAt: Deadline)(implicit evT: M2 <:< M,
                                                                                                    evK: K2 <:< K,
                                                                                                    evV: V2 <:< V): BAG[MultiMap_Experimental[M2, K2, V2, F, BAG]] =
    getOrPut(mapKey = mapKey, expireAt = Some(expireAt), forceClear = true)

  def replace[M2, K2, V2, F2](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2], expireAt: Deadline)(implicit evT: M2 <:< M,
                                                                                                                                 evK: K2 <:< K,
                                                                                                                                 evV: V2 <:< V,
                                                                                                                                 evF: F2 <:< F): BAG[MultiMap_Experimental[M2, K2, V2, F2, BAG]] =
    getOrPut(mapKey = mapKey, expireAt = Some(expireAt), forceClear = true)

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[init]] if clearing existing entries is not required.
   */

  def replace[M2](mapKey: M2, expireAt: Option[Deadline])(implicit evT: M2 <:< M): BAG[MultiMap_Experimental[M2, K, V, F, BAG]] =
    getOrPut(mapKey = mapKey, expireAt = expireAt, forceClear = true)

  def replace[M2, K2](mapKey: M2, keyType: Class[K2], expireAt: Option[Deadline])(implicit evT: M2 <:< M,
                                                                                  evK: K2 <:< K): BAG[MultiMap_Experimental[M2, K2, V, F, BAG]] =
    getOrPut(mapKey = mapKey, expireAt = expireAt, forceClear = true)

  def replace[M2, K2, V2](mapKey: M2, keyType: Class[K2], valueType: Class[V2], expireAt: Option[Deadline])(implicit evT: M2 <:< M,
                                                                                                            evK: K2 <:< K,
                                                                                                            evV: V2 <:< V): BAG[MultiMap_Experimental[M2, K2, V2, F, BAG]] =
    getOrPut(mapKey = mapKey, expireAt = expireAt, forceClear = true)

  def replace[M2, K2, V2, F2](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2], expireAt: Option[Deadline])(implicit evT: M2 <:< M,
                                                                                                                                         evK: K2 <:< K,
                                                                                                                                         evV: V2 <:< V,
                                                                                                                                         evF: F2 <:< F): BAG[MultiMap_Experimental[M2, K2, V2, F2, BAG]] =
    getOrPut(mapKey = mapKey, expireAt = expireAt, forceClear = true)


  def remove(mapKey: M): BAG[Boolean] =
    bag.flatMap(prepareRemove(mapKey = mapKey, expiration = None, forceClear = true, expire = false)) {
      buffer =>
        if (buffer.isEmpty)
          bag.success(false)
        else
          bag.transform(innerMap.commit(buffer)) {
            _ =>
              true
          }
    }

  /**
   * Inserts a child map to this [[MultiMap_Experimental]].
   *
   * @param mapKey     key assign to the child
   * @param expireAt   expiration
   * @param forceClear if true, removes all existing entries before initialising the child Map.
   *                   Clear uses a range entry to clear existing key-values and inserts to [[Range]]
   *                   in [[swaydb.core.level.zero.LevelZero]]'s [[Map]] entries can be slower because it
   *                   requires skipList to be cloned on each insert. As the compaction progresses the
   *                   range entries will get applied the performance goes back to normal. But try to avoid
   *                   using clear.
   */
  private def getOrPut[M2, K2, V2, F2](mapKey: M2, expireAt: Option[Deadline], forceClear: Boolean)(implicit evT: M2 <:< M,
                                                                                                    evK: K2 <:< K,
                                                                                                    evV: V2 <:< V,
                                                                                                    evF: F2 <:< F): BAG[MultiMap_Experimental[M2, K2, V2, F2, BAG]] =
    bag.flatMap(get(mapKey)) {
      case Some(_map) =>
        val map = _map.asInstanceOf[MultiMap_Experimental[M2, K2, V2, F2, BAG]]

        if (forceClear)
          create(mapKey = mapKey, expireAt = expireAt, forceClear = forceClear, expire = false)
        else
          expireAt match {
            case Some(updatedExpiration) =>
              val newExpiration = defaultExpiration earlier updatedExpiration
              //if the expiration is not updated return the map.
              if (defaultExpiration contains newExpiration)
                bag.success(map)
              else // expiration is updated perform create.
                create(mapKey = mapKey, expireAt = Some(newExpiration), forceClear = false, expire = true)

            case None =>
              bag.success(map)
          }

      case None =>
        create(mapKey = mapKey, expireAt = expireAt, forceClear = false, expire = false)
    }

  /**
   * Flatten all nest children of this map.
   *
   * Requires a [[Bag.Sync]] instead of [[Bag.Async]].
   */
  def flatten[BAG[_]](implicit bag: Bag.Sync[BAG]): BAG[ListBuffer[MultiMap_Experimental[M, K, V, F, BAG]]] =
    stream(bag).foldLeft(ListBuffer[MultiMap_Experimental[M, K, V, F, BAG]]()) {
      case (buffer, childBag) =>
        val child = bag.getUnsafe(childBag)

        child foreach {
          child =>
            buffer += child
            val children = bag.getUnsafe(child.schema.flatten[BAG])
            buffer ++= children
        }

        buffer
    }


  private def create[M2, K2, V2, F2](mapKey: M2, expireAt: Option[Deadline], forceClear: Boolean, expire: Boolean)(implicit evT: M2 <:< M,
                                                                                                                   evK: K2 <:< K,
                                                                                                                   evV: V2 <:< V,
                                                                                                                   evF: F2 <:< F): BAG[MultiMap_Experimental[M2, K2, V2, F2, BAG]] = {
    val expiration = expireAt earlier defaultExpiration

    val buffer = prepareRemove(mapKey = mapKey, expiration = expiration, forceClear = forceClear, expire = expire)

    bag.flatMap(buffer) {
      buffer =>
        val nextMapId = counter.next
        buffer += Prepare.Put(MultiMapKey.SubMap[M](mapId, mapKey), MultiValue.MapId(nextMapId), expiration)
        buffer += Prepare.Put(MultiMapKey.MapStart(nextMapId), MultiValue.None, expiration)
        buffer += Prepare.Put(MultiMapKey.MapEntriesStart(nextMapId), MultiValue.None, expiration)
        buffer += Prepare.Put(MultiMapKey.MapEntriesEnd(nextMapId), MultiValue.None, expiration)
        buffer += Prepare.Put(MultiMapKey.SubMapsStart(nextMapId), MultiValue.None, expiration)
        buffer += Prepare.Put(MultiMapKey.SubMapsEnd(nextMapId), MultiValue.None, expiration)
        buffer += Prepare.Put(MultiMapKey.MapEnd(nextMapId), MultiValue.None, expiration)

        bag.transform(innerMap.commit(buffer)) {
          _ =>
            MultiMap_Experimental(
              innerMap = innerMap,
              mapKey = mapKey.asInstanceOf[M],
              mapId = nextMapId,
              defaultExpiration = expiration
            ).asInstanceOf[MultiMap_Experimental[M2, K2, V2, F2, BAG]]
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
  private def prepareRemove(expiration: Option[Deadline],
                            forceClear: Boolean,
                            expire: Boolean): BAG[ListBuffer[Prepare[MultiMapKey[M, K], MultiValue[V], Nothing]]] = {
    val buffer = ListBuffer.empty[Prepare[MultiMapKey[M, K], MultiValue[V], Nothing]]

    //todo - use the map level BAG instead synchronous IO.ApiIO.
    if (forceClear || expire) {
      //ignore expiry if forceClear is set to true. ForceClear should remove instead of just setting a new expiry.
      val prepareRemoveExpiry =
        if (!forceClear && expire)
          expiration
        else
          None

      prepareRemove[IO.ApiIO](prepareRemoveExpiry) match {
        case IO.Right(removes) =>
          buffer ++= removes
          bag.success(buffer)

        case IO.Left(value) =>
          bag.failure(value.exception)
      }
    }
    else
      bag.success(buffer)
  }


  /**
   * Builds [[Prepare.Remove]] statements to remove the key's map and all that key's children.
   */
  private def prepareRemove(mapKey: M,
                            expiration: Option[Deadline],
                            forceClear: Boolean,
                            expire: Boolean): BAG[ListBuffer[Prepare[MultiMapKey[M, K], MultiValue[V], Nothing]]] =
    bag.flatMap(get(mapKey)) {
      case Some(child) =>
        val buffer = child.schema.prepareRemove(expiration = expiration, forceClear = forceClear, expire = expire)

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
  private def buildPrepareRemove(subMapKey: M, subMapId: Long, expire: Option[Deadline]): Seq[Prepare.Remove[MultiMapKey[M, K]]] = {
    Seq(
      Prepare.Remove(MultiMapKey.SubMap(mapId, subMapKey), None, expire),
      Prepare.Remove(MultiMapKey.MapStart(subMapId), Some(MultiMapKey.MapEnd(subMapId)), expire)
    )
  }

  /**
   * Builds [[Prepare.Remove]] statements for all children of this map.
   */
  private def prepareRemove[BAG[_]](expire: Option[Deadline])(implicit bag: Bag.Sync[BAG]): BAG[ListBuffer[Prepare.Remove[MultiMapKey[M, K]]]] =
    stream(bag).foldLeft(ListBuffer.empty[Prepare.Remove[MultiMapKey[M, K]]]) {
      case (buffer, childBag) =>
        val child = bag.getUnsafe(childBag)

        child foreach {
          child =>
            buffer ++= buildPrepareRemove(child.mapKey, child.mapId, expire)
            val childPrepares = bag.getUnsafe(child.schema.prepareRemove(expire))
            buffer ++= childPrepares
        }

        buffer
    }

  /**
   * Returns the child Map
   */

  def get[M2](mapKey: M2)(implicit evT: M2 <:< M): BAG[Option[MultiMap_Experimental[M2, K, V, F, BAG]]] =
    get(mapKey, bag)

  def get[M2, K2](mapKey: M2, keyType: Class[K2])(implicit evT: M2 <:< M,
                                                  evK: K2 <:< K): BAG[Option[MultiMap_Experimental[M2, K2, V, F, BAG]]] =
    get(mapKey, bag)

  def get[M2, K2, V2](mapKey: M2, keyType: Class[K2], valueType: Class[V2])(implicit evT: M2 <:< M,
                                                                            evK: K2 <:< K,
                                                                            evV: V2 <:< V): BAG[Option[MultiMap_Experimental[M2, K2, V2, F, BAG]]] =
    get(mapKey, bag)

  def get[M2, K2, V2, F2](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2])(implicit evT: M2 <:< M,
                                                                                                         evK: K2 <:< K,
                                                                                                         evV: V2 <:< V,
                                                                                                         evF: F2 <:< F): BAG[Option[MultiMap_Experimental[M2, K2, V2, F2, BAG]]] =
    get(mapKey, bag)

  private def get[M2, K2, V2, F2, BAG[_]](mapKey: M2, bag: Bag[BAG])(implicit evT: M2 <:< M,
                                                                     evK: K2 <:< K,
                                                                     evV: V2 <:< V,
                                                                     evF: F2 <:< F): BAG[Option[MultiMap_Experimental[M2, K2, V2, F2, BAG]]] = {
    bag.map(innerMap.getKeyValueDeadline(SubMap(mapId, mapKey), bag)) {
      case Some(((key: SubMap[M2], deadline), value: MultiValue.MapId)) =>
        implicit val bag2: Bag[BAG] = bag

        Some(
          MultiMap_Experimental[M, K, V, F, BAG](
            innerMap = innerMap.toBag[BAG],
            mapKey = key.subMapKey,
            mapId = value.id,
            defaultExpiration = deadline
          ).asInstanceOf[MultiMap_Experimental[M2, K2, V2, F2, BAG]]
        )

      case Some(((key, _), value)) =>
        throw new Exception(
          s"Expected key ${classOf[SubMap[_]].getSimpleName}. Got ${key.getClass.getSimpleName}. " +
            s"Expected value ${classOf[MultiValue.MapId].getSimpleName}. Got ${value.getClass.getSimpleName}. "
        )

      case None =>
        None
    }
  }

  /**
   * Keys of all child Maps.
   */
  def keys: Stream[M, BAG] =
    innerMap
      .keys
      .stream
      .after(MultiMapKey.SubMapsStart(mapId))
      .takeWhile {
        case MultiMapKey.SubMap(parentMap, _) =>
          parentMap == mapId

        case _ =>
          false
      }
      .collect {
        case MultiMapKey.SubMap(_, dataKey) =>
          dataKey
      }

  //todo - flatten Options and BAG.
  def stream: Stream[BAG[Option[MultiMap_Experimental[M, K, V, F, BAG]]], BAG] =
    keys.map(key => get(key))

  private def stream[BAG[_]](bag: Bag[BAG]): Stream[BAG[Option[MultiMap_Experimental[M, K, V, F, BAG]]], BAG] = {
    val free = keys.free.map((key: M) => get[M, K, V, F, BAG](mapKey = key, bag = bag))
    new Stream(free)(bag)
  }

  def isEmpty: BAG[Boolean] =
    bag.transform(keys.headOrNull) {
      head =>
        head == null
    }

  def nonEmpty: BAG[Boolean] =
    bag.transform(isEmpty)(!_)
}
