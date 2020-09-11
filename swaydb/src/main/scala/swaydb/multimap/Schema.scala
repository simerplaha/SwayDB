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

import swaydb.core.map.counter.Counter
import swaydb.core.util.Times._
import swaydb.multimap.MultiKey.Child
import swaydb.serializers._
import swaydb.{Apply, Bag, IO, Map, MultiMap, Prepare, PureFunction, Stream}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * Provides APIs to manage children/nested maps/child maps of [[MultiMap]].
 */
class Schema[M, K, V, F, BAG[_]](innerMap: Map[MultiKey[M, K], MultiValue[V], PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]], BAG],
                                 val mapId: Long,
                                 val defaultExpiration: Option[Deadline])(implicit keySerializer: Serializer[K],
                                                                          childKeySerializer: Serializer[M],
                                                                          valueSerializer: Serializer[V],
                                                                          counter: Counter,
                                                                          bag: Bag[BAG]) {

  /**
   * Creates new or initialises the existing map.
   */
  def init[M2 <: M](mapKey: M2): BAG[MultiMap[M2, K, V, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = None, forceClear = false)

  def init[M2 <: M, K2 <: K](mapKey: M2, keyType: Class[K2]): BAG[MultiMap[M2, K2, V, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = None, forceClear = false)

  def init[M2 <: M, K2 <: K, V2 <: V](mapKey: M2, keyType: Class[K2], valueType: Class[V2]): BAG[MultiMap[M2, K2, V2, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = None, forceClear = false)

  def init[M2 <: M, K2 <: K, V2 <: V, F2 <: F](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2]): BAG[MultiMap[M2, K2, V2, F2, BAG]] =
    getOrPut(childKey = mapKey, expireAt = None, forceClear = false)

  /**
   * Creates new or initialises the existing map.
   */
  def init[M2 <: M](mapKey: M2, expireAfter: FiniteDuration): BAG[MultiMap[M2, K, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = false)

  def init[M2 <: M, K2 <: K](mapKey: M2, keyType: Class[K2], expireAfter: FiniteDuration): BAG[MultiMap[M2, K2, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = false)

  def init[M2 <: M, K2 <: K, V2 <: V](mapKey: M2, keyType: Class[K2], valueType: Class[V2], expireAfter: FiniteDuration): BAG[MultiMap[M2, K2, V2, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = false)

  def init[M2 <: M, K2 <: K, V2 <: V, F2 <: F](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2], expireAfter: FiniteDuration): BAG[MultiMap[M2, K2, V2, F2, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = false)


  /**
   * Creates new or initialises the existing map.
   */
  def init[M2 <: M](mapKey: M2, expireAt: Deadline): BAG[MultiMap[M2, K, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAt), forceClear = false)


  def init[M2 <: M, K2 <: K](mapKey: M2, keyType: Class[K2], expireAt: Deadline): BAG[MultiMap[M2, K2, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAt), forceClear = false)

  def init[M2 <: M, K2 <: K, V2 <: V](mapKey: M2, keyType: Class[K2], valueType: Class[V2], expireAt: Deadline): BAG[MultiMap[M2, K2, V2, F, BAG]] =
    getOrPut(mapKey, Some(expireAt), forceClear = false)

  def init[M2 <: M, K2 <: K, V2 <: V, F2 <: F](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2], expireAt: Deadline): BAG[MultiMap[M2, K2, V2, F2, BAG]] =
    getOrPut(mapKey, Some(expireAt), forceClear = false)


  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[init]] if clearing existing entries is not required.
   */
  def replace[M2 <: M](mapKey: M2): BAG[MultiMap[M2, K, V, F, BAG]] =
    getOrPut(mapKey, None, forceClear = true)

  def replace[M2 <: M, K2 <: K](mapKey: M2, keyType: Class[K2]): BAG[MultiMap[M2, K2, V, F, BAG]] =
    getOrPut(mapKey, None, forceClear = true)

  def replace[M2 <: M, K2 <: K, V2 <: V](mapKey: M2, keyType: Class[K2], valueType: Class[V2]): BAG[MultiMap[M2, K2, V2, F, BAG]] =
    getOrPut(mapKey, None, forceClear = true)

  def replace[M2 <: M, K2 <: K, V2 <: V, F2 <: F](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2]): BAG[MultiMap[M2, K2, V2, F2, BAG]] =
    getOrPut(mapKey, None, forceClear = true)

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[init]] if clearing existing entries is not required.
   */
  def replace[M2 <: M](mapKey: M2, expireAfter: FiniteDuration): BAG[MultiMap[M2, K, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = true)

  def replace[M2 <: M, K2 <: K](mapKey: M2, keyType: Class[K2], expireAfter: FiniteDuration): BAG[MultiMap[M2, K2, V, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = true)

  def replace[M2 <: M, K2 <: K, V2 <: V](mapKey: M2, keyType: Class[K2], valueType: Class[V2], expireAfter: FiniteDuration): BAG[MultiMap[M2, K2, V2, F, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = true)

  def replace[M2 <: M, K2 <: K, V2 <: V, F2 <: F](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2], expireAfter: FiniteDuration): BAG[MultiMap[M2, K2, V2, F2, BAG]] =
    getOrPut(mapKey, Some(expireAfter.fromNow), forceClear = true)

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[init]] if clearing existing entries is not required.
   */
  def replace[M2 <: M](mapKey: M2, expireAt: Deadline): BAG[MultiMap[M2, K, V, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = Some(expireAt), forceClear = true)

  def replace[M2 <: M, K2 <: K](mapKey: M2, keyType: Class[K2], expireAt: Deadline): BAG[MultiMap[M2, K2, V, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = Some(expireAt), forceClear = true)

  def replace[M2 <: M, K2 <: K, V2 <: V](mapKey: M2, keyType: Class[K2], valueType: Class[V2], expireAt: Deadline): BAG[MultiMap[M2, K2, V2, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = Some(expireAt), forceClear = true)

  def replace[M2 <: M, K2 <: K, V2 <: V, F2 <: F](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2], expireAt: Deadline): BAG[MultiMap[M2, K2, V2, F2, BAG]] =
    getOrPut(childKey = mapKey, expireAt = Some(expireAt), forceClear = true)

  /**
   * Clears existing entries before creating the Map.
   *
   * @note Put has slower immediate write performance for preceding key-value entries.
   *       Always use [[init]] if clearing existing entries is not required.
   */

  def replace[M2 <: M](mapKey: M2, expireAt: Option[Deadline]): BAG[MultiMap[M2, K, V, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = expireAt, forceClear = true)

  def replace[M2 <: M, K2 <: K](mapKey: M2, keyType: Class[K2], expireAt: Option[Deadline]): BAG[MultiMap[M2, K2, V, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = expireAt, forceClear = true)

  def replace[M2 <: M, K2 <: K, V2 <: V](mapKey: M2, keyType: Class[K2], valueType: Class[V2], expireAt: Option[Deadline]): BAG[MultiMap[M2, K2, V2, F, BAG]] =
    getOrPut(childKey = mapKey, expireAt = expireAt, forceClear = true)

  def replace[M2 <: M, K2 <: K, V2 <: V, F2 <: F](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2], expireAt: Option[Deadline]): BAG[MultiMap[M2, K2, V2, F2, BAG]] =
    getOrPut(childKey = mapKey, expireAt = expireAt, forceClear = true)


  /**
   * @return false if the map does not exist else true on successful remove.
   */
  def remove(mapKey: M): BAG[Boolean] =
    bag.flatMap(prepareRemove(mapKey = mapKey, expiration = None, forceClear = true, expire = false)) {
      buffer =>
        if (buffer.isEmpty)
          bag.success(false)
        else
          bag.transform(innerMap.commitIterable(buffer)) {
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
  private def getOrPut[M2 <: M, K2 <: K, V2 <: V, F2 <: F](childKey: M2, expireAt: Option[Deadline], forceClear: Boolean): BAG[MultiMap[M2, K2, V2, F2, BAG]] =
    bag.flatMap(get(childKey)) {
      case Some(_child) =>
        val childMap = _child.asInstanceOf[MultiMap[M2, K2, V2, F2, BAG]]

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

  /**
   * Flatten all nest children of this map.
   *
   * Requires a [[Bag.Sync]] instead of [[Bag.Async]].
   */
  def flatten[BAG[_]](implicit bag: Bag.Sync[BAG]): BAG[ListBuffer[MultiMap[M, K, V, F, BAG]]] =
    stream(bag).foldLeft(ListBuffer[MultiMap[M, K, V, F, BAG]]()) {
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


  private def create[M2 <: M, K2 <: K, V2 <: V, F2 <: F](childKey: M2, childId: Option[Long], expireAt: Option[Deadline], forceClear: Boolean, expire: Boolean): BAG[MultiMap[M2, K2, V2, F2, BAG]] = {
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

        bag.transform(innerMap.commitIterable(buffer)) {
          _ =>
            MultiMap(
              innerMap = innerMap,
              mapKey = childKey.asInstanceOf[M],
              mapId = childIdOrNew,
              defaultExpiration = expiration
            ).asInstanceOf[MultiMap[M2, K2, V2, F2, BAG]]
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
                            expire: Boolean): BAG[ListBuffer[Prepare[MultiKey[M, K], MultiValue[V], Nothing]]] = {
    val buffer = ListBuffer.empty[Prepare[MultiKey[M, K], MultiValue[V], Nothing]]

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
                            expire: Boolean): BAG[ListBuffer[Prepare[MultiKey[M, K], MultiValue[V], Nothing]]] =
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
  private def buildPrepareRemove(subMapKey: M, subMapId: Long, expire: Option[Deadline]): Seq[Prepare.Remove[MultiKey[M, K]]] = {
    Seq(
      Prepare.Remove(MultiKey.Child(mapId, subMapKey), None, expire),
      Prepare.Remove(MultiKey.Start(subMapId), Some(MultiKey.End(subMapId)), expire)
    )
  }

  /**
   * Builds [[Prepare.Remove]] statements for all children of this map.
   */
  private def prepareRemove[BAG[_]](expire: Option[Deadline])(implicit bag: Bag.Sync[BAG]): BAG[ListBuffer[Prepare.Remove[MultiKey[M, K]]]] =
    stream(bag).foldLeft(ListBuffer.empty[Prepare.Remove[MultiKey[M, K]]]) {
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

  def get[M2 <: M](mapKey: M2): BAG[Option[MultiMap[M2, K, V, F, BAG]]] =
    get(mapKey, bag)

  def get[M2 <: M, K2 <: K](mapKey: M2, keyType: Class[K2]): BAG[Option[MultiMap[M2, K2, V, F, BAG]]] =
    get(mapKey, bag)

  def get[M2 <: M, K2 <: K, V2 <: V](mapKey: M2, keyType: Class[K2], valueType: Class[V2]): BAG[Option[MultiMap[M2, K2, V2, F, BAG]]] =
    get(mapKey, bag)

  def get[M2 <: M, K2 <: K, V2 <: V, F2 <: F](mapKey: M2, keyType: Class[K2], valueType: Class[V2], functionType: Class[F2]): BAG[Option[MultiMap[M2, K2, V2, F2, BAG]]] =
    get(mapKey, bag)

  private def get[M2 <: M, K2 <: K, V2 <: V, F2 <: F, BAG[_]](mapKey: M2, bag: Bag[BAG]): BAG[Option[MultiMap[M2, K2, V2, F2, BAG]]] = {
    bag.map(innerMap.getKeyValueDeadline(Child(mapId, mapKey), bag)) {
      case Some(((key: Child[M2], value: MultiValue.MapId), deadline)) =>
        implicit val bag2: Bag[BAG] = bag

        Some(
          MultiMap[M, K, V, F, BAG](
            innerMap = innerMap.toBag[BAG],
            mapKey = key.childKey,
            mapId = value.id,
            defaultExpiration = deadline
          ).asInstanceOf[MultiMap[M2, K2, V2, F2, BAG]]
        )

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
   * Keys of all child Maps.
   */
  def keys: Stream[M, BAG] =
    innerMap
      .keys
      .stream
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

  //todo - flatten Options and BAG.
  def stream: Stream[BAG[Option[MultiMap[M, K, V, F, BAG]]], BAG] =
    keys.map(key => get(key))

  private def stream[BAG[_]](bag: Bag[BAG]): Stream[BAG[Option[MultiMap[M, K, V, F, BAG]]], BAG] = {
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
