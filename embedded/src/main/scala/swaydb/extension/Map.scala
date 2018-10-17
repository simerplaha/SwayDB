/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.extension

import swaydb.core.util.TryUtil
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.iterator._
import swaydb.serializers.Serializer
import swaydb.{Batch, Data, Map}

import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.{Failure, Success, Try}

private[swaydb] object Map {
  def apply[K, V](map: swaydb.Map[Key[K], Option[V]],
                  mapKey: Seq[K])(implicit keySerializer: Serializer[K],
                                  valueSerializer: Serializer[V],
                                  mapKeySerializer: Serializer[Key[K]],
                                  optionValueSerializer: Serializer[Option[V]],
                                  ordering: Ordering[Slice[Byte]]): Map[K, V] =
    new Map[K, V](map, mapKey)

  /**
    * Creates the entries range for the [[Map]]'s mapKey/mapId.
    */
  def entriesRangeKeys[K](mapKey: Seq[K]): (Key.EntriesStart[K], Key.EntriesEnd[K]) =
    (Key.EntriesStart(mapKey), Key.EntriesEnd(mapKey))

  /**
    * Fetches all range key-values for all [[Map]]s within this [[Map]].
    *
    * All key-values are stored in this format. This function creates all [[Key.Start]] to [[Key.End]]
    * ranges for the current [[Map]] and all child [[Map]].
    *
    * MapKey.Start(Seq(1))
    *   MapKey.EntriesStart(Seq(1))
    *     MapKey.Entry(Seq(1), 1)
    *   MapKey.EntriesEnd(Seq(1))
    *   MapKey.SubMapsStart(Seq(1))
    *     MapKey.SubMap(Seq(1), 1000)
    *   MapKey.SubMapsEnd(Seq(1))
    * MapKey.End(Seq(1))
    */
  def childSubMapRanges[K, V](parentMap: Map[K, V])(implicit keySerializer: Serializer[K],
                                                    mapKeySerializer: Serializer[Key[K]],
                                                    ordering: Ordering[Slice[Byte]],
                                                    valueSerializer: Serializer[V],
                                                    optionValueSerializer: Serializer[Option[V]]): List[(Key.SubMap[K], Key.Start[K], Key.End[K])] =
    parentMap.subMapsOnly().foldLeft(List.empty[(Key.SubMap[K], Key.Start[K], Key.End[K])]) {
      case (previousList, (subMapKey, _)) => {
        val subMapKeys = parentMap.mapKey :+ subMapKey
        //                  remove the subMap reference from parent         &        remove subMap block
        val keysToRemove = (Key.SubMap(parentMap.mapKey, subMapKey), Key.Start(subMapKeys), Key.End(subMapKeys))
        previousList :+ keysToRemove
      } ++ {
        childSubMapRanges(
          Map[K, V](
            map = parentMap.innerMap(),
            mapKey = parentMap.mapKey :+ subMapKey
          )
        )
      }
    }

  /**
    * Build [[Batch.Remove]] for the input [[Key]] ranges.
    */
  def toBatchRemove[K](batches: Iterable[(Key.SubMap[K], Key.Start[K], Key.End[K])]): Iterable[Data.Remove[Key[K]]] =
    batches flatMap {
      case (subMap, start, end) =>
        Seq(Batch.Remove(subMap: Key[K]), Batch.Remove(start: Key[K], end: Key[K]))
    }

  /**
    * Returns batch entries to create a new [[Map]].
    *
    * Note: If the map already exists, it will be removed including all it's child maps similar to a in-memory [[scala.collection.mutable.Map]].
    */
  def putMap[K, V](map: swaydb.Map[Key[K], Option[V]],
                   mapKey: Seq[K],
                   value: Option[V])(implicit keySerializer: Serializer[K],
                                     mapKeySerializer: Serializer[Key[K]],
                                     valueSerializer: Serializer[V],
                                     optionValueSerializer: Serializer[Option[V]],
                                     ordering: Ordering[Slice[Byte]]): Try[Iterable[Batch[Key[K], Option[V]]]] = {

    //batch to remove all SubMaps.
    val removeSubMapsBatches =
      toBatchRemove(childSubMapRanges(parentMap = Map[K, V](map, mapKey)))

    val (thisMapEntriesStart, thisMapEntriesEnd) = Map.entriesRangeKeys(mapKey)

    //mapKey should have at least one key. A mapKey with only 1 key indicates that it's for the rootMap.
    mapKey.lastOption map {
      last =>
        Try {
          removeSubMapsBatches ++
            Seq(
              //add subMap entry to parent Map's key
              Batch.Put(Key.SubMap(mapKey.dropRight(1), last), None),
              Batch.Remove(thisMapEntriesStart, thisMapEntriesEnd), //remove all exiting entries
              //value only needs to be set for Start.
              Batch.Put(Key.Start(mapKey), value),
              //values should be None for the following batch entries because they are iteration purposes only and values for
              //entries are never read.
              Batch.Put(Key.EntriesStart(mapKey), None),
              Batch.Put(Key.EntriesEnd(mapKey), None),
              Batch.Put(Key.SubMapsStart(mapKey), None),
              Batch.Put(Key.SubMapsEnd(mapKey), None),
              Batch.Put(Key.End(mapKey), None)
            )
        }
    } getOrElse {
      Failure(new Exception("Cannot put map with empty key."))
    }
  }

  def updateMapValue[K, V](mapKey: Seq[K],
                           value: V)(implicit keySerializer: Serializer[K],
                                     mapKeySerializer: Serializer[Key[K]],
                                     valueSerializer: Serializer[V],
                                     ordering: Ordering[Slice[Byte]]): Seq[Data.Put[Key[K], Option[V]]] =
    Seq(Batch.Put(Key.Start(mapKey), Some(value)))

  def removeMap[K, V](map: swaydb.Map[Key[K], Option[V]],
                      mapKey: Seq[K])(implicit keySerializer: Serializer[K],
                                      mapKeySerializer: Serializer[Key[K]],
                                      valueSerializer: Serializer[V],
                                      optionValueSerializer: Serializer[Option[V]],
                                      ordering: Ordering[Slice[Byte]]): Seq[Data.Remove[Key[K]]] =
    Seq[Data.Remove[Key[K]]](
      Batch.Remove(Key.SubMap[K](mapKey.dropRight(1), mapKey.last)), //remove the subMap entry from parent Map i.e this
      Batch.Remove(Key.Start[K](mapKey), Key.End[K](mapKey)) //remove the subMap itself
    ) ++ {
      //fetch all child subMaps from the subMap being removed and batch remove them.
      Map.toBatchRemove(Map.childSubMapRanges(Map[K, V](map, mapKey)))
    }
}

/**
  * Key-value or Map database API.
  *
  * For documentation check - http://swaydb.io/api/
  */
class Map[K, V](map: swaydb.Map[Key[K], Option[V]],
                mapKey: Seq[K])(implicit keySerializer: Serializer[K],
                                mapKeySerializer: Serializer[Key[K]],
                                ordering: Ordering[Slice[Byte]],
                                valueSerializerOption: Serializer[Option[V]],
                                valueSerializer: Serializer[V]) extends SubMapIterator[K, Option[V]](mapKey, dbIterator = DBIterator[Key[K], Option[V]](map.db, Some(From(Key.Start(mapKey), orAfter = false, orBefore = false, before = false, after = true)))) {

  def putMap(key: K, value: V): Try[Map[K, V]] = {
    val subMapKey = mapKey :+ key
    Map.putMap[K, V](
      map = map,
      mapKey = subMapKey,
      value = Some(value)
    ) flatMap {
      batches =>
        map.batch(batches) map {
          _ =>
            Map[K, V](
              map = map,
              mapKey = subMapKey
            )
        }
    }
  }

  def updateMapValue(key: K, value: V): Try[Map[K, V]] = {
    val subMapKey = mapKey :+ key
    map.batch {
      Map.updateMapValue[K, V](
        mapKey = subMapKey,
        value = value
      )
    } map {
      _ =>
        Map[K, V](
          map = map,
          mapKey = subMapKey
        )
    }
  }

  def getMap(key: K): Try[Option[Map[K, V]]] = {
    containsMap(key) map {
      exists =>
        if (exists)
          Some(
            Map[K, V](
              map = map,
              mapKey = mapKey :+ key
            )
          )
        else
          None
    }
  }

  def containsMap(key: K): Try[Boolean] =
    map.contains(Key.Start(mapKey :+ key))

  def updateValue(value: V): Try[Map[K, V]] =
    map.batch {
      Map.updateMapValue[K, V](
        mapKey = mapKey,
        value = value
      )
    } map {
      _ =>
        Map[K, V](
          map = map,
          mapKey = mapKey
        )
    }

  def exists(): Try[Boolean] =
    map.contains(Key.Start(mapKey))

  def getValue(): Try[Option[V]] =
    map.get(Key.Start(mapKey)) flatMap {
      case Some(value) =>
        Success(value)
      case None =>
        Failure(new Exception("Map does not exist."))
    }

  def getMapValue(key: K): Try[Option[V]] =
    map.get(Key.Start(mapKey :+ key)) flatMap {
      case Some(value) =>
        Success(value)
      case None =>
        Failure(new Exception("Map does not exist."))
    }

  def removeMap(key: K): Try[Level0Meter] =
    map.batch(Map.removeMap(map, mapKey :+ key))

  def put(key: K, value: V): Try[Level0Meter] =
    map.put(key = Key.Entry(mapKey, key), value = Some(value))

  def put(key: K, value: V, expireAfter: FiniteDuration): Try[Level0Meter] =
    map.put(Key.Entry(mapKey, key), Some(value), expireAfter.fromNow)

  def put(key: K, value: V, expireAt: Deadline): Try[Level0Meter] =
    map.put(Key.Entry(mapKey, key), Some(value), expireAt)

  def remove(key: K): Try[Level0Meter] =
    map.remove(Key.Entry(mapKey, key))

  def remove(from: K, to: K): Try[Level0Meter] =
    map.remove(Key.Entry(mapKey, from), Key.Entry(mapKey, to))

  def removeKeyValues(): Try[Level0Meter] = {
    val (start, end) = Map.entriesRangeKeys(mapKey)
    map.remove(start, end)
  }

  def expire(key: K, after: FiniteDuration): Try[Level0Meter] =
    map.expire(Key.Entry(mapKey, key), after.fromNow)

  def expire(key: K, at: Deadline): Try[Level0Meter] =
    map.expire(Key.Entry(mapKey, key), at)

  def expire(from: K, to: K, after: FiniteDuration): Try[Level0Meter] =
    map.expire(Key.Entry(mapKey, from), Key.Entry(mapKey, to), after.fromNow)

  def expire(from: K, to: K, at: Deadline): Try[Level0Meter] =
    map.expire(Key.Entry(mapKey, from), Key.Entry(mapKey, to), at)

  def update(key: K, value: V): Try[Level0Meter] =
    map.update(Key.Entry(mapKey, key), Some(value))

  def update(from: K, to: K, value: V): Try[Level0Meter] =
    map.update(Key.Entry(mapKey, from), Key.Entry(mapKey, to), Some(value))

  def batch(batch: Batch[K, V]*): Try[Level0Meter] =
    this.batch(batch)

  def batch(batch: Iterable[Batch[K, V]]): Try[Level0Meter] =
    map.batch(
      batch map {
        case Data.Put(key, value, deadline) =>
          Data.Put(Key.Entry(mapKey, key), value = Some(value), deadline = deadline)
        case Data.Remove(from, to, deadline) =>
          Data.Remove(from = Key.Entry(mapKey, from), to = to.map(Key.Entry(mapKey, _)), deadline = deadline)
        case Data.Update(from, to, value) =>
          Data.Update(from = Key.Entry(mapKey, from), to = to.map(Key.Entry(mapKey, _)), value = Some(value))
        case Data.Add(elem, deadline) =>
          Data.Add(elem = Key.Entry(mapKey, elem), deadline = deadline)
      }
    )

  def batchPut(keyValues: (K, V)*): Try[Level0Meter] =
    batchPut(keyValues)

  def batchPut(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    map.batchPut {
      keyValues map {
        case (key, value) =>
          (Key.Entry(mapKey, key), Some(value))
      }
    }

  def batchUpdate(keyValues: (K, V)*): Try[Level0Meter] =
    batchUpdate(keyValues)

  def batchUpdate(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    map.batchUpdate {
      keyValues map {
        case (key, value) =>
          (Key.Entry(mapKey, key), Some(value))
      }
    }

  def batchRemove(keys: K*): Try[Level0Meter] =
    batchRemove(keys)

  def batchRemove(keys: Iterable[K]): Try[Level0Meter] =
    map.batchRemove(keys.map(key => Key.Entry(mapKey, key)))

  def batchExpire(keys: (K, Deadline)*): Try[Level0Meter] =
    batchExpire(keys)

  def batchExpire(keys: Iterable[(K, Deadline)]): Try[Level0Meter] =
    map.batchExpire(keys.map(keyDeadline => (Key.Entry(mapKey, keyDeadline._1), keyDeadline._2)))

  /**
    * Returns target value for the input key.
    */
  def get(key: K): Try[Option[V]] =
    map.get(Key.Entry(mapKey, key)) flatMap {
      case Some(value) =>
        Success(value)
      case None =>
        Failure(new Exception("Map does not exist."))
    }

  /**
    * Returns target full key for the input partial key.
    *
    * This function is mostly used for Set databases where partial ordering on the Key is provided.
    */
  def getKey(key: K): Try[Option[K]] =
    map.getKey(Key.Entry(mapKey, key)) flatMap {
      case Some(key) =>
        key match {
          case Key.Entry(_, dataKey) =>
            Success(Some(dataKey))
          case got =>
            Failure(new Exception(s"Unable to fetch key. Got: $got expected MapKey.Entry"))
        }
      case None =>
        TryUtil.successNone
    }

  def getKeyValue(key: K): Try[Option[(K, V)]] =
    map.getKeyValue(Key.Entry(mapKey, key)) flatMap {
      case Some((key, value)) =>
        key match {
          case Key.Entry(_, dataKey) =>
            value map {
              value =>
                Success(Some(dataKey, value))
            } getOrElse {
              Failure(new Exception("Value does not exist."))
            }

          case got =>
            Failure(new Exception(s"Unable to fetch keyValue. Got: $got expected MapKey.Entry"))
        }
      case None =>
        TryUtil.successNone
    }

  def keys: SubMapKeysIterator[K] =
    SubMapKeysIterator[K](
      mapKey = mapKey,
      keysIterator =
        DBKeysIterator[Key[K]](
          db = map.db,
          from = Some(From(Key.Start(mapKey), orAfter = false, orBefore = false, before = false, after = true))
        )
    )

  def contains(key: K): Try[Boolean] =
    map contains Key.Entry(mapKey, key)

  def mightContain(key: K): Try[Boolean] =
    map mightContain Key.Entry(mapKey, key)

  def level0Meter: Level0Meter =
    map.level0Meter

  def level1Meter: LevelMeter =
    map.level1Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    map.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    map.sizeOfSegments

  def keySize(key: K): Int =
    map keySize Key.Entry(mapKey, key)

  def valueSize(value: V): Int =
    map valueSize Some(value)

  def expiration(key: K): Try[Option[Deadline]] =
    map expiration Key.Entry(mapKey, key)

  def timeLeft(key: K): Try[Option[FiniteDuration]] =
    expiration(key).map(_.map(_.timeLeft))

  private[swaydb] def innerMap() =
    map

}