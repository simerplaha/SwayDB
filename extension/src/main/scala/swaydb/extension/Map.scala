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
import swaydb.extension.iterator.{MapIterator, MapKeysIterator}
import swaydb.serializers.Serializer
import swaydb.{Batch, Data, From}
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.{Failure, Success, Try}
import swaydb.data.order.KeyOrder

private[swaydb] object Map {
  def apply[K, V](map: swaydb.Map[Key[K], Option[V]],
                  mapKey: Seq[K])(implicit keySerializer: Serializer[K],
                                  valueSerializer: Serializer[V],
                                  mapKeySerializer: Serializer[Key[K]],
                                  optionValueSerializer: Serializer[Option[V]],
                                  keyOrder: KeyOrder[Slice[Byte]]): Map[K, V] =
    new Map[K, V](map, mapKey)

  /**
    * Creates the entries range for the [[Map]]'s mapKey/mapId.
    */
  def entriesRangeKeys[K](mapKey: Seq[K]): (Key.MapEntriesStart[K], Key.MapEntriesEnd[K]) =
    (Key.MapEntriesStart(mapKey), Key.MapEntriesEnd(mapKey))

  /**
    * Fetches all range key-values for all [[Map]]s within this [[Map]].
    *
    * All key-values are stored in this format. This function creates all [[Key.MapStart]] to [[Key.MapEnd]]
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
                                                    keyOrder: KeyOrder[Slice[Byte]],
                                                    valueSerializer: Serializer[V],
                                                    optionValueSerializer: Serializer[Option[V]]): List[(Key.SubMap[K], Key.MapStart[K], Key.MapEnd[K])] =
    parentMap.maps.foldLeft(List.empty[(Key.SubMap[K], Key.MapStart[K], Key.MapEnd[K])]) {
      case (previousList, (subMapKey, _)) => {
        val subMapKeys = parentMap.mapKey :+ subMapKey
        //                  remove the subMap reference from parent         &        remove subMap block
        val keysToRemove = (Key.SubMap(parentMap.mapKey, subMapKey), Key.MapStart(subMapKeys), Key.MapEnd(subMapKeys))
        previousList :+ keysToRemove
      } ++ {
        childSubMapRanges(
          Map[K, V](
            map = parentMap.baseMap(),
            mapKey = parentMap.mapKey :+ subMapKey
          )
        )
      }
    }

  /**
    * Build [[Batch.Remove]] for the input [[Key]] ranges.
    */
  def toBatchRemove[K](batches: Iterable[(Key.SubMap[K], Key.MapStart[K], Key.MapEnd[K])]): Iterable[Data.Remove[Key[K]]] =
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
                                     keyOrder: KeyOrder[Slice[Byte]]): Try[Iterable[Batch[Key[K], Option[V]]]] = {

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
              Batch.Put(Key.SubMap(mapKey.dropRight(1), last), value),
              Batch.Remove(thisMapEntriesStart, thisMapEntriesEnd), //remove all exiting entries
              //value only needs to be set for Start.
              Batch.Put(Key.MapStart(mapKey), value),
              //values should be None for the following batch entries because they are iteration purposes only and values for
              //entries are never read.
              Batch.Put(Key.MapEntriesStart(mapKey), None),
              Batch.Put(Key.MapEntriesEnd(mapKey), None),
              Batch.Put(Key.SubMapsStart(mapKey), None),
              Batch.Put(Key.SubMapsEnd(mapKey), None),
              Batch.Put(Key.MapEnd(mapKey), None)
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
                                     keyOrder: KeyOrder[Slice[Byte]]): Seq[Data.Put[Key[K], Option[V]]] =

    mapKey.lastOption map {
      last =>
        Seq[Data.Put[Key[K], Option[V]]](
          Batch.Put(Key.SubMap(mapKey.dropRight(1), last), Some(value)),
          Batch.Put(Key.MapStart(mapKey), Option(value))
        )
    } getOrElse {
      Seq(Batch.Put(Key.MapStart(mapKey), Option(value)))
    }

  def removeMap[K, V](map: swaydb.Map[Key[K], Option[V]],
                      mapKey: Seq[K])(implicit keySerializer: Serializer[K],
                                      mapKeySerializer: Serializer[Key[K]],
                                      valueSerializer: Serializer[V],
                                      optionValueSerializer: Serializer[Option[V]],
                                      keyOrder: KeyOrder[Slice[Byte]]): Seq[Data.Remove[Key[K]]] =
    Seq[Data.Remove[Key[K]]](
      Batch.Remove(Key.SubMap[K](mapKey.dropRight(1), mapKey.last)), //remove the subMap entry from parent Map i.e this
      Batch.Remove(Key.MapStart[K](mapKey), Key.MapEnd[K](mapKey)) //remove the subMap itself
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
                                keyOrder: KeyOrder[Slice[Byte]],
                                valueSerializerOption: Serializer[Option[V]],
                                valueSerializer: Serializer[V]) extends MapIterator[K, V](mapKey, dbIterator = map.copy(map.db, Some(From(Key.MapStart(mapKey), orAfter = false, orBefore = false, before = false, after = true)))) {

  def maps: Maps[K, V] =
    new Maps[K, V](map, mapKey)

  def exists(): Try[Boolean] =
    map.contains(Key.MapStart(mapKey))

  /**
    * Returns None if the map does not exist or returns the value.
    */
  def getValue(): Try[Option[V]] =
    map.get(Key.MapStart(mapKey)).map(_.flatten)

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

  def put(key: K, value: V): Try[Level0Meter] =
    map.put(key = Key.MapEntry(mapKey, key), value = Some(value))

  def put(key: K, value: V, expireAfter: FiniteDuration): Try[Level0Meter] =
    map.put(Key.MapEntry(mapKey, key), Some(value), expireAfter.fromNow)

  def put(key: K, value: V, expireAt: Deadline): Try[Level0Meter] =
    map.put(Key.MapEntry(mapKey, key), Some(value), expireAt)

  def preparePut(key: K, value: V): Batch[Key.MapEntry[K], Option[V]] =
    preparePut(key, value, None)

  def preparePut(key: K, value: V, expireAfter: FiniteDuration): Batch[Key.MapEntry[K], Option[V]] =
    preparePut(key, value, Some(expireAfter.fromNow))

  def preparePut(key: K, value: V, deadline: Deadline): Batch[Key.MapEntry[K], Option[V]] =
    preparePut(key, value, Some(deadline))

  private def preparePut(key: K, value: V, deadline: Option[Deadline]): Batch[Key.MapEntry[K], Option[V]] =
    Data.Put(Key.MapEntry(mapKey, key), value = Some(value), deadline = deadline)

  def remove(key: K): Try[Level0Meter] =
    map.remove(Key.MapEntry(mapKey, key))

  def remove(from: K, to: K): Try[Level0Meter] =
    map.remove(Key.MapEntry(mapKey, from), Key.MapEntry(mapKey, to))

  def prepareRemove(key: K): Batch[Key.MapEntry[K], Option[V]] =
    makeRemoveBatch(key, None, None)

  def prepareRemove(from: K, to: K): Batch[Key.MapEntry[K], Option[V]] =
    makeRemoveBatch(from, Some(to), None)

  def commit(entries: Batch[Key.MapEntry[K], Option[V]]*) =
    baseMap().batch(entries)

  private def makeRemoveBatch(from: K, to: Option[K], deadline: Option[Deadline]): Batch[Key.MapEntry[K], Option[V]] =
    Data.Remove(from = Key.MapEntry(mapKey, from), to = to.map(Key.MapEntry(mapKey, _)), deadline = deadline)

  /**
    * Removes all key-values from the current Map. SubMaps and subMap's key-values or not altered.
    */
  def clear(): Try[Level0Meter] = {
    val (start, end) = Map.entriesRangeKeys(mapKey)
    map.batch(
      //remove key-value entries, but also re-insert the start and end entries for the Map.
      Batch.Remove(start, end),
      Batch.Put(start, None),
      Batch.Put(end, None)
    )
  }

  def expire(key: K, after: FiniteDuration): Try[Level0Meter] =
    map.expire(Key.MapEntry(mapKey, key), after.fromNow)

  def expire(key: K, at: Deadline): Try[Level0Meter] =
    map.expire(Key.MapEntry(mapKey, key), at)

  def expire(from: K, to: K, after: FiniteDuration): Try[Level0Meter] =
    map.expire(Key.MapEntry(mapKey, from), Key.MapEntry(mapKey, to), after.fromNow)

  def expire(from: K, to: K, at: Deadline): Try[Level0Meter] =
    map.expire(Key.MapEntry(mapKey, from), Key.MapEntry(mapKey, to), at)

  def update(key: K, value: V): Try[Level0Meter] =
    map.update(Key.MapEntry(mapKey, key), Some(value))

  def update(from: K, to: K, value: V): Try[Level0Meter] =
    map.update(Key.MapEntry(mapKey, from), Key.MapEntry(mapKey, to), Some(value))

  def batch(batch: Batch[K, V]*): Try[Level0Meter] =
    this.batch(batch)

  private def makeBatch(batch: Batch[K, V]): Batch[Key.MapEntry[K], Option[V]] =
    batch match {
      case Data.Put(key, value, deadline) =>
        preparePut(key, value, deadline)
      case Data.Remove(from, to, deadline) =>
        Data.Remove(from = Key.MapEntry(mapKey, from), to = to.map(Key.MapEntry(mapKey, _)), deadline = deadline)
      case Data.Update(from, to, value) =>
        Data.Update(from = Key.MapEntry(mapKey, from), to = to.map(Key.MapEntry(mapKey, _)), value = Some(value))
      case Data.Add(elem, deadline) =>
        Data.Add(elem = Key.MapEntry(mapKey, elem), deadline = deadline)
    }

  private def makeBatch(batch: Iterable[Batch[K, V]]): Iterable[Batch[Key.MapEntry[K], Option[V]]] =
    batch map makeBatch

  def batch(batch: Iterable[Batch[K, V]]): Try[Level0Meter] =
    map.batch(makeBatch(batch))

  def batchPut(keyValues: (K, V)*): Try[Level0Meter] =
    batchPut(keyValues)

  def batchPut(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    map.batchPut {
      keyValues map {
        case (key, value) =>
          (Key.MapEntry(mapKey, key), Some(value))
      }
    }

  def batchUpdate(keyValues: (K, V)*): Try[Level0Meter] =
    batchUpdate(keyValues)

  def batchUpdate(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    map.batchUpdate {
      keyValues map {
        case (key, value) =>
          (Key.MapEntry(mapKey, key), Some(value))
      }
    }

  def batchRemove(keys: K*): Try[Level0Meter] =
    batchRemove(keys)

  def batchRemove(keys: Iterable[K]): Try[Level0Meter] =
    map.batchRemove(keys.map(key => Key.MapEntry(mapKey, key)))

  def batchExpire(keys: (K, Deadline)*): Try[Level0Meter] =
    batchExpire(keys)

  def batchExpire(keys: Iterable[(K, Deadline)]): Try[Level0Meter] =
    map.batchExpire(keys.map(keyDeadline => (Key.MapEntry(mapKey, keyDeadline._1), keyDeadline._2)))

  /**
    * Returns target value for the input key.
    *
    * @return Returns None is the key does not exist.
    */
  def get(key: K): Try[Option[V]] =
    map.get(Key.MapEntry(mapKey, key)) flatMap {
      case Some(value) =>
        Success(value)
      case None =>
        TryUtil.successNone
    }

  /**
    * Returns target full key for the input partial key.
    *
    * This function is mostly used for Set databases where partial ordering on the Key is provided.
    */
  def getKey(key: K): Try[Option[K]] =
    map.getKey(Key.MapEntry(mapKey, key)) flatMap {
      case Some(key) =>
        key match {
          case Key.MapEntry(_, dataKey) =>
            Success(Some(dataKey))
          case got =>
            Failure(new Exception(s"Unable to fetch key. Got: $got expected MapKey.Entry"))
        }
      case None =>
        TryUtil.successNone
    }

  def getKeyValue(key: K): Try[Option[(K, V)]] =
    map.getKeyValue(Key.MapEntry(mapKey, key)) flatMap {
      case Some((key, value)) =>
        key match {
          case Key.MapEntry(_, dataKey) =>
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

  def keys: MapKeysIterator[K] =
    MapKeysIterator[K](
      mapKey = mapKey,
      keysIterator =
        new swaydb.Set[Key[K]](
          db = map.db,
          from = Some(From(Key.MapStart(mapKey), orAfter = false, orBefore = false, before = false, after = true))
        )
    )

  def contains(key: K): Try[Boolean] =
    map contains Key.MapEntry(mapKey, key)

  def mightContain(key: K): Try[Boolean] =
    map mightContain Key.MapEntry(mapKey, key)

  def level0Meter: Level0Meter =
    map.level0Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    map.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    map.sizeOfSegments

  def keySize(key: K): Int =
    map keySize Key.MapEntry(mapKey, key)

  def valueSize(value: V): Int =
    map valueSize Some(value)

  def expiration(key: K): Try[Option[Deadline]] =
    map expiration Key.MapEntry(mapKey, key)

  def timeLeft(key: K): Try[Option[FiniteDuration]] =
    expiration(key).map(_.map(_.timeLeft))

  private[swaydb] def baseMap(): swaydb.Map[Key[K], Option[V]] =
    map
}
