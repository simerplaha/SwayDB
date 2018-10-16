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
import swaydb.data.map.MapKey
import swaydb.data.slice.Slice
import swaydb.iterator._
import swaydb.serializers.Serializer
import swaydb.{Batch, Data, Map, SwayDB}

import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.{Failure, Success, Try}

private[swaydb] object SubMap {
  def apply[K, V](db: SwayDB,
                  mapKey: Seq[K])(implicit keySerializer: Serializer[K],
                                  valueSerializer: Serializer[V],
                                  ordering: Ordering[Slice[Byte]]): SubMap[K, V] = {
    implicit val mapKeySerializer = MapKey.mapKeySerializer(keySerializer)
    val map = Map[MapKey[K], V](db)
    new SubMap[K, V](map, mapKey)
  }

  /**
    * Creates the entries range for the [[SubMap]]'s mapKey/mapId.
    */
  def entriesRangeKeys[K](mapKey: Seq[K]): (MapKey.EntriesStart[K], MapKey.EntriesEnd[K]) =
    (MapKey.EntriesStart(mapKey), MapKey.EntriesEnd(mapKey))

  /**
    * Fetches all range key-values for all [[SubMap]]s within this [[SubMap]].
    *
    * All key-values are stored in this format. This function creates all [[MapKey.Start]] to [[MapKey.End]]
    * ranges for the current [[SubMap]] and all child [[SubMap]].
    *
    * MapKey.Start(Seq(1)),
    *   MapKey.EntriesStart(Seq(1))
    *     MapKey.Entry(Seq(1), 1)
    *   MapKey.EntriesEnd(Seq(1))
    *   MapKey.SubMapsStart(Seq(1))
    *     MapKey.SubMap(Seq(1), 1000)
    *   MapKey.SubMapsEnd(Seq(1))
    * MapKey.End(Seq(1))
    */
  def childSubMapRanges[K, V](subMap: SubMap[K, V])(implicit keySerializer: Serializer[K],
                                                    mapKeySerializer: Serializer[MapKey[K]],
                                                    ordering: Ordering[Slice[Byte]],
                                                    valueSerializer: Serializer[V]): List[(MapKey.Start[K], MapKey.End[K])] =
    subMap.subMapsOnly().foldLeft(List.empty[(MapKey.Start[K], MapKey.End[K])]) {
      case (previousList, (subMapKey, _)) => {
        val subMapKeys = subMap.mapKey :+ subMapKey
        previousList :+ (MapKey.Start(subMapKeys), MapKey.End(subMapKeys))
      } ++ {
        childSubMapRanges(SubMap[K, V](db = subMap.innerMap().db, mapKey = subMap.mapKey :+ subMapKey))
      }
    }

  /**
    * Build [[Batch.Remove]] for the input [[MapKey]] ranges.
    */
  def toBatchRemove[K](batches: Iterable[(MapKey[K], MapKey[K])]): Iterable[Data.Remove[MapKey[K]]] =
    batches map {
      case (start, end) =>
        Batch.Remove(start, end)
    }

  def putMap[K, V](map: Map[MapKey[K], V],
                   mapKey: Seq[K],
                   value: V)(implicit keySerializer: Serializer[K],
                             mapKeySerializer: Serializer[MapKey[K]],
                             valueSerializer: Serializer[V],
                             ordering: Ordering[Slice[Byte]]): Iterable[Batch[MapKey[K], V]] = {

    //batch to remove all SubMaps.
    val removeSubMapsBatches =
      toBatchRemove(childSubMapRanges(subMap = SubMap[K, V](map.db, mapKey)))

    val (thisMapEntriesStart, thisMapEntriesEnd) = SubMap.entriesRangeKeys(mapKey)

    val parentMapEntry =
      if (mapKey.size > 1)
        Some(Batch.Put(MapKey.SubMap(mapKey.dropRight(1), mapKey.last), value)) //add subMap entry to parent Map's key
      else //if this is the rootMap then there is no need to create a SubMap entry.
        None

    //batch to create a new map.
    val createMapBatch =
      Seq(
        Batch.Remove(thisMapEntriesStart, thisMapEntriesEnd), //remove all exiting entries
        Batch.Put(MapKey.Start(mapKey), value),
        Batch.Put(MapKey.EntriesStart(mapKey), value),
        Batch.Put(MapKey.EntriesEnd(mapKey), value),
        Batch.Put(MapKey.SubMapsStart(mapKey), value),
        Batch.Put(MapKey.SubMapsEnd(mapKey), value),
        Batch.Put(MapKey.End(mapKey), value)
      ) ++ parentMapEntry

    removeSubMapsBatches ++ createMapBatch
  }

  def updateMapValue[K, V](mapKey: Seq[K],
                           value: V)(implicit keySerializer: Serializer[K],
                                     mapKeySerializer: Serializer[MapKey[K]],
                                     valueSerializer: Serializer[V],
                                     ordering: Ordering[Slice[Byte]]): Seq[Data.Put[MapKey[K], V]] =
    Seq(
      Batch.Put(MapKey.SubMap(mapKey.dropRight(1), mapKey.last), value), //add subMap entry to parent Map's key
      Batch.Put(MapKey.Start(mapKey), value),
      Batch.Put(MapKey.EntriesStart(mapKey), value),
      Batch.Put(MapKey.EntriesEnd(mapKey), value),
      Batch.Put(MapKey.SubMapsStart(mapKey), value),
      Batch.Put(MapKey.SubMapsEnd(mapKey), value),
      Batch.Put(MapKey.End(mapKey), value)
    )

  def removeMap[K, V](map: Map[MapKey[K], V],
                      mapKey: Seq[K])(implicit keySerializer: Serializer[K],
                                      mapKeySerializer: Serializer[MapKey[K]],
                                      valueSerializer: Serializer[V],
                                      ordering: Ordering[Slice[Byte]]): Try[Level0Meter] =
    map.batch {
      Seq(
        Batch.Remove(MapKey.SubMap[K](mapKey.dropRight(1), mapKey.last)), //remove the subMap entry from parent Map i.e this
        Batch.Remove(MapKey.Start[K](mapKey), MapKey.End[K](mapKey)) //remove the subMap itself
      ) ++ {
        //fetch all child subMaps from the subMap being removed and batch remove them.
        SubMap.toBatchRemove[K](SubMap.childSubMapRanges[K, V](SubMap[K, V](map.db, mapKey)))
      }
    }
}

/**
  * Key-value or Map database API.
  *
  * For documentation check - http://swaydb.io/api/
  */
class SubMap[K, V](map: Map[MapKey[K], V],
                   mapKey: Seq[K])(implicit keySerializer: Serializer[K],
                                   mapKeySerializer: Serializer[MapKey[K]],
                                   ordering: Ordering[Slice[Byte]],
                                   valueSerializer: Serializer[V]) extends SubMapIterator[K, V](mapKey, dbIterator = DBIterator[MapKey[K], V](map.db, Some(From(MapKey.Start(mapKey), orAfter = false, orBefore = false, before = false, after = true)))) {

  def putMap(key: K, value: V): Try[SubMap[K, V]] = {
    val subMapKey = mapKey :+ key
    map.batch {
      SubMap.putMap[K, V](
        map = map,
        mapKey = subMapKey,
        value = value
      )
    } map {
      _ =>
        SubMap[K, V](
          db = map.db,
          mapKey = subMapKey
        )
    }
  }

  def updateMapValue(key: K, value: V): Try[SubMap[K, V]] = {
    val subMapKey = mapKey :+ key
    map.batch {
      SubMap.updateMapValue[K, V](
        mapKey = subMapKey,
        value = value
      )
    } map {
      _ =>
        SubMap[K, V](
          db = map.db,
          mapKey = subMapKey
        )
    }
  }

  def getMap(key: K): Try[Option[SubMap[K, V]]] = {
    val subMapKey = mapKey :+ key
    map.contains(MapKey.Start(subMapKey)) map {
      exists =>
        if (exists)
          Some(
            SubMap[K, V](
              db = map.db,
              mapKey = subMapKey
            )
          )
        else
          None
    }
  }

  def updateValue(value: V): Try[SubMap[K, V]] =
    map.batch {
      SubMap.updateMapValue[K, V](
        mapKey = mapKey,
        value = value
      )
    } map {
      _ =>
        SubMap[K, V](
          db = map.db,
          mapKey = mapKey
        )
    }

  def getMapValue(key: K): Try[Option[V]] =
    map.get(MapKey.Start(mapKey :+ key))

  def removeMap(key: K): Try[Level0Meter] =
    SubMap.removeMap(map, mapKey :+ key)

  def remove(): Try[Level0Meter] =
    remove(List((MapKey.Start[K](mapKey), MapKey.End[K](mapKey))) ++ SubMap.childSubMapRanges[K, V](this))

  private def remove(batches: Iterable[(MapKey[K], MapKey[K])]): Try[Level0Meter] =
    map.batch {
      batches map {
        case (start, end) =>
          Batch.Remove(start, end)
      }
    }

  def put(key: K, value: V): Try[Level0Meter] =
    map.put(key = MapKey.Entry(mapKey, key), value = value)

  def put(key: K, value: V, expireAfter: FiniteDuration): Try[Level0Meter] =
    map.put(MapKey.Entry(mapKey, key), value, expireAfter.fromNow)

  def put(key: K, value: V, expireAt: Deadline): Try[Level0Meter] =
    map.put(MapKey.Entry(mapKey, key), value, expireAt)

  def remove(key: K): Try[Level0Meter] =
    map.remove(MapKey.Entry(mapKey, key))

  def remove(from: K, to: K): Try[Level0Meter] =
    map.remove(MapKey.Entry(mapKey, from), MapKey.Entry(mapKey, to))

  def removeKeyValues(): Try[Level0Meter] = {
    val (start, end) = SubMap.entriesRangeKeys(mapKey)
    map.remove(start, end)
  }

  def expire(key: K, after: FiniteDuration): Try[Level0Meter] =
    map.expire(MapKey.Entry(mapKey, key), after.fromNow)

  def expire(key: K, at: Deadline): Try[Level0Meter] =
    map.expire(MapKey.Entry(mapKey, key), at)

  def expire(from: K, to: K, after: FiniteDuration): Try[Level0Meter] =
    map.expire(MapKey.Entry(mapKey, from), MapKey.Entry(mapKey, to), after.fromNow)

  def expire(from: K, to: K, at: Deadline): Try[Level0Meter] =
    map.expire(MapKey.Entry(mapKey, from), MapKey.Entry(mapKey, to), at)

  def update(key: K, value: V): Try[Level0Meter] =
    map.update(MapKey.Entry(mapKey, key), value)

  def update(from: K, to: K, value: V): Try[Level0Meter] =
    map.update(MapKey.Entry(mapKey, from), MapKey.Entry(mapKey, to), value)

  def batch(batch: Batch[K, V]*): Try[Level0Meter] =
    this.batch(batch)

  def batch(batch: Iterable[Batch[K, V]]): Try[Level0Meter] =
    map.batch(
      batch map {
        case data @ Data.Put(_, _, _) =>
          data.copy(MapKey.Entry(mapKey, data.key))
        case data @ Data.Remove(_, to, _) =>
          data.copy(from = MapKey.Entry(mapKey, data.from), to = to.map(MapKey.Entry(mapKey, _)))
        case data @ Data.Update(_, to, _) =>
          data.copy(from = MapKey.Entry(mapKey, data.from), to = to.map(MapKey.Entry(mapKey, _)))
        case data @ Data.Add(_, _) =>
          data.copy(elem = MapKey.Entry(mapKey, data.elem))
      }
    )

  def batchPut(keyValues: (K, V)*): Try[Level0Meter] =
    batchPut(keyValues)

  def batchPut(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    map.batchPut {
      keyValues map {
        case (key, value) =>
          (MapKey.Entry(mapKey, key), value)
      }
    }

  def batchUpdate(keyValues: (K, V)*): Try[Level0Meter] =
    batchUpdate(keyValues)

  def batchUpdate(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    map.batchUpdate {
      keyValues map {
        case (key, value) =>
          (MapKey.Entry(mapKey, key), value)
      }
    }

  def batchRemove(keys: K*): Try[Level0Meter] =
    batchRemove(keys)

  def batchRemove(keys: Iterable[K]): Try[Level0Meter] =
    map.batchRemove(keys.map(key => MapKey.Entry(mapKey, key)))

  def batchExpire(keys: (K, Deadline)*): Try[Level0Meter] =
    batchExpire(keys)

  def batchExpire(keys: Iterable[(K, Deadline)]): Try[Level0Meter] =
    map.batchExpire(keys.map(keyDeadline => (MapKey.Entry(mapKey, keyDeadline._1), keyDeadline._2)))

  /**
    * Returns target value for the input key.
    */
  def get(key: K): Try[Option[V]] =
    map.get(MapKey.Entry(mapKey, key))

  /**
    * Returns target full key for the input partial key.
    *
    * This function is mostly used for Set databases where partial ordering on the Key is provided.
    */
  def getKey(key: K): Try[Option[K]] =
    map.getKey(MapKey.Entry(mapKey, key)) flatMap {
      case Some(key) =>
        key match {
          case MapKey.Entry(_, dataKey) =>
            Success(Some(dataKey))
          case got =>
            Failure(new Exception(s"Unable to fetch key. Got: $got expected MapKey.Entry"))
        }
      case None =>
        TryUtil.successNone
    }

  def getKeyValue(key: K): Try[Option[(K, V)]] =
    map.getKeyValue(MapKey.Entry(mapKey, key)) flatMap {
      case Some((key, value)) =>
        key match {
          case MapKey.Entry(_, dataKey) =>
            Success(Some(dataKey, value))
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
        DBKeysIterator[MapKey[K]](
          db = map.db,
          from = Some(From(MapKey.Start(mapKey), orAfter = false, orBefore = false, before = false, after = true))
        )
    )

  def contains(key: K): Try[Boolean] =
    map contains MapKey.Entry(mapKey, key)

  def mightContain(key: K): Try[Boolean] =
    map mightContain MapKey.Entry(mapKey, key)

  def level0Meter: Level0Meter =
    map.level0Meter

  def level1Meter: LevelMeter =
    map.level1Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    map.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    map.sizeOfSegments

  def keySize(key: K): Int =
    map keySize MapKey.Entry(mapKey, key)

  def valueSize(value: V): Int =
    map valueSize value

  def expiration(key: K): Try[Option[Deadline]] =
    map expiration MapKey.Entry(mapKey, key)

  def timeLeft(key: K): Try[Option[FiniteDuration]] =
    expiration(key).map(_.map(_.timeLeft))

  private[swaydb] def innerMap() =
    map

}