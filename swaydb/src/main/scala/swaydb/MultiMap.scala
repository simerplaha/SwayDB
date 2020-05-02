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

package swaydb

import java.nio.file.Path

import swaydb.MultiMapKey.{MapEntry, SubMap, _}
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.serializers.Serializer

import scala.collection.mutable
import scala.concurrent.duration.{Deadline, FiniteDuration}

object MultiMap {

  implicit def nothing[K, V]: Functions[K, V, Nothing] =
    new Functions[K, V, Nothing]()(null, null)

  implicit def void[K, V]: Functions[K, V, Void] =
    new Functions[K, V, Void]()(null, null)

  object Functions {
    def apply[K, V, F](functions: F*)(implicit keySerializer: Serializer[K],
                                      valueSerializer: Serializer[V],
                                      ev: F <:< swaydb.PureFunction[K, V, Apply.Map[V]]) = {
      val f = new Functions[K, V, F]()
      functions.foreach(f.register(_))
      f
    }

    def apply[K, V, F](functions: Iterable[F])(implicit keySerializer: Serializer[K],
                                               valueSerializer: Serializer[V],
                                               ev: F <:< swaydb.PureFunction[K, V, Apply.Map[V]]) = {
      val f = new Functions[K, V, F]()
      functions.foreach(f.register(_))
      f
    }
  }

  final case class Functions[K, V, F]()(implicit keySerializer: Serializer[K],
                                        valueSerializer: Serializer[V]) {

    private implicit val optionalSerialiser = Serializer.toOption(valueSerializer)

    private[swaydb] val innerFunctions = swaydb.Map.Functions[MultiMapKey[K], Option[V], swaydb.PureFunction[MultiMapKey[K], Option[V], Apply.Map[Option[V]]]]()

    def register[PF <: F](functions: PF*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Unit =
      functions.foreach(register(_))

    def register[PF <: F](function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Unit = {
      val innerFunction =
        (function: swaydb.PureFunction[K, V, Apply.Map[V]]) match {
          case function: swaydb.PureFunction.OnValue[V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnValue[Option[V], Apply.Map[Option[V]]] {
              override def apply(value: Option[V]): Apply.Map[Option[V]] =
                value match {
                  case Some(userValue) =>
                    Apply.Map.toOption(function.apply(userValue))

                  case None =>
                    throw new Exception("Function applied to None user value")
                }
            }

          case function: swaydb.PureFunction.OnKey[K, V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnKey[MultiMapKey[K], Option[V], Apply.Map[Option[V]]] {
              override def apply(key: MultiMapKey[K], deadline: Option[Deadline]): Apply.Map[Option[V]] =
                Apply.Map.toOption(function.apply(key.parentKey.last, deadline))
            }

          case function: swaydb.PureFunction.OnKeyValue[K, V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnKeyValue[MultiMapKey[K], Option[V], Apply.Map[Option[V]]] {
              override def apply(key: MultiMapKey[K], value: Option[V], deadline: Option[Deadline]): Apply.Map[Option[V]] =
                value match {
                  case Some(userValue) =>
                    Apply.Map.toOption(function.apply(key.parentKey.last, userValue, deadline))

                  case None =>
                    throw new Exception("Function applied to None user value")
                }
            }
        }

      innerFunctions.register(innerFunction)
    }
  }
}

case class MultiMap[K, V, F, BAG[_]] private(private[swaydb] val map: Map[MultiMapKey[K], Option[V], PureFunction[MultiMapKey[K], Option[V], Apply.Map[Option[V]]], BAG],
                                             mapKey: Seq[K],
                                             private val reverseIteration: Boolean = false,
                                             private val defaultExpiration: Option[Deadline] = None)(implicit keySerializer: Serializer[K],
                                                                                                     valueSerializer: Serializer[V],
                                                                                                     bag: Bag[BAG]) extends SwayMap[K, V, F, BAG] { self =>

  private def failure(expected: Class[_], actual: Class[_]) = throw new IllegalStateException(s"Internal error: ${expected.getName} expected but found ${actual.getName}.")

  private def failure(expected: String, actual: String) = throw new IllegalStateException(s"Internal error: $expected expected but found $actual.")

  override def path: Path =
    map.path

  def putMap(key: K): BAG[MultiMap[K, V, F, BAG]] =
    putMap(key, None)

  def putMap(key: K, expireAfter: FiniteDuration): BAG[MultiMap[K, V, F, BAG]] =
    putMap(key, Some(expireAfter.fromNow))

  def putMap(key: K, expireAt: Deadline): BAG[MultiMap[K, V, F, BAG]] =
    putMap(key, Some(expireAt))

  def putMap(key: K, expireAt: Option[Deadline]): BAG[MultiMap[K, V, F, BAG]] = {
    val childMapKey = mapKey :+ key

    val expiry = expireAt.orElse(defaultExpiration)

    val prepare: Seq[Prepare[MultiMapKey[K], Option[V], Nothing]] =
      Seq(
        Prepare.Remove(MapStart(childMapKey), MapEnd(childMapKey)),
        Prepare.Put(SubMap(mapKey, key), None, expiry),
        Prepare.Put(MultiMapKey.MapStart(childMapKey), None, expiry),
        Prepare.Put(MultiMapKey.MapEntriesStart(childMapKey), None, expiry),
        Prepare.Put(MultiMapKey.MapEntriesEnd(childMapKey), None, expiry),
        Prepare.Put(MultiMapKey.SubMapsStart(childMapKey), None, expiry),
        Prepare.Put(MultiMapKey.SubMapsEnd(childMapKey), None, expiry),
        Prepare.Put(MultiMapKey.MapEnd(childMapKey), None, expiry)
      )

    bag.map(map.commit(prepare)) {
      _ =>
        this.copy(map, childMapKey)
    }
  }

  def getMap(key: K): BAG[Option[MultiMap[K, V, F, BAG]]] = {
    val mapPrefix = mapKey :+ key

    bag.map(map.contains(MapStart(mapPrefix))) {
      contains =>
        if (contains)
          Some(this.copy(map, mapPrefix))
        else
          None
    }
  }

  def streamMaps: Stream[MultiMap[K, V, F, BAG]] =
    map
      .after(MultiMapKey.SubMapsStart(mapKey))
      .keys
      .stream
      .takeWhile {
        case user: UserEntry[K] =>
          user match {
            case MapEntry(_, _) =>
              false

            case SubMap(parentKey, _) =>
              parentKey == mapKey
          }

        case _ =>
          false
      }
      .map {
        case SubMap(key, dataKey) =>
          MultiMap(map, key :+ dataKey)

        case entry =>
          failure(classOf[SubMap[_]], entry.getClass)
      }

  override def put(key: K, value: V): BAG[OK] =
    map.put(MapEntry(mapKey, key), Some(value))

  override def put(key: K, value: V, expireAfter: FiniteDuration): BAG[OK] =
    map.put(MapEntry(mapKey, key), Some(value), expireAfter)

  override def put(key: K, value: V, expireAt: Deadline): BAG[OK] =
    map.put(MapEntry(mapKey, key), Some(value), expireAt)

  override def put(keyValues: (K, V)*): BAG[OK] = {
    val innerKeyValues =
      keyValues map {
        case (key, value) =>
          Prepare.Put(MapEntry(mapKey, key), Some(value), defaultExpiration)
      }

    map.commit(innerKeyValues)
  }

  override def put(keyValues: Stream[(K, V)]): BAG[OK] = {
    val stream: Stream[Prepare[MultiMapKey[K], Option[V], PureFunction[MultiMapKey[K], Option[V], Apply.Map[Option[V]]]]] =
      keyValues.map {
        case (key, value) =>
          Prepare.Put(MapEntry(mapKey, key), Some(value), defaultExpiration)
      }

    map.commit(stream)
  }

  override def put(keyValues: Iterable[(K, V)]): BAG[OK] = {
    val stream =
      keyValues.map {
        case (key, value) =>
          Prepare.Put(MapEntry(mapKey, key), Some(value), defaultExpiration)
      }

    map.commit(stream)
  }

  override def put(keyValues: Iterator[(K, V)]): BAG[OK] =
    put(keyValues.to(Iterable))

  override def remove(key: K): BAG[OK] =
    map.remove(MapEntry(mapKey, key))

  override def remove(keys: K*): BAG[OK] =
    map.remove {
      keys.map(key => MapEntry(mapKey, key))
    }

  override def remove(keys: Stream[K]): BAG[OK] =
    map.remove {
      keys.map(key => MapEntry(mapKey, key): MultiMapKey[K])
    }

  override def remove(keys: Iterable[K]): BAG[OK] =
    map.remove {
      keys.map(key => MapEntry(mapKey, key))
    }

  override def remove(keys: Iterator[K]): BAG[OK] =
    map.remove {
      keys.map(key => MapEntry(mapKey, key): MultiMapKey[K])
    }

  override def expire(key: K, after: FiniteDuration): BAG[OK] =
    map.expire(MapEntry(mapKey, key), after)

  override def expire(key: K, at: Deadline): BAG[OK] =
    map.expire(MapEntry(mapKey, key), at)

  //updates expiration
  def expire(after: FiniteDuration): BAG[OK] =
    map.expire(MapStart(mapKey), MapEnd(mapKey), after)

  def expire(at: Deadline): BAG[OK] =
    map.expire(MapStart(mapKey), MapEnd(mapKey), at)

  def expire(from: K, to: K, after: FiniteDuration): BAG[OK] =
    map.expire(MapEntry(mapKey, from), MapEntry(mapKey, to), after)

  def expire(from: K, to: K, at: Deadline): BAG[OK] =
    map.expire(MapEntry(mapKey, from), MapEntry(mapKey, to), at)

  override def clear(): BAG[OK] = {
    val entriesStart = MapEntriesStart(mapKey)
    val entriesEnd = MapEntriesEnd(mapKey)

    val entries =
      Seq(
        Prepare.Remove(entriesStart, entriesEnd),
        Prepare.Put(entriesStart, None),
        Prepare.Put(entriesEnd, None)
      )

    map.commit(entries)
  }

  override def get(key: K): BAG[Option[V]] =
    bag.flatMap(map.get(MultiMapKey.MapEntry(mapKey, key))) {
      case Some(value) =>
        value match {
          case some @ Some(_) =>
            bag.success(some)

          case None =>
            bag.failure(failure(classOf[MapEntry[_]], None.getClass))
        }

      case None =>
        bag.none
    }

  override def getKey(key: K): BAG[Option[K]] =
    bag.map(map.getKey(MapEntry(mapKey, key))) {
      case Some(MapEntry(_, key)) =>
        Some(key)

      case Some(entry) =>
        failure(MapEntry.getClass, entry.getClass)

      case None =>
        None
    }

  override def getKeyValue(key: K): BAG[Option[(K, V)]] =
    bag.map(map.getKeyValue(MapEntry(mapKey, key))) {
      case Some((MapEntry(_, key), Some(value))) =>
        Some((key, value))

      case Some((MapEntry(_, _), None)) =>
        failure("Value", "None")

      case Some(entry) =>
        failure(MapEntry.getClass, entry.getClass)

      case None =>
        None
    }

  override def contains(key: K): BAG[Boolean] =
    map.contains(MapEntry(mapKey, key))

  override def mightContain(key: K): BAG[Boolean] =
    map.mightContain(MapEntry(mapKey, key))

  override def mightContainFunction(functionId: K): BAG[Boolean] =
    map.mightContainFunction(MapEntry(mapKey, functionId))

  override private[swaydb] def keySet: mutable.Set[K] =
    ???

  override def levelZeroMeter: LevelZeroMeter =
    map.levelZeroMeter

  override def levelMeter(levelNumber: Int): Option[LevelMeter] =
    map.levelMeter(levelNumber)

  override def sizeOfSegments: Long =
    map.sizeOfSegments

  override def expiration(key: K): BAG[Option[Deadline]] =
    map.expiration(MapEntry(mapKey, key))

  override def timeLeft(key: K): BAG[Option[FiniteDuration]] =
    map.timeLeft(MapEntry(mapKey, key))

  override def from(key: K): SwayMap[K, V, F, BAG] =
    copy(map.from(MapEntry(mapKey, key)), mapKey)

  override def before(key: K): SwayMap[K, V, F, BAG] =
    copy(map.before(MapEntry(mapKey, key)), mapKey)

  override def fromOrBefore(key: K): SwayMap[K, V, F, BAG] =
    copy(map.fromOrBefore(MapEntry(mapKey, key)), mapKey)

  override def after(key: K): SwayMap[K, V, F, BAG] =
    copy(map.after(MapEntry(mapKey, key)), mapKey)

  override def fromOrAfter(key: K): SwayMap[K, V, F, BAG] =
    copy(map.fromOrAfter(MapEntry(mapKey, key)), mapKey)

  override def headOption: BAG[Option[(K, V)]] =
    stream.headOption

  override def headOrNull: BAG[(K, V)] =
    stream.headOrNull

  override def stream: Stream[(K, V)] =
    if (reverseIteration)
      map
        .before(MapEntriesEnd(mapKey))
        .reverse
        .stream
        .takeWhile {
          case (MapEntry(parent, _), _) =>
            parent == mapKey

          case _ =>
            false
        }
        .collect {
          case (MapEntry(_, key), Some(value)) =>
            (key, value)
        }
    else
      map
        .after(MapEntriesStart(mapKey))
        .stream
        .takeWhile {
          case (MapEntry(parent, _), _) =>
            parent == mapKey

          case _ =>
            false
        }
        .collect {
          case (MapEntry(_, key), Some(value)) =>
            (key, value)
        }

  override def iterator[BAG[_]](implicit bag: Bag.Sync[BAG]): Iterator[BAG[(K, V)]] =
    stream.iterator

  override def sizeOfBloomFilterEntries: BAG[Int] =
    map.sizeOfBloomFilterEntries

  override def isEmpty: BAG[Boolean] =
    bag.map(stream.headOption)(_.isEmpty)

  override def nonEmpty: BAG[Boolean] =
    bag.map(stream.headOption)(_.nonEmpty)

  override def lastOption: BAG[Option[(K, V)]] =
    stream.lastOption

  override def reverse: MultiMap[K, V, F, BAG] =
    copy(map = map, mapKey = mapKey, reverseIteration = true)

  override def toBag[X[_]](implicit bag: Bag[X]): SwayMap[K, V, F, X] =
    MultiMap[K, V, F, X](map.toBag, mapKey)

  override def asScala: mutable.Map[K, V] =
    ScalaMap(this.toBag[Bag.Less])

  override def close(): BAG[Unit] =
    map.close()

  override def delete(): BAG[Unit] =
    map.delete()
}