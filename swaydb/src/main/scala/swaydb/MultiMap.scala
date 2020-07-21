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

import java.nio.file.Path

import swaydb.MultiMapKey.{MapEntriesEnd, MapEntriesStart, MapEntry}
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.serializers.{Serializer, _}

import scala.collection.compat._
import scala.collection.mutable
import scala.concurrent.duration.{Deadline, FiniteDuration}

object MultiMap {

  /**
   * Given the inner [[swaydb.Map]] instance this creates a parent [[MultiMap]] instance.
   */
  private[swaydb] def apply[K, V, F, BAG[_]](rootMap: swaydb.Map[MultiMapKey[K], Option[V], PureFunction[MultiMapKey[K], Option[V], Apply.Map[Option[V]]], BAG])(implicit bag: swaydb.Bag[BAG],
                                                                                                                                                                 keySerializer: Serializer[K],
                                                                                                                                                                 valueSerializer: Serializer[V]): BAG[MultiMap[K, V, F, BAG]] =
    bag.flatMap(rootMap.isEmpty) {
      isEmpty =>
        val rootMapKey = Seq.empty[K]

        def initialEntries: BAG[OK] =
          rootMap.commit(
            Seq(
              Prepare.Put(MultiMapKey.MapStart(rootMapKey), None),
              Prepare.Put(MultiMapKey.MapEntriesStart(rootMapKey), None),
              Prepare.Put(MultiMapKey.MapEntriesEnd(rootMapKey), None),
              Prepare.Put(MultiMapKey.SubMapsStart(rootMapKey), None),
              Prepare.Put(MultiMapKey.SubMapsEnd(rootMapKey), None),
              Prepare.Put(MultiMapKey.MapEnd(rootMapKey), None)
            )
          )

        //Root Map has empty keys so if this database is new commit initial entries.
        if (isEmpty)
          bag.transform(initialEntries) {
            _ =>
              swaydb.MultiMap[K, V, F, BAG](
                map = rootMap,
                mapKey = rootMapKey
              )
          }
        else
          bag.success(
            swaydb.MultiMap[K, V, F, BAG](
              map = rootMap,
              mapKey = rootMapKey
            )
          )
    }

  private[swaydb] def failure(expected: Class[_], actual: Class[_]) = throw new IllegalStateException(s"Internal error: ${expected.getName} expected but found ${actual.getName}.")

  private[swaydb] def failure(expected: String, actual: String) = throw exception(expected, actual)

  private[swaydb] def exception(expected: String, actual: String) = new IllegalStateException(s"Internal error: $expected expected but found $actual.")

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

  /**
   * All registered function for a [[MultiMap]].
   */
  final case class Functions[K, V, F]()(implicit keySerializer: Serializer[K],
                                        valueSerializer: Serializer[V]) {

    private implicit val optionalSerialiser = Serializer.toOption(valueSerializer)

    private[swaydb] val innerFunctions = swaydb.Map.Functions[MultiMapKey[K], Option[V], swaydb.PureFunction[MultiMapKey[K], Option[V], Apply.Map[Option[V]]]]()

    def register[PF <: F](functions: PF*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Unit =
      functions.foreach(register(_))

    /**
     * Register the function converting it to [[MultiMap.map]]'s function type.
     */
    def register[PF <: F](function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Unit = {
      val innerFunction =
        (function: swaydb.PureFunction[K, V, Apply.Map[V]]) match {
          //convert all MultiMap Functions to Map functions that register them since MultiMap is
          //just a parent implementation over Map.
          case function: swaydb.PureFunction.OnValue[V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnValue[Option[V], Apply.Map[Option[V]]] {
              override def apply(value: Option[V]): Apply.Map[Option[V]] =
                value match {
                  case Some(userValue) =>
                    Apply.Map.toOption(function.apply(userValue))

                  case None =>
                    //UserEntries are never None for innerMap.
                    throw new Exception("Function applied to None user value")
                }

              //use user function's functionId
              override def id: String =
                function.id
            }

          case function: swaydb.PureFunction.OnKey[K, V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnKey[MultiMapKey[K], Option[V], Apply.Map[Option[V]]] {
              override def apply(key: MultiMapKey[K], deadline: Option[Deadline]): Apply.Map[Option[V]] =
                Apply.Map.toOption(function.apply(key.parentKey.last, deadline))

              //use user function's functionId
              override def id: String =
                function.id
            }

          case function: swaydb.PureFunction.OnKeyValue[K, V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnKeyValue[MultiMapKey[K], Option[V], Apply.Map[Option[V]]] {
              override def apply(key: MultiMapKey[K], value: Option[V], deadline: Option[Deadline]): Apply.Map[Option[V]] =
                value match {
                  case Some(userValue) =>
                    Apply.Map.toOption(function.apply(key.parentKey.last, userValue, deadline))

                  case None =>
                    //UserEntries are never None for innertMap.
                    throw new Exception("Function applied to None user value")
                }

              //use user function's functionId
              override def id: String =
                function.id
            }
        }

      innerFunctions.register(innerFunction)
    }
  }

}

/**
 * [[MultiMap]] extends [[swaydb.Map]]'s API to allow storing multiple Maps withing a single Map.
 *
 * [[MultiMap]] is just a simple extension that uses custom data types ([[MultiMapKey]]) and
 * KeyOrder ([[MultiMapKey.ordering]]) for it's API.
 */
case class MultiMap[K, V, F, BAG[_]] private(private val map: Map[MultiMapKey[K], Option[V], PureFunction[MultiMapKey[K], Option[V], Apply.Map[Option[V]]], BAG],
                                             mapKey: Iterable[K],
                                             private val from: Option[From[MultiMapKey.MapEntry[K]]] = None,
                                             private val reverseIteration: Boolean = false,
                                             defaultExpiration: Option[Deadline] = None)(implicit keySerializer: Serializer[K],
                                                                                         valueSerializer: Serializer[V],
                                                                                         val bag: Bag[BAG]) extends MapT[K, V, F, BAG] { self =>

  override def path: Path =
    map.path

  /**
   * APIs for managing child map of this [[MultiMap]].
   */
  def children: Children[K, V, F, BAG] =
    new swaydb.Children(
      map = map,
      mapKey = mapKey,
      defaultExpiration = defaultExpiration
    )

  def put(key: K, value: V): BAG[OK] =
    map.put(MapEntry(mapKey, key), Some(value))

  def put(key: K, value: V, expireAfter: FiniteDuration): BAG[OK] =
    map.put(MapEntry(mapKey, key), Some(value), expireAfter)

  def put(key: K, value: V, expireAt: Deadline): BAG[OK] =
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

  def remove(key: K): BAG[OK] =
    map.remove(MapEntry(mapKey, key))

  def remove(from: K, to: K): BAG[OK] =
    map.remove(MapEntry(mapKey, from), MapEntry(mapKey, to))

  def remove(keys: K*): BAG[OK] =
    map.remove {
      keys.map(key => MapEntry(mapKey, key))
    }

  def remove(keys: Stream[K]): BAG[OK] =
    bag.flatMap(keys.materialize)(remove)

  def remove(keys: Iterable[K]): BAG[OK] =
    remove(keys.iterator)

  def remove(keys: Iterator[K]): BAG[OK] =
    map.remove(keys.map(key => MapEntry(mapKey, key)))

  def expire(key: K, after: FiniteDuration): BAG[OK] =
    map.expire(MapEntry(mapKey, key), after)

  def expire(key: K, at: Deadline): BAG[OK] =
    map.expire(MapEntry(mapKey, key), at)

  def expire(from: K, to: K, after: FiniteDuration): BAG[OK] =
    map.expire(MapEntry(mapKey, from), MapEntry(mapKey, to), after)

  def expire(from: K, to: K, at: Deadline): BAG[OK] =
    map.expire(MapEntry(mapKey, from), MapEntry(mapKey, to), at)

  def expire(keys: (K, Deadline)*): BAG[OK] =
    bag.suspend(expire(keys))

  def expire(keys: Stream[(K, Deadline)]): BAG[OK] =
    bag.flatMap(keys.materialize)(expire)

  def expire(keys: Iterable[(K, Deadline)]): BAG[OK] =
    expire(keys.iterator)

  def expire(keys: Iterator[(K, Deadline)]): BAG[OK] = {
    val iterable: Iterable[Prepare[MultiMapKey[K], Option[V], PureFunction[MultiMapKey[K], Option[V], Apply.Map[Option[V]]]]] =
      keys.map {
        case (key, deadline) =>
          Prepare.Expire(MapEntry(mapKey, key), deadline)
      }.to(Iterable)

    map.commit(iterable)
  }

  def update(key: K, value: V): BAG[OK] =
    map.update(MapEntry(mapKey, key), Some(value))

  def update(from: K, to: K, value: V): BAG[OK] =
    map.update(MapEntry(mapKey, from), MapEntry(mapKey, to), Some(value))

  def update(keyValues: (K, V)*): BAG[OK] = {
    val updates =
      keyValues.map {
        case (key, value) =>
          Prepare.Update(MapEntry(mapKey, key), Some(value))
      }

    map.commit(updates)
  }

  def update(keyValues: Stream[(K, V)]): BAG[OK] =
    bag.flatMap(keyValues.materialize)(update)

  def update(keyValues: Iterable[(K, V)]): BAG[OK] = {
    val updates =
      keyValues.map {
        case (key, value) =>
          Prepare.Update(MapEntry(mapKey, key), Some(value))
      }

    map.commit(updates)
  }

  def update(keyValues: Iterator[(K, V)]): BAG[OK] =
    update(keyValues.to(Iterable))

  def clear(): BAG[OK] = {
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

  def applyFunction[PF <: F](key: K, function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] = {
    val innerKey = map.keySerializer.write(MapEntry(mapKey, key))
    val functionId = Slice.writeString(function.id)
    map.core.function(innerKey, functionId)
  }

  def applyFunction[PF <: F](from: K, to: K, function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] = {
    val fromKey = map.keySerializer.write(MapEntry(mapKey, from))
    val toKey = map.keySerializer.write(MapEntry(mapKey, to))
    val functionId = Slice.writeString(function.id)
    map.core.function(fromKey, toKey, functionId)
  }

  /**
   * Converts [[Prepare]] statements of this [[MultiMap]] to inner [[Map]]'s statements.
   */
  private def toInnerMapPrepare(prepare: Iterable[Prepare[K, V, _]]): Iterable[Prepare[MultiMapKey[K], Option[V], PureFunction[MultiMapKey[K], Option[V], Apply.Map[Option[V]]]]] =
    prepare.map {
      case Prepare.Put(key, value, deadline) =>
        Prepare.Put(MapEntry(mapKey, key), Some(value), deadline orElse defaultExpiration)

      case Prepare.Remove(from, to, deadline) =>
        to match {
          case Some(to) =>
            Prepare.Remove(MapEntry(mapKey, from), Some(MapEntry(mapKey, to)), deadline orElse defaultExpiration)

          case None =>
            Prepare.Remove[MultiMapKey[K]](from = MapEntry(mapKey, from), to = None, deadline = deadline orElse defaultExpiration)
        }

      case Prepare.Update(from, to, value) =>
        to match {
          case Some(to) =>
            Prepare.Update[MultiMapKey[K], Option[V]](MapEntry(mapKey, from), Some(MapEntry(mapKey, to)), value = Some(value))

          case None =>
            Prepare.Update[MultiMapKey[K], Option[V]](key = MapEntry(mapKey, from), value = Some(value))
        }

      case Prepare.ApplyFunction(from, to, function) =>
        // Temporary solution: casted because the actual instance itself not used internally.
        // Core only uses the String value of function.id which is searched in functionStore to validate function.
        val castedFunction = function.asInstanceOf[PureFunction[MultiMapKey[K], Option[V], Apply.Map[Option[V]]]]

        to match {
          case Some(to) =>
            Prepare.ApplyFunction(from = MapEntry(mapKey, from), to = Some(MapEntry(mapKey, to)), function = castedFunction)

          case None =>
            Prepare.ApplyFunction(from = MapEntry(mapKey, from), to = None, function = castedFunction)
        }

      case Prepare.Add(elem, deadline) =>
        Prepare.Put(MapEntry(mapKey, elem), None, deadline orElse defaultExpiration)
    }

  def commit[PF <: F](prepare: Prepare[K, V, PF]*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    map.commit(toInnerMapPrepare(prepare))

  def commit[PF <: F](prepare: Stream[Prepare[K, V, PF]])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    bag.flatMap(prepare.materialize) {
      prepares =>
        commit(prepares)
    }

  def commit[PF <: F](prepare: Iterable[Prepare[K, V, PF]])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    map.commit(toInnerMapPrepare(prepare))

  def get(key: K): BAG[Option[V]] =
    bag.flatMap(map.get(MultiMapKey.MapEntry(mapKey, key))) {
      case Some(value) =>
        value match {
          case some @ Some(_) =>
            bag.success(some)

          case None =>
            bag.failure(MultiMap.failure(classOf[MapEntry[_]], None.getClass))
        }

      case None =>
        bag.none
    }

  def getKey(key: K): BAG[Option[K]] =
    bag.map(map.getKey(MapEntry(mapKey, key))) {
      case Some(MapEntry(_, key)) =>
        Some(key)

      case Some(entry) =>
        MultiMap.failure(MapEntry.getClass, entry.getClass)

      case None =>
        None
    }

  def getKeyValue(key: K): BAG[Option[(K, V)]] =
    bag.map(map.getKeyValue(MapEntry(mapKey, key))) {
      case Some((MapEntry(_, key), Some(value))) =>
        Some((key, value))

      case Some((MapEntry(_, _), None)) =>
        MultiMap.failure("Value", "None")

      case Some(entry) =>
        MultiMap.failure(MapEntry.getClass, entry.getClass)

      case None =>
        None
    }

  def contains(key: K): BAG[Boolean] =
    map.contains(MapEntry(mapKey, key))

  def mightContain(key: K): BAG[Boolean] =
    map.mightContain(MapEntry(mapKey, key))

  def mightContainFunction[PF <: F](function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[Boolean] =
    map.core.mightContainFunction(Slice.writeString(function.id))

  /**
   * TODO keys function.
   */
  //  def keys: Set[K, F, BAG] =
  //    Set[K, F, BAG](
  //      core = map.core,
  //      from =
  //        from map {
  //          from =>
  //            from.copy(key = from.key.dataKey)
  //        },
  //      reverseIteration = reverseIteration
  //    )(keySerializer, bag)

  private[swaydb] def keySet: mutable.Set[K] =
    throw new NotImplementedError("KeySet function is not yet implemented. Please request for this on GitHub - https://github.com/simerplaha/SwayDB/issues.")

  def levelZeroMeter: LevelZeroMeter =
    map.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    map.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    map.sizeOfSegments

  def keySize(key: K): Int =
    (key: Slice[Byte]).size

  def valueSize(value: V): Int =
    (value: Slice[Byte]).size

  def expiration(key: K): BAG[Option[Deadline]] =
    map.expiration(MapEntry(mapKey, key))

  def timeLeft(key: K): BAG[Option[FiniteDuration]] =
    bag.map(expiration(key))(_.map(_.timeLeft))

  def from(key: K): MultiMap[K, V, F, BAG] =
    copy(from = Some(From(key = MapEntry(mapKey, key), orBefore = false, orAfter = false, before = false, after = false)))

  def before(key: K): MultiMap[K, V, F, BAG] =
    copy(from = Some(From(key = MapEntry(mapKey, key), orBefore = false, orAfter = false, before = true, after = false)))

  def fromOrBefore(key: K): MultiMap[K, V, F, BAG] =
    copy(from = Some(From(key = MapEntry(mapKey, key), orBefore = true, orAfter = false, before = false, after = false)))

  def after(key: K): MultiMap[K, V, F, BAG] =
    copy(from = Some(From(key = MapEntry(mapKey, key), orBefore = false, orAfter = false, before = false, after = true)))

  def fromOrAfter(key: K): MultiMap[K, V, F, BAG] =
    copy(from = Some(From(key = MapEntry(mapKey, key), orBefore = false, orAfter = true, before = false, after = false)))

  def headOption: BAG[Option[(K, V)]] =
    stream.headOption

  def headOrNull: BAG[(K, V)] =
    stream.headOrNull

  //restricts this Stream to fetch entries of this Map only.
  private def boundStreamToMap(stream: Stream[(MultiMapKey[K], Option[V])]): Stream[(K, V)] =
    stream
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

  def stream: Stream[(K, V)] =
    from match {
      case Some(from) =>
        val start =
          if (from.before)
            map.before(from.key)
          else if (from.after)
            map.after(from.key)
          else if (from.orBefore)
            map.fromOrBefore(from.key)
          else
            map.fromOrAfter(from.key)

        if (reverseIteration)
          boundStreamToMap(start.reverse.stream)
        else
          boundStreamToMap(start.stream)

      case None =>
        if (reverseIteration)
          boundStreamToMap {
            map
              .before(MapEntriesEnd(mapKey))
              .reverse
              .stream
          }
        else
          boundStreamToMap {
            map
              .after(MapEntriesStart(mapKey))
              .stream
          }
    }

  def iterator[BAG[_]](implicit bag: Bag.Sync[BAG]): Iterator[BAG[(K, V)]] =
    stream.iterator(bag)

  def sizeOfBloomFilterEntries: BAG[Int] =
    map.sizeOfBloomFilterEntries

  def isEmpty: BAG[Boolean] =
    bag.map(stream.headOption)(_.isEmpty)

  def nonEmpty: BAG[Boolean] =
    bag.map(stream.headOption)(_.nonEmpty)

  def lastOption: BAG[Option[(K, V)]] =
    stream.lastOption

  def reverse: MultiMap[K, V, F, BAG] =
    copy(reverseIteration = true)

  /**
   * Returns an Async API of type O where the [[Bag]] is known.
   */
  def toBag[X[_]](implicit bag: Bag[X]): MultiMap[K, V, F, X] =
    MultiMap(map.toBag[X], mapKey, from, reverseIteration, defaultExpiration)

  def asScala: scala.collection.mutable.Map[K, V] =
    ScalaMap[K, V, F](toBag[Bag.Less](Bag.less))

  def close(): BAG[Unit] =
    map.close()

  def delete(): BAG[Unit] =
    map.delete()

  override def toString(): String =
    classOf[Map[_, _, _, BAG]].getClass.getSimpleName
}