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
import swaydb.core.map.counter.Counter
import swaydb.core.util.Times._
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.data.stream.{From, SourceFree, StreamFree}
import swaydb.multimap.MultiValue.Their
import swaydb.multimap.{MultiValue, Schema, Transaction}
import swaydb.serializers.{Serializer, _}

import scala.collection.compat._
import scala.collection.mutable
import scala.concurrent.duration.{Deadline, FiniteDuration}

object MultiMap_Experimental {

  //this should start from 1 because 0 will be used for format changes.
  val rootMapId: Long = Counter.startId

  /**
   * Given the inner [[swaydb.Map]] instance this creates a parent [[MultiMap_Experimental]] instance.
   */
  private[swaydb] def apply[M, K, V, F, BAG[_]](rootMap: swaydb.Map[MultiMapKey[M, K], MultiValue[V], PureFunction[MultiMapKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]], BAG])(implicit bag: swaydb.Bag[BAG],
                                                                                                                                                                                      keySerializer: Serializer[K],
                                                                                                                                                                                      tableSerializer: Serializer[M],
                                                                                                                                                                                      valueSerializer: Serializer[V],
                                                                                                                                                                                      counter: Counter): BAG[MultiMap_Experimental[M, K, V, F, BAG]] =
    bag.flatMap(rootMap.isEmpty) {
      isEmpty =>

        def initialEntries: BAG[OK] =
          rootMap.commit(
            Seq(
              Prepare.Put(MultiMapKey.MapStart(MultiMap_Experimental.rootMapId), MultiValue.None),
              Prepare.Put(MultiMapKey.MapEntriesStart(MultiMap_Experimental.rootMapId), MultiValue.None),
              Prepare.Put(MultiMapKey.MapEntriesEnd(MultiMap_Experimental.rootMapId), MultiValue.None),
              Prepare.Put(MultiMapKey.SubMapsStart(MultiMap_Experimental.rootMapId), MultiValue.None),
              Prepare.Put(MultiMapKey.SubMapsEnd(MultiMap_Experimental.rootMapId), MultiValue.None),
              Prepare.Put(MultiMapKey.MapEnd(MultiMap_Experimental.rootMapId), MultiValue.None)
            )
          )

        //RootMap has empty keys so if this database is new commit initial entries.
        if (isEmpty)
          bag.transform(initialEntries) {
            _ =>
              swaydb.MultiMap_Experimental[M, K, V, F, BAG](
                innerMap = rootMap,
                mapKey = null.asInstanceOf[M],
                mapId = MultiMap_Experimental.rootMapId
              )
          }
        else
          bag.success(
            swaydb.MultiMap_Experimental[M, K, V, F, BAG](
              innerMap = rootMap,
              mapKey = null.asInstanceOf[M],
              mapId = MultiMap_Experimental.rootMapId
            )
          )
    }

  private[swaydb] def failure(expected: Class[_], actual: Class[_]) = throw new IllegalStateException(s"Internal error: ${expected.getName} expected but found ${actual.getName}.")

  private[swaydb] def failure(expected: String, actual: String) = throw exception(expected, actual)

  private[swaydb] def exception(expected: String, actual: String) = new IllegalStateException(s"Internal error: $expected expected but found $actual.")

  implicit def nothing[T, K, V]: Functions[T, K, V, Nothing] =
    new Functions[T, K, V, Nothing]()(null, null, null)

  implicit def void[T, K, V]: Functions[T, K, V, Void] =
    new Functions[T, K, V, Void]()(null, null, null)

  object Functions {
    def apply[M, K, V, F](functions: F*)(implicit keySerializer: Serializer[K],
                                         tableSerializer: Serializer[M],
                                         valueSerializer: Serializer[V],
                                         ev: F <:< swaydb.PureFunction[K, V, Apply.Map[V]]) = {
      val f = new Functions[M, K, V, F]()
      functions.foreach(f.register(_))
      f
    }

    def apply[M, K, V, F](functions: Iterable[F])(implicit keySerializer: Serializer[K],
                                                  tableSerializer: Serializer[M],
                                                  valueSerializer: Serializer[V],
                                                  ev: F <:< swaydb.PureFunction[K, V, Apply.Map[V]]) = {
      val f = new Functions[M, K, V, F]()
      functions.foreach(f.register(_))
      f
    }
  }

  /**
   * All registered function for a [[MultiMap_Experimental]].
   */
  final case class Functions[M, K, V, F]()(implicit keySerializer: Serializer[K],
                                           tableSerializer: Serializer[M],
                                           valueSerializer: Serializer[V]) {

    private implicit val optionalSerialiser = MultiValue.serialiser(valueSerializer)

    private[swaydb] val innerFunctions = swaydb.Map.Functions[MultiMapKey[M, K], MultiValue[V], swaydb.PureFunction[MultiMapKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]]()

    def register[PF <: F](functions: PF*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Unit =
      functions.foreach(register(_))

    /**
     * Validates [[MultiMapKey]] supplied to the function. Only [[MultiMapKey.MapEntry]] is accepted
     * since functions are applied to Map's key-values only.
     */
    @inline def validate[O](key: MultiMapKey[M, K])(f: K => O): O =
      key match {
        case MultiMapKey.MapEntry(_, dataKey) =>
          f(dataKey)

        case entry: MultiMapKey[_, _] =>
          throw new Exception(s"MapEntry expected but got ${entry.getClass.getName}")
      }

    /**
     * User values for inner map cannot be None.
     */
    @inline def validate[O](value: MultiValue[V])(f: V => O): O =
      value match {
        case their: MultiValue.Their[V] =>
          f(their.value)

        case _: MultiValue.Our =>
          //UserEntries are never Our values for innertMap.
          throw new Exception("Function applied to Our user value")
      }

    /**
     * Vaidates both key and value before applying the function.
     */
    @inline def validate[O](key: MultiMapKey[M, K], value: MultiValue[V])(f: (K, V) => O): O =
      validate(key) {
        dataKey =>
          validate(value) {
            userValue =>
              f(dataKey, userValue)
          }
      }

    /**
     * Register the function converting it to [[MultiMap_Experimental.innerMap]]'s function type.
     */
    def register[PF <: F](function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Unit = {
      val innerFunction =
        (function: swaydb.PureFunction[K, V, Apply.Map[V]]) match {
          //convert all MultiMap Functions to Map functions that register them since MultiMap is
          //just a parent implementation over Map.
          case function: swaydb.PureFunction.OnValue[V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnValue[MultiValue[V], Apply.Map[MultiValue[V]]] {
              override def apply(value: MultiValue[V]): Apply.Map[MultiValue[V]] =
                validate(value) {
                  value =>
                    function
                      .apply(value)
                      .map(value => MultiValue.Their(value))
                }

              //use user function's functionId
              override def id: String =
                function.id
            }

          case function: swaydb.PureFunction.OnKey[K, V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnKey[MultiMapKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]] {
              override def apply(key: MultiMapKey[M, K], deadline: Option[Deadline]): Apply.Map[MultiValue[V]] =
                validate(key) {
                  dataKey =>
                    function
                      .apply(dataKey, deadline)
                      .map(value => MultiValue.Their(value))
                }

              //use user function's functionId
              override def id: String =
                function.id
            }

          case function: swaydb.PureFunction.OnKeyValue[K, V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnKeyValue[MultiMapKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]] {
              override def apply(key: MultiMapKey[M, K], value: MultiValue[V], deadline: Option[Deadline]): Apply.Map[MultiValue[V]] =
                validate(key, value) {
                  (dataKey, userValue) =>
                    function
                      .apply(dataKey, userValue, deadline)
                      .map(value => MultiValue.Their(value))
                }

              //use user function's functionId
              override def id: String =
                function.id
            }
        }

      innerFunctions.register(innerFunction)
    }
  }


  /**
   * Converts [[Prepare]] statements of this [[MultiMap_Experimental]] to inner [[Map]]'s statements.
   */
  def toInnerPrepare[M, K, V, F](prepare: Transaction[M, K, V, F]): Prepare[MultiMapKey[M, K], MultiValue[V], PureFunction[MultiMapKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]] =
    toInnerPrepare(prepare.mapId, prepare.defaultExpiration, prepare.prepare)

  /**
   * Converts [[Prepare]] statements of this [[MultiMap_Experimental]] to inner [[Map]]'s statements.
   */
  def toInnerPrepare[M, K, V, F](mapId: Long, defaultExpiration: Option[Deadline], prepare: Prepare[K, V, F]): Prepare[MultiMapKey[M, K], MultiValue[V], PureFunction[MultiMapKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]] =
    prepare match {
      case Prepare.Put(key, value, deadline) =>
        Prepare.Put(MapEntry(mapId, key), MultiValue.Their(value), deadline earlier defaultExpiration)

      case Prepare.Remove(from, to, deadline) =>
        to match {
          case Some(to) =>
            Prepare.Remove(MapEntry(mapId, from), Some(MapEntry(mapId, to)), deadline earlier defaultExpiration)

          case None =>
            Prepare.Remove[MultiMapKey[M, K]](from = MapEntry(mapId, from), to = None, deadline = deadline earlier defaultExpiration)
        }

      case Prepare.Update(from, to, value) =>
        to match {
          case Some(to) =>
            Prepare.Update[MultiMapKey[M, K], MultiValue[V]](MapEntry(mapId, from), Some(MapEntry(mapId, to)), value = MultiValue.Their(value))

          case None =>
            Prepare.Update[MultiMapKey[M, K], MultiValue[V]](key = MapEntry(mapId, from), value = MultiValue.Their(value))
        }

      case Prepare.ApplyFunction(from, to, function) =>
        // Temporary solution: casted because the actual instance itself not used internally.
        // Core only uses the String value of function.id which is searched in functionStore to validate function.
        val castedFunction = function.asInstanceOf[PureFunction[MultiMapKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]]

        to match {
          case Some(to) =>
            Prepare.ApplyFunction(from = MapEntry(mapId, from), to = Some(MapEntry(mapId, to)), function = castedFunction)

          case None =>
            Prepare.ApplyFunction(from = MapEntry(mapId, from), to = None, function = castedFunction)
        }

      case Prepare.Add(elem, deadline) =>
        Prepare.Put(MapEntry(mapId, elem), MultiValue.None, deadline earlier defaultExpiration)
    }

}

/**
 * [[MultiMap_Experimental]] extends [[swaydb.Map]]'s API to allow storing multiple Maps withing a single Map.
 *
 * [[MultiMap_Experimental]] is just a simple extension that uses custom data types ([[MultiMapKey]]) and
 * KeyOrder ([[MultiMapKey.ordering]]) for it's API.
 */
case class MultiMap_Experimental[M, K, V, F, BAG[_]] private(private[swaydb] val innerMap: Map[MultiMapKey[M, K], MultiValue[V], PureFunction[MultiMapKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]], BAG],
                                                             mapKey: M,
                                                             mapId: Long,
                                                             defaultExpiration: Option[Deadline] = None)(implicit keySerializer: Serializer[K],
                                                                                                         tableSerializer: Serializer[M],
                                                                                                         valueSerializer: Serializer[V],
                                                                                                         counter: Counter,
                                                                                                         val bag: Bag[BAG]) extends MapT[K, V, F, BAG] { self =>

  override def path: Path =
    innerMap.path

  /**
   * APIs for managing child map of this [[MultiMap_Experimental]].
   */
  val schema: Schema[M, K, V, F, BAG] =
    new Schema(
      innerMap = innerMap,
      mapId = mapId,
      defaultExpiration = defaultExpiration
    )

  /**
   * Narrows this [[MultiMap_Experimental]]'s map key type [[M]]
   */
  def narrow[M2](mapKey: Class[M2])(implicit evT: M2 <:< M): MultiMap_Experimental[M2, K, V, F, BAG] =
    this.asInstanceOf[MultiMap_Experimental[M2, K, V, F, BAG]]

  /**
   * Narrows this [[MultiMap_Experimental]]'s map key type [[M]] and key-value key type [[K]]
   */
  def narrow[M2, K2](mapKey: Class[M2],
                     keyType: Class[K2])(implicit evT: M2 <:< M,
                                         evK: K2 <:< K): MultiMap_Experimental[M2, K2, V, F, BAG] =
    this.asInstanceOf[MultiMap_Experimental[M2, K2, V, F, BAG]]

  /**
   * Narrows this [[MultiMap_Experimental]]'s map key type [[M]], key-value key type [[K]] and value type [[V]]
   */
  def narrow[M2, K2, V2](mapKey: Class[M2],
                         keyType: Class[K2],
                         valueType: Class[V2])(implicit evT: M2 <:< M,
                                               evK: K2 <:< K,
                                               evV: V2 <:< V): MultiMap_Experimental[M2, K2, V2, F, BAG] =
    this.asInstanceOf[MultiMap_Experimental[M2, K2, V2, F, BAG]]

  /**
   * Narrows this [[MultiMap_Experimental]]'s map key type [[M]], key-value key type [[K]], value type [[V]] and function type [[F]].
   */
  def narrow[M2, K2, V2, F2](mapKey: Class[M2],
                             keyType: Class[K2],
                             valueType: Class[V2],
                             functionType: Class[F2])(implicit evT: M2 <:< M,
                                                      evK: K2 <:< K,
                                                      evV: V2 <:< V,
                                                      evF: F2 <:< F): MultiMap_Experimental[M2, K2, V2, F2, BAG] =
    this.asInstanceOf[MultiMap_Experimental[M2, K2, V2, F2, BAG]]

  def put(key: K, value: V): BAG[OK] =
    innerMap.put(MapEntry(mapId, key), MultiValue.Their(value), defaultExpiration)

  def put(key: K, value: V, expireAfter: FiniteDuration): BAG[OK] =
    put(key, value, expireAfter.fromNow)

  def put(key: K, value: V, expireAt: Deadline): BAG[OK] =
    innerMap.put(MapEntry(mapId, key), MultiValue.Their(value), defaultExpiration earlier expireAt)

  override def put(keyValues: (K, V)*): BAG[OK] = {
    val innerKeyValues =
      keyValues map {
        case (key, value) =>
          Prepare.Put(MapEntry(mapId, key), MultiValue.Their(value), defaultExpiration)
      }

    innerMap.commit(innerKeyValues)
  }

  override def put(keyValues: Stream[(K, V), BAG]): BAG[OK] = {
    val stream: Stream[Prepare[MultiMapKey[M, K], MultiValue[V], PureFunction[MultiMapKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]], BAG] =
      keyValues.map {
        case (key, value) =>
          Prepare.Put(MapEntry(mapId, key), MultiValue.Their(value), defaultExpiration)
      }

    innerMap.commit(stream)
  }

  override def put(keyValues: Iterable[(K, V)]): BAG[OK] = {
    val stream =
      keyValues.map {
        case (key, value) =>
          Prepare.Put(MapEntry(mapId, key), MultiValue.Their(value), defaultExpiration)
      }

    innerMap.commit(stream)
  }

  override def put(keyValues: Iterator[(K, V)]): BAG[OK] =
    put(keyValues.to(Iterable))

  def remove(key: K): BAG[OK] =
    innerMap.remove(MapEntry(mapId, key))

  def remove(from: K, to: K): BAG[OK] =
    innerMap.remove(MapEntry(mapId, from), MapEntry(mapId, to))

  def remove(keys: K*): BAG[OK] =
    innerMap.remove {
      keys.map(key => MapEntry(mapId, key))
    }

  def remove(keys: Stream[K, BAG]): BAG[OK] =
    bag.flatMap(keys.materialize)(remove)

  def remove(keys: Iterable[K]): BAG[OK] =
    remove(keys.iterator)

  def remove(keys: Iterator[K]): BAG[OK] =
    innerMap.remove(keys.map(key => MapEntry(mapId, key)))

  def expire(key: K, after: FiniteDuration): BAG[OK] =
    innerMap.expire(MapEntry(mapId, key), defaultExpiration.earlier(after.fromNow))

  def expire(key: K, at: Deadline): BAG[OK] =
    innerMap.expire(MapEntry(mapId, key), defaultExpiration.earlier(at))

  def expire(from: K, to: K, after: FiniteDuration): BAG[OK] =
    innerMap.expire(MapEntry(mapId, from), MapEntry(mapId, to), defaultExpiration.earlier(after.fromNow))

  def expire(from: K, to: K, at: Deadline): BAG[OK] =
    innerMap.expire(MapEntry(mapId, from), MapEntry(mapId, to), defaultExpiration.earlier(at))

  def expire(keys: (K, Deadline)*): BAG[OK] =
    bag.suspend(expire(keys))

  def expire(keys: Stream[(K, Deadline), BAG]): BAG[OK] =
    bag.flatMap(keys.materialize)(expire)

  def expire(keys: Iterable[(K, Deadline)]): BAG[OK] =
    expire(keys.iterator)

  def expire(keys: Iterator[(K, Deadline)]): BAG[OK] = {
    val iterable: Iterable[Prepare[MultiMapKey[M, K], MultiValue[V], PureFunction[MultiMapKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]]] =
      keys.map {
        case (key, deadline) =>
          Prepare.Expire(MapEntry(mapId, key), deadline.earlier(defaultExpiration))
      }.to(Iterable)

    innerMap.commit(iterable)
  }

  def update(key: K, value: V): BAG[OK] =
    innerMap.update(MapEntry(mapId, key), MultiValue.Their(value))

  def update(from: K, to: K, value: V): BAG[OK] =
    innerMap.update(MapEntry(mapId, from), MapEntry(mapId, to), MultiValue.Their(value))

  def update(keyValues: (K, V)*): BAG[OK] = {
    val updates =
      keyValues.map {
        case (key, value) =>
          Prepare.Update(MapEntry(mapId, key), MultiValue.Their(value))
      }

    innerMap.commit(updates)
  }

  def update(keyValues: Stream[(K, V), BAG]): BAG[OK] =
    bag.flatMap(keyValues.materialize)(update)

  def update(keyValues: Iterable[(K, V)]): BAG[OK] = {
    val updates =
      keyValues.map {
        case (key, value) =>
          Prepare.Update(MapEntry(mapId, key), MultiValue.Their(value))
      }

    innerMap.commit(updates)
  }

  def update(keyValues: Iterator[(K, V)]): BAG[OK] =
    update(keyValues.to(Iterable))

  def clearKeyValues(): BAG[OK] = {
    val entriesStart = MapEntriesStart(mapId)
    val entriesEnd = MapEntriesEnd(mapId)

    val entries =
      Seq(
        Prepare.Remove(entriesStart, entriesEnd),
        Prepare.Put(entriesStart, MultiValue.None),
        Prepare.Put(entriesEnd, MultiValue.None)
      )

    innerMap.commit(entries)
  }

  /**
   * @note In other operations like [[expire]], [[remove]], [[put]] the input expiration value is compared with [[defaultExpiration]]
   *       to get the nearest expiration. But functions does not check if the custom logic within the function expires
   *       key-values earlier than [[defaultExpiration]].
   */
  def applyFunction[PF <: F](key: K, function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] = {
    val innerKey = innerMap.keySerializer.write(MapEntry(mapId, key))
    val functionId = Slice.writeString(function.id)
    innerMap.core.applyFunction(innerKey, functionId)
  }

  /**
   * @note In other operations like [[expire]], [[remove]], [[put]] the input expiration value is compared with [[defaultExpiration]]
   *       to get the nearest expiration. But functions does not check if the custom logic within the function expires
   *       key-values earlier than [[defaultExpiration]].
   */
  def applyFunction[PF <: F](from: K, to: K, function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] = {
    val fromKey = innerMap.keySerializer.write(MapEntry(mapId, from))
    val toKey = innerMap.keySerializer.write(MapEntry(mapId, to))
    val functionId = Slice.writeString(function.id)
    innerMap.core.applyFunction(fromKey, toKey, functionId)
  }

  /**
   * Converts [[Prepare]] statement for this map into [[Prepare]] statement for this Map's parent Map so that
   * multiple [[MultiMap_Experimental]] [[Prepare]] statements can be executed as a single transaction.
   *
   * @see [[MultiMap_Experimental.commit]] to commit [[Transaction]]s.
   */
  def toTransaction[PF <: F](prepare: Prepare[K, V, PF]*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Seq[Transaction[M, K, V, PF]] =
    prepare.map(prepare => new Transaction(mapId, defaultExpiration, prepare))

  /**
   * Converts [[Prepare]] statement for this map into [[Prepare]] statement for this Map's parent Map so that
   * multiple [[MultiMap_Experimental]] [[Prepare]] statements can be executed as a single transaction.
   *
   * @see [[MultiMap_Experimental.commit]] to commit [[Transaction]]s.
   */
  def toTransaction[PF <: F](prepare: Stream[Prepare[K, V, PF], BAG])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[Iterable[Transaction[M, K, V, PF]]] =
    bag.transform(prepare.materialize) {
      prepares =>
        toTransaction(prepares)
    }

  /**
   * Converts [[Prepare]] statement for this map into [[Prepare]] statement for this Map's parent Map so that
   * multiple [[MultiMap_Experimental]] [[Prepare]] statements can be executed as a single transaction.
   *
   * @see [[MultiMap_Experimental.commit]] to commit [[Transaction]]s.
   */
  def toTransaction[PF <: F](prepare: Iterable[Prepare[K, V, PF]])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Iterable[Transaction[M, K, V, PF]] =
    prepare.map(prepare => new Transaction(mapId, defaultExpiration, prepare))

  /**
   * Commits transaction to global map.
   */
  def commit[M2, K2, V2, PF <: F](transaction: Iterable[Transaction[M, K, V, PF]])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]],
                                                                                   evT: M2 <:< M,
                                                                                   evK: K2 <:< K,
                                                                                   evV: V2 <:< V): BAG[OK] =
    innerMap.commit {
      transaction map {
        transaction =>
          MultiMap_Experimental.toInnerPrepare(transaction)
      }
    }

  def commit[PF <: F](prepare: Prepare[K, V, PF]*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    innerMap.commit(prepare.map(prepare => MultiMap_Experimental.toInnerPrepare(mapId, defaultExpiration, prepare)))

  def commit[PF <: F](prepare: Stream[Prepare[K, V, PF], BAG])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    bag.flatMap(prepare.materialize) {
      prepares =>
        commit(prepares)
    }

  def commit[PF <: F](prepare: Iterable[Prepare[K, V, PF]])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    innerMap.commit(prepare.map(prepare => MultiMap_Experimental.toInnerPrepare(mapId, defaultExpiration, prepare)))

  def get(key: K): BAG[Option[V]] =
    bag.flatMap(innerMap.get(MultiMapKey.MapEntry(mapId, key))) {
      case Some(value) =>
        value match {
          case their: Their[V] =>
            bag.success(Some(their.value))

          case _: MultiValue.Our =>
            bag.failure(MultiMap_Experimental.failure(classOf[Their[_]], classOf[MultiValue.Our]))

        }

      case None =>
        bag.none
    }

  def getKey(key: K): BAG[Option[K]] =
    bag.map(innerMap.getKey(MapEntry(mapId, key))) {
      case Some(MapEntry(_, key)) =>
        Some(key)

      case Some(entry) =>
        MultiMap_Experimental.failure(MapEntry.getClass, entry.getClass)

      case None =>
        None
    }

  def getKeyValue(key: K): BAG[Option[(K, V)]] =
    bag.map(innerMap.getKeyValue(MapEntry(mapId, key))) {
      case Some((MapEntry(_, key), their: MultiValue.Their[V])) =>
        Some((key, their.value))

      case Some((MapEntry(_, _), _: MultiValue.Our)) =>
        MultiMap_Experimental.failure(classOf[MultiValue.Their[V]], classOf[MultiValue.Our])

      case Some(entry) =>
        MultiMap_Experimental.failure(MapEntry.getClass, entry.getClass)

      case None =>
        None
    }

  override def getKeyDeadline(key: K): BAG[Option[(K, Option[Deadline])]] =
    getKeyDeadline(key, bag)

  def getKeyDeadline[BAG[_]](key: K, bag: Bag[BAG]): BAG[Option[(K, Option[Deadline])]] =
    bag.map(innerMap.getKeyDeadline(MapEntry(mapId, key), bag)) {
      case Some((MapEntry(_, key), deadline)) =>
        Some((key, deadline))

      case Some(entry) =>
        MultiMap_Experimental.failure(MapEntry.getClass, entry.getClass)

      case None =>
        None
    }

  override def getKeyValueDeadline[BAG[_]](key: K, bag: Bag[BAG]): BAG[Option[((K, Option[Deadline]), V)]] =
    bag.map(innerMap.getKeyValueDeadline(MapEntry(mapId, key), bag)) {
      case Some(((MapEntry(_, key), deadline), value: MultiValue.Their[V])) =>
        Some(((key, deadline), value.value))

      case Some(((key, _), value)) =>
        throw new Exception(
          s"Expected key ${classOf[MapEntry[_]].getSimpleName}. Got ${key.getClass.getSimpleName}. " +
            s"Expected value ${classOf[MultiValue.Their[_]].getSimpleName}. Got ${value.getClass.getSimpleName}. "
        )

      case None =>
        None
    }

  def contains(key: K): BAG[Boolean] =
    innerMap.contains(MapEntry(mapId, key))

  def mightContain(key: K): BAG[Boolean] =
    innerMap.mightContain(MapEntry(mapId, key))

  def mightContainFunction[PF <: F](function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[Boolean] =
    innerMap.core.mightContainFunction(Slice.writeString(function.id))

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
    innerMap.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    innerMap.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    innerMap.sizeOfSegments

  def keySize(key: K): Int =
    (key: Slice[Byte]).size

  def valueSize(value: V): Int =
    (value: Slice[Byte]).size

  def expiration(key: K): BAG[Option[Deadline]] =
    innerMap.expiration(MapEntry(mapId, key))

  def timeLeft(key: K): BAG[Option[FiniteDuration]] =
    bag.map(expiration(key))(_.map(_.timeLeft))

  def headOption: BAG[Option[(K, V)]] =
    stream.headOption

  def headOrNull: BAG[(K, V)] =
    stream.headOrNull

  private def sourceFree(): SourceFree[K, (K, V)] =
    new SourceFree[K, (K, V)](from = None, reverse = false) {

      var freeStream: StreamFree[(K, V)] = _

      override private[swaydb] def headOrNull[BAG[_]](from: Option[From[K]], reverse: Boolean)(implicit bag: Bag[BAG]) = {
        val appliedStream =
          from match {
            case Some(from) =>
              val start =
                if (from.before)
                  innerMap.stream.before(MultiMapKey.MapEntry(mapId, from.key))
                else if (from.after)
                  innerMap.stream.after(MultiMapKey.MapEntry(mapId, from.key))
                else if (from.orBefore)
                  innerMap.stream.fromOrBefore(MultiMapKey.MapEntry(mapId, from.key))
                else if (from.orAfter)
                  innerMap.stream.fromOrAfter(MultiMapKey.MapEntry(mapId, from.key))
                else
                  innerMap.stream.from(MultiMapKey.MapEntry(mapId, from.key))

              if (reverse)
                start.reverse
              else
                start

            case None =>
              if (reverse)
                innerMap
                  .stream
                  .before(MapEntriesEnd(mapId))
                  .reverse
              else
                innerMap
                  .stream
                  .after(MapEntriesStart(mapId))
          }

        //restricts this Stream to fetch entries of this Map only.

        freeStream =
          appliedStream
            .free
            .takeWhile {
              case (MapEntry(mapId, _), _) =>
                mapId == mapId

              case _ =>
                false
            }
            .collect {
              case (MapEntry(_, key), their: MultiValue.Their[V]) =>
                (key, their.value)
            }

        freeStream.headOrNull
      }

      override private[swaydb] def nextOrNull[BAG[_]](previous: (K, V), reverse: Boolean)(implicit bag: Bag[BAG]) =
        freeStream.nextOrNull(previous)
    }

  def stream: Source[K, (K, V), BAG] =
    new Source(sourceFree())

  def iterator[BAG[_]](implicit bag: Bag.Sync[BAG]): Iterator[BAG[(K, V)]] =
    stream.iterator(bag)

  def sizeOfBloomFilterEntries: BAG[Int] =
    innerMap.sizeOfBloomFilterEntries

  def isEmpty: BAG[Boolean] =
    bag.map(stream.headOption)(_.isEmpty)

  def nonEmpty: BAG[Boolean] =
    bag.map(stream.headOption)(_.nonEmpty)

  def lastOption: BAG[Option[(K, V)]] =
    stream.lastOption

  /**
   * Returns an Async API of type O where the [[Bag]] is known.
   */
  def toBag[X[_]](implicit bag: Bag[X]): MultiMap_Experimental[M, K, V, F, X] =
    MultiMap_Experimental(
      innerMap = innerMap.toBag[X],
      mapKey = mapKey,
      mapId = mapId,
      defaultExpiration = defaultExpiration
    )

  def asScala: scala.collection.mutable.Map[K, V] =
    ScalaMap[K, V, F](toBag[Bag.Less](Bag.less))

  def close(): BAG[Unit] =
    innerMap.close()

  def delete(): BAG[Unit] =
    innerMap.delete()

  override def toString(): String =
    classOf[Map[_, _, _, BAG]].getSimpleName
}
