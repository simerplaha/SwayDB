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

import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.map.counter.Counter
import swaydb.core.util.Times._
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.data.stream.{From, SourceFree, StreamFree}
import swaydb.multimap.{MultiKey, MultiValue, Schema, Transaction}
import swaydb.serializers.{Serializer, _}

import scala.collection.compat._
import scala.collection.mutable
import scala.concurrent.duration.{Deadline, FiniteDuration}

object MultiMap {

  val folderName = "multimap-gen"

  //this should start from 1 because 0 will be used for format changes.
  val rootMapId: Long = Counter.startId

  private[swaydb] def withPersistentCounter[M, K, V, F, BAG[_]](path: Path,
                                                                mmap: swaydb.data.config.MMAP.Map,
                                                                map: swaydb.Map[MultiKey[M, K], MultiValue[V], PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]], BAG])(implicit bag: swaydb.Bag[BAG],
                                                                                                                                                                                            keySerializer: Serializer[K],
                                                                                                                                                                                            mapKeySerializer: Serializer[M],
                                                                                                                                                                                            valueSerializer: Serializer[V]): BAG[MultiMap[M, K, V, F, BAG]] = {
    implicit val writer = swaydb.core.map.serializer.CounterMapEntryWriter.CounterPutMapEntryWriter
    implicit val reader = swaydb.core.map.serializer.CounterMapEntryReader.CounterPutMapEntryReader
    implicit val core = map.core.bufferSweeper
    implicit val forceSaveApplier = ForceSaveApplier.Enabled

    Counter.persistent(
      path = path.resolve(MultiMap.folderName),
      mmap = mmap,
      mod = 1000,
      flushCheckpointSize = 1.mb
    ) match {
      case IO.Right(counter) =>
        implicit val implicitCounter: Counter = counter
        swaydb.MultiMap[M, K, V, F, BAG](map)

      case IO.Left(error) =>
        bag.failure(error.exception)
    }
  }

  /**
   * Given the inner [[swaydb.Map]] instance this creates a parent [[MultiMap]] instance.
   */
  private[swaydb] def apply[M, K, V, F, BAG[_]](rootMap: swaydb.Map[MultiKey[M, K], MultiValue[V], PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]], BAG])(implicit bag: swaydb.Bag[BAG],
                                                                                                                                                                                keySerializer: Serializer[K],
                                                                                                                                                                                mapKeySerializer: Serializer[M],
                                                                                                                                                                                valueSerializer: Serializer[V],
                                                                                                                                                                                counter: Counter): BAG[MultiMap[M, K, V, F, BAG]] =
    bag.flatMap(rootMap.isEmpty) {
      isEmpty =>

        def initialEntries: BAG[OK] =
          rootMap.commit(
            Seq(
              Prepare.Put(MultiKey.Start(MultiMap.rootMapId), MultiValue.None),
              Prepare.Put(MultiKey.KeysStart(MultiMap.rootMapId), MultiValue.None),
              Prepare.Put(MultiKey.KeysEnd(MultiMap.rootMapId), MultiValue.None),
              Prepare.Put(MultiKey.ChildrenStart(MultiMap.rootMapId), MultiValue.None),
              Prepare.Put(MultiKey.ChildrenEnd(MultiMap.rootMapId), MultiValue.None),
              Prepare.Put(MultiKey.End(MultiMap.rootMapId), MultiValue.None)
            )
          )

        //RootMap has empty keys so if this database is new commit initial entries.
        if (isEmpty)
          bag.transform(initialEntries) {
            _ =>
              swaydb.MultiMap[M, K, V, F, BAG](
                innerMap = rootMap,
                mapKey = null.asInstanceOf[M],
                mapId = MultiMap.rootMapId
              )
          }
        else
          bag.success(
            swaydb.MultiMap[M, K, V, F, BAG](
              innerMap = rootMap,
              mapKey = null.asInstanceOf[M],
              mapId = MultiMap.rootMapId
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
                                         mapKeySerializer: Serializer[M],
                                         valueSerializer: Serializer[V],
                                         ev: F <:< swaydb.PureFunction[K, V, Apply.Map[V]]) = {
      val f = new Functions[M, K, V, F]()
      functions.foreach(f.register(_))
      f
    }

    def apply[M, K, V, F](functions: Iterable[F])(implicit keySerializer: Serializer[K],
                                                  mapKeySerializer: Serializer[M],
                                                  valueSerializer: Serializer[V],
                                                  ev: F <:< swaydb.PureFunction[K, V, Apply.Map[V]]) = {
      val f = new Functions[M, K, V, F]()
      functions.foreach(f.register(_))
      f
    }
  }

  /**
   * All registered function for a [[MultiMap]].
   */
  final case class Functions[M, K, V, F]()(implicit keySerializer: Serializer[K],
                                           mapKeySerializer: Serializer[M],
                                           valueSerializer: Serializer[V]) {

    private implicit val optionalSerialiser = MultiValue.serialiser(valueSerializer)

    private[swaydb] val innerFunctions = swaydb.Map.Functions[MultiKey[M, K], MultiValue[V], swaydb.PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]]()

    def register[PF <: F](functions: PF*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Unit =
      functions.foreach(register(_))

    /**
     * Validates [[MultiKey]] supplied to the function. Only [[MultiKey.Key]] is accepted
     * since functions are applied to Map's key-values only.
     */
    @inline def validate[O](key: MultiKey[M, K])(f: K => O): O =
      key match {
        case MultiKey.Key(_, dataKey) =>
          f(dataKey)

        case entry: MultiKey[_, _] =>
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
    @inline def validate[O](key: MultiKey[M, K], value: MultiValue[V])(f: (K, V) => O): O =
      validate(key) {
        dataKey =>
          validate(value) {
            userValue =>
              f(dataKey, userValue)
          }
      }

    /**
     * Register the function converting it to [[MultiMap.innerMap]]'s function type.
     */
    def register[PF <: F](function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Unit = {
      val innerFunction =
        (function: swaydb.PureFunction[K, V, Apply.Map[V]]) match {
          //convert all MultiMap Functions to Map functions that register them since MultiMap is
          //just a parent implementation over Map.
          case function: swaydb.PureFunction.OnValue[V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnValue[MultiValue[V], Apply.Map[MultiValue[V]]] {
              //use user function's functionId
              override val id: String =
                function.id

              override def apply(value: MultiValue[V]): Apply.Map[MultiValue[V]] =
                validate(value) {
                  value =>
                    function
                      .apply(value)
                      .map(value => MultiValue.Their(value))
                }
            }

          case function: swaydb.PureFunction.OnKey[K, V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnKey[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]] {
              //use user function's functionId
              override val id: String =
                function.id

              override def apply(key: MultiKey[M, K], deadline: Option[Deadline]): Apply.Map[MultiValue[V]] =
                validate(key) {
                  dataKey =>
                    function
                      .apply(dataKey, deadline)
                      .map(value => MultiValue.Their(value))
                }
            }

          case function: swaydb.PureFunction.OnKeyValue[K, V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnKeyValue[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]] {
              //use user function's functionId
              override val id: String =
                function.id

              override def apply(key: MultiKey[M, K], value: MultiValue[V], deadline: Option[Deadline]): Apply.Map[MultiValue[V]] =
                validate(key, value) {
                  (dataKey, userValue) =>
                    function
                      .apply(dataKey, userValue, deadline)
                      .map(value => MultiValue.Their(value))
                }
            }
        }

      innerFunctions.register(innerFunction)
    }
  }


  /**
   * Converts [[Prepare]] statements of this [[MultiMap]] to inner [[Map]]'s statements.
   */
  def toInnerPrepare[M, K, V, F](prepare: Transaction[M, K, V, F]): Prepare[MultiKey[M, K], MultiValue[V], PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]] =
    toInnerPrepare(prepare.mapId, prepare.defaultExpiration, prepare.prepare)

  /**
   * Converts [[Prepare]] statements of this [[MultiMap]] to inner [[Map]]'s statements.
   */
  def toInnerPrepare[M, K, V, F](mapId: Long, defaultExpiration: Option[Deadline], prepare: Prepare[K, V, F]): Prepare[MultiKey[M, K], MultiValue[V], PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]] =
    prepare match {
      case Prepare.Put(key, value, deadline) =>
        Prepare.Put(MultiKey.Key(mapId, key), MultiValue.Their(value), deadline earlier defaultExpiration)

      case Prepare.Remove(from, to, deadline) =>
        to match {
          case Some(to) =>
            Prepare.Remove(MultiKey.Key(mapId, from), Some(MultiKey.Key(mapId, to)), deadline earlier defaultExpiration)

          case None =>
            Prepare.Remove[MultiKey[M, K]](from = MultiKey.Key(mapId, from), to = None, deadline = deadline earlier defaultExpiration)
        }

      case Prepare.Update(from, to, value) =>
        to match {
          case Some(to) =>
            Prepare.Update[MultiKey[M, K], MultiValue[V]](MultiKey.Key(mapId, from), Some(MultiKey.Key(mapId, to)), value = MultiValue.Their(value))

          case None =>
            Prepare.Update[MultiKey[M, K], MultiValue[V]](key = MultiKey.Key(mapId, from), value = MultiValue.Their(value))
        }

      case Prepare.ApplyFunction(from, to, function) =>
        // Temporary solution: casted because the actual instance itself not used internally.
        // Core only uses the String value of function.id which is searched in functionStore to validate function.
        val castedFunction = function.asInstanceOf[PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]]

        to match {
          case Some(to) =>
            Prepare.ApplyFunction(from = MultiKey.Key(mapId, from), to = Some(MultiKey.Key(mapId, to)), function = castedFunction)

          case None =>
            Prepare.ApplyFunction(from = MultiKey.Key(mapId, from), to = None, function = castedFunction)
        }

      case Prepare.Add(elem, deadline) =>
        Prepare.Put(MultiKey.Key(mapId, elem), MultiValue.None, deadline earlier defaultExpiration)
    }

}

/**
 * [[MultiMap]] extends [[swaydb.Map]]'s API to allow storing multiple Maps withing a single Map.
 *
 * [[MultiMap]] is just a simple extension that uses custom data types ([[MultiKey]]) and
 * KeyOrder ([[MultiKey.ordering]]) for it's API.
 */
case class MultiMap[M, K, V, F, BAG[_]] private(private[swaydb] val innerMap: Map[MultiKey[M, K], MultiValue[V], PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]], BAG],
                                                mapKey: M,
                                                mapId: Long,
                                                defaultExpiration: Option[Deadline] = None)(implicit keySerializer: Serializer[K],
                                                                                            mapKeySerializer: Serializer[M],
                                                                                            valueSerializer: Serializer[V],
                                                                                            counter: Counter,
                                                                                            val bag: Bag[BAG]) extends MapT[K, V, F, BAG] { self =>

  override def path: Path =
    innerMap.path

  /**
   * APIs for managing child map of this [[MultiMap]].
   */
  def schema: Schema[M, K, V, F, BAG] =
    new Schema(
      innerMap = innerMap,
      mapId = mapId,
      defaultExpiration = defaultExpiration
    )

  /**
   * Narrows this [[MultiMap]]'s map key type [[M]]
   */
  def narrow[M2](mapKey: Class[M2])(implicit evT: M2 <:< M): MultiMap[M2, K, V, F, BAG] =
    this.asInstanceOf[MultiMap[M2, K, V, F, BAG]]

  /**
   * Narrows this [[MultiMap]]'s map key type [[M]] and key-value key type [[K]]
   */
  def narrow[M2, K2](mapKey: Class[M2],
                     keyType: Class[K2])(implicit evT: M2 <:< M,
                                         evK: K2 <:< K): MultiMap[M2, K2, V, F, BAG] =
    this.asInstanceOf[MultiMap[M2, K2, V, F, BAG]]

  /**
   * Narrows this [[MultiMap]]'s map key type [[M]], key-value key type [[K]] and value type [[V]]
   */
  def narrow[M2, K2, V2](mapKey: Class[M2],
                         keyType: Class[K2],
                         valueType: Class[V2])(implicit evT: M2 <:< M,
                                               evK: K2 <:< K,
                                               evV: V2 <:< V): MultiMap[M2, K2, V2, F, BAG] =
    this.asInstanceOf[MultiMap[M2, K2, V2, F, BAG]]

  /**
   * Narrows this [[MultiMap]]'s map key type [[M]], key-value key type [[K]], value type [[V]] and function type [[F]].
   */
  def narrow[M2, K2, V2, F2](mapKey: Class[M2],
                             keyType: Class[K2],
                             valueType: Class[V2],
                             functionType: Class[F2])(implicit evT: M2 <:< M,
                                                      evK: K2 <:< K,
                                                      evV: V2 <:< V,
                                                      evF: F2 <:< F): MultiMap[M2, K2, V2, F2, BAG] =
    this.asInstanceOf[MultiMap[M2, K2, V2, F2, BAG]]

  def put(key: K, value: V): BAG[OK] =
    innerMap.put(MultiKey.Key(mapId, key), MultiValue.Their(value), defaultExpiration)

  def put(key: K, value: V, expireAfter: FiniteDuration): BAG[OK] =
    put(key, value, expireAfter.fromNow)

  def put(key: K, value: V, expireAt: Deadline): BAG[OK] =
    innerMap.put(MultiKey.Key(mapId, key), MultiValue.Their(value), defaultExpiration earlier expireAt)

  override def put(keyValues: (K, V)*): BAG[OK] = {
    val innerKeyValues =
      keyValues map {
        case (key, value) =>
          Prepare.Put(MultiKey.Key(mapId, key), MultiValue.Their(value), defaultExpiration)
      }

    innerMap.commit(innerKeyValues)
  }

  override def put(keyValues: Stream[(K, V), BAG]): BAG[OK] = {
    val stream: Stream[Prepare[MultiKey[M, K], MultiValue[V], PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]], BAG] =
      keyValues.map {
        case (key, value) =>
          Prepare.Put(MultiKey.Key(mapId, key), MultiValue.Their(value), defaultExpiration)
      }

    innerMap.commit(stream)
  }

  override def put(keyValues: Iterable[(K, V)]): BAG[OK] = {
    val stream =
      keyValues.map {
        case (key, value) =>
          Prepare.Put(MultiKey.Key(mapId, key), MultiValue.Their(value), defaultExpiration)
      }

    innerMap.commit(stream)
  }

  override def put(keyValues: Iterator[(K, V)]): BAG[OK] =
    put(keyValues.to(Iterable))

  def remove(key: K): BAG[OK] =
    innerMap.remove(MultiKey.Key(mapId, key))

  def remove(from: K, to: K): BAG[OK] =
    innerMap.remove(MultiKey.Key(mapId, from), MultiKey.Key(mapId, to))

  def remove(keys: K*): BAG[OK] =
    innerMap.remove {
      keys.map(key => MultiKey.Key(mapId, key))
    }

  def remove(keys: Stream[K, BAG]): BAG[OK] =
    bag.flatMap(keys.materialize)(remove)

  def remove(keys: Iterable[K]): BAG[OK] =
    remove(keys.iterator)

  def remove(keys: Iterator[K]): BAG[OK] =
    innerMap.remove(keys.map(key => MultiKey.Key(mapId, key)))

  def expire(key: K, after: FiniteDuration): BAG[OK] =
    innerMap.expire(MultiKey.Key(mapId, key), defaultExpiration.earlier(after.fromNow))

  def expire(key: K, at: Deadline): BAG[OK] =
    innerMap.expire(MultiKey.Key(mapId, key), defaultExpiration.earlier(at))

  def expire(from: K, to: K, after: FiniteDuration): BAG[OK] =
    innerMap.expire(MultiKey.Key(mapId, from), MultiKey.Key(mapId, to), defaultExpiration.earlier(after.fromNow))

  def expire(from: K, to: K, at: Deadline): BAG[OK] =
    innerMap.expire(MultiKey.Key(mapId, from), MultiKey.Key(mapId, to), defaultExpiration.earlier(at))

  def expire(keys: (K, Deadline)*): BAG[OK] =
    bag.suspend(expire(keys))

  def expire(keys: Stream[(K, Deadline), BAG]): BAG[OK] =
    bag.flatMap(keys.materialize)(expire)

  def expire(keys: Iterable[(K, Deadline)]): BAG[OK] =
    expire(keys.iterator)

  def expire(keys: Iterator[(K, Deadline)]): BAG[OK] = {
    val iterable: Iterable[Prepare[MultiKey[M, K], MultiValue[V], PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]]] =
      keys.map {
        case (key, deadline) =>
          Prepare.Expire(MultiKey.Key(mapId, key), deadline.earlier(defaultExpiration))
      }.to(Iterable)

    innerMap.commit(iterable)
  }

  def update(key: K, value: V): BAG[OK] =
    innerMap.update(MultiKey.Key(mapId, key), MultiValue.Their(value))

  def update(from: K, to: K, value: V): BAG[OK] =
    innerMap.update(MultiKey.Key(mapId, from), MultiKey.Key(mapId, to), MultiValue.Their(value))

  def update(keyValues: (K, V)*): BAG[OK] = {
    val updates =
      keyValues.map {
        case (key, value) =>
          Prepare.Update(MultiKey.Key(mapId, key), MultiValue.Their(value))
      }

    innerMap.commit(updates)
  }

  def update(keyValues: Stream[(K, V), BAG]): BAG[OK] =
    bag.flatMap(keyValues.materialize)(update)

  def update(keyValues: Iterable[(K, V)]): BAG[OK] = {
    val updates =
      keyValues.map {
        case (key, value) =>
          Prepare.Update(MultiKey.Key(mapId, key), MultiValue.Their(value))
      }

    innerMap.commit(updates)
  }

  def update(keyValues: Iterator[(K, V)]): BAG[OK] =
    update(keyValues.to(Iterable))

  def clearKeyValues(): BAG[OK] = {
    val entriesStart = MultiKey.KeysStart(mapId)
    val entriesEnd = MultiKey.KeysEnd(mapId)

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
    val innerKey = innerMap.keySerializer.write(MultiKey.Key(mapId, key))
    val functionId = Slice.writeString(function.id)
    innerMap.core.applyFunction(innerKey, functionId)
  }

  /**
   * @note In other operations like [[expire]], [[remove]], [[put]] the input expiration value is compared with [[defaultExpiration]]
   *       to get the nearest expiration. But functions does not check if the custom logic within the function expires
   *       key-values earlier than [[defaultExpiration]].
   */
  def applyFunction[PF <: F](from: K, to: K, function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] = {
    val fromKey = innerMap.keySerializer.write(MultiKey.Key(mapId, from))
    val toKey = innerMap.keySerializer.write(MultiKey.Key(mapId, to))
    val functionId = Slice.writeString(function.id)
    innerMap.core.applyFunction(fromKey, toKey, functionId)
  }

  /**
   * Converts [[Prepare]] statement for this map into [[Prepare]] statement for this Map's parent Map so that
   * multiple [[MultiMap]] [[Prepare]] statements can be executed as a single transaction.
   *
   * @see [[MultiMap.commit]] to commit [[Transaction]]s.
   */
  def toTransaction[PF <: F](prepare: Prepare[K, V, PF]*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Seq[Transaction[M, K, V, PF]] =
    prepare.map(prepare => new Transaction(mapId, defaultExpiration, prepare))

  /**
   * Converts [[Prepare]] statement for this map into [[Prepare]] statement for this Map's parent Map so that
   * multiple [[MultiMap]] [[Prepare]] statements can be executed as a single transaction.
   *
   * @see [[MultiMap.commit]] to commit [[Transaction]]s.
   */
  def toTransaction[PF <: F](prepare: Stream[Prepare[K, V, PF], BAG])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[Iterable[Transaction[M, K, V, PF]]] =
    bag.transform(prepare.materialize) {
      prepares =>
        toTransaction(prepares)
    }

  /**
   * Converts [[Prepare]] statement for this map into [[Prepare]] statement for this Map's parent Map so that
   * multiple [[MultiMap]] [[Prepare]] statements can be executed as a single transaction.
   *
   * @see [[MultiMap.commit]] to commit [[Transaction]]s.
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
          MultiMap.toInnerPrepare(transaction)
      }
    }

  def commit[PF <: F](prepare: Prepare[K, V, PF]*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    innerMap.commit(prepare.map(prepare => MultiMap.toInnerPrepare(mapId, defaultExpiration, prepare)))

  def commit[PF <: F](prepare: Stream[Prepare[K, V, PF], BAG])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    bag.flatMap(prepare.materialize) {
      prepares =>
        commit(prepares)
    }

  def commit[PF <: F](prepare: Iterable[Prepare[K, V, PF]])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    innerMap.commit(prepare.map(prepare => MultiMap.toInnerPrepare(mapId, defaultExpiration, prepare)))

  def get(key: K): BAG[Option[V]] =
    bag.flatMap(innerMap.get(MultiKey.Key(mapId, key))) {
      case Some(value) =>
        value match {
          case their: MultiValue.Their[V] =>
            bag.success(Some(their.value))

          case _: MultiValue.Our =>
            bag.failure(MultiMap.failure(classOf[MultiValue.Their[_]], classOf[MultiValue.Our]))

        }

      case None =>
        bag.none
    }

  def getKey(key: K): BAG[Option[K]] =
    bag.map(innerMap.getKey(MultiKey.Key(mapId, key))) {
      case Some(MultiKey.Key(_, key)) =>
        Some(key)

      case Some(entry) =>
        MultiMap.failure(MultiKey.Key.getClass, entry.getClass)

      case None =>
        None
    }

  def getKeyValue(key: K): BAG[Option[(K, V)]] =
    bag.map(innerMap.getKeyValue(MultiKey.Key(mapId, key))) {
      case Some((MultiKey.Key(_, key), their: MultiValue.Their[V])) =>
        Some((key, their.value))

      case Some((MultiKey.Key(_, _), _: MultiValue.Our)) =>
        MultiMap.failure(classOf[MultiValue.Their[V]], classOf[MultiValue.Our])

      case Some(entry) =>
        MultiMap.failure(MultiKey.Key.getClass, entry.getClass)

      case None =>
        None
    }

  override def getKeyDeadline(key: K): BAG[Option[(K, Option[Deadline])]] =
    getKeyDeadline(key, bag)

  def getKeyDeadline[BAG[_]](key: K, bag: Bag[BAG]): BAG[Option[(K, Option[Deadline])]] =
    bag.map(innerMap.getKeyDeadline(MultiKey.Key(mapId, key), bag)) {
      case Some((MultiKey.Key(_, key), deadline)) =>
        Some((key, deadline))

      case Some(entry) =>
        MultiMap.failure(MultiKey.Key.getClass, entry.getClass)

      case None =>
        None
    }

  override def getKeyValueDeadline[BAG[_]](key: K, bag: Bag[BAG]): BAG[Option[((K, V), Option[Deadline])]] =
    bag.map(innerMap.getKeyValueDeadline(MultiKey.Key(mapId, key), bag)) {
      case Some(((MultiKey.Key(_, key), value: MultiValue.Their[V]), deadline)) =>
        Some(((key, value.value), deadline))

      case Some(((key, value), _)) =>
        throw new Exception(
          s"Expected key ${classOf[MultiKey.Key[_]].getSimpleName}. Got ${key.getClass.getSimpleName}. " +
            s"Expected value ${classOf[MultiValue.Their[_]].getSimpleName}. Got ${value.getClass.getSimpleName}. "
        )

      case None =>
        None
    }

  def contains(key: K): BAG[Boolean] =
    innerMap.contains(MultiKey.Key(mapId, key))

  def mightContain(key: K): BAG[Boolean] =
    innerMap.mightContain(MultiKey.Key(mapId, key))

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
    innerMap.expiration(MultiKey.Key(mapId, key))

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
                  innerMap.stream.before(MultiKey.Key(mapId, from.key))
                else if (from.after)
                  innerMap.stream.after(MultiKey.Key(mapId, from.key))
                else if (from.orBefore)
                  innerMap.stream.fromOrBefore(MultiKey.Key(mapId, from.key))
                else if (from.orAfter)
                  innerMap.stream.fromOrAfter(MultiKey.Key(mapId, from.key))
                else
                  innerMap.stream.from(MultiKey.Key(mapId, from.key))

              if (reverse)
                start.reverse
              else
                start

            case None =>
              if (reverse)
                innerMap
                  .stream
                  .before(MultiKey.KeysEnd(mapId))
                  .reverse
              else
                innerMap
                  .stream
                  .after(MultiKey.KeysStart(mapId))
          }

        //restricts this Stream to fetch entries of this Map only.

        freeStream =
          appliedStream
            .free
            .takeWhile {
              case (MultiKey.Key(mapId, _), _) =>
                mapId == mapId

              case _ =>
                false
            }
            .collect {
              case (MultiKey.Key(_, key), their: MultiValue.Their[V]) =>
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

  override def clearAppliedFunctions(): BAG[Iterable[String]] =
    innerMap.clearAppliedFunctions()

  override def clearAppliedAndRegisteredFunctions(): BAG[Iterable[String]] =
    innerMap.clearAppliedAndRegisteredFunctions()

  override def isFunctionApplied[PF <: F](functionId: PF)(implicit ev: PF <:< PureFunction[K, V, Apply.Map[V]]): Boolean =
    innerMap.core.isFunctionApplied(Slice.writeString(functionId.id))

  /**
   * Returns an Async API of type O where the [[Bag]] is known.
   */
  def toBag[X[_]](implicit bag: Bag[X]): MultiMap[M, K, V, F, X] =
    MultiMap(
      innerMap = innerMap.toBag[X],
      mapKey = mapKey,
      mapId = mapId,
      defaultExpiration = defaultExpiration
    )

  def asScala: scala.collection.mutable.Map[K, V] =
    ScalaMap[K, V](toBag[Bag.Less](Bag.less))

  def close(): BAG[Unit] =
    bag.and(bag(counter.close)) {
      innerMap.close()
    }

  def delete(): BAG[Unit] =
    bag.and(bag(counter.close)) {
      innerMap.delete()
    }

  override def toString(): String =
    classOf[Map[_, _, _, BAG]].getSimpleName

}
