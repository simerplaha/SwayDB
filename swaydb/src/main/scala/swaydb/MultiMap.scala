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

import swaydb.multimap.Transaction
import swaydb.MultiMapKey.{MapEntriesEnd, MapEntriesStart, MapEntry}
import swaydb.core.util.Times._
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.multimap.Schema
import swaydb.serializers.{Serializer, _}

import scala.collection.compat._
import scala.collection.mutable
import scala.concurrent.duration.{Deadline, FiniteDuration}

object MultiMap {

  /**
   * Given the inner [[swaydb.Map]] instance this creates a parent [[MultiMap]] instance.
   */
  private[swaydb] def apply[M, K, V, F, BAG[_]](rootMap: swaydb.Map[MultiMapKey[M, K], Option[V], PureFunction[MultiMapKey[M, K], Option[V], Apply.Map[Option[V]]], BAG])(implicit bag: swaydb.Bag[BAG],
                                                                                                                                                                          keySerializer: Serializer[K],
                                                                                                                                                                          tableSerializer: Serializer[M],
                                                                                                                                                                          valueSerializer: Serializer[V]): BAG[MultiMap[M, K, V, F, BAG]] =
    bag.flatMap(rootMap.isEmpty) {
      isEmpty =>
        val rootMapKey = Seq.empty[M]

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

        //RootMap has empty keys so if this database is new commit initial entries.
        if (isEmpty)
          bag.transform(initialEntries) {
            _ =>
              swaydb.MultiMap[M, K, V, F, BAG](
                innerMap = rootMap,
                thisMapKey = rootMapKey
              )
          }
        else
          bag.success(
            swaydb.MultiMap[M, K, V, F, BAG](
              innerMap = rootMap,
              thisMapKey = rootMapKey
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
   * All registered function for a [[MultiMap]].
   */
  final case class Functions[M, K, V, F]()(implicit keySerializer: Serializer[K],
                                           tableSerializer: Serializer[M],
                                           valueSerializer: Serializer[V]) {

    private implicit val optionalSerialiser = Serializer.toNestedOption(valueSerializer)

    private[swaydb] val innerFunctions = swaydb.Map.Functions[MultiMapKey[M, K], Option[V], swaydb.PureFunction[MultiMapKey[M, K], Option[V], Apply.Map[Option[V]]]]()

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
    @inline def validate[O](value: Option[V])(f: V => O): O =
      value match {
        case Some(userValue) =>
          f(userValue)

        case None =>
          //UserEntries are never None for innertMap.
          throw new Exception("Function applied to None user value")
      }

    /**
     * Vaidates both key and value before applying the function.
     */
    @inline def validate[O](key: MultiMapKey[M, K], value: Option[V])(f: (K, V) => O): O =
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
            new swaydb.PureFunction.OnValue[Option[V], Apply.Map[Option[V]]] {
              override def apply(value: Option[V]): Apply.Map[Option[V]] =
                validate(value) {
                  value =>
                    Apply.Map.toOption(function.apply(value))
                }

              //use user function's functionId
              override def id: String =
                function.id
            }

          case function: swaydb.PureFunction.OnKey[K, V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnKey[MultiMapKey[M, K], Option[V], Apply.Map[Option[V]]] {
              override def apply(key: MultiMapKey[M, K], deadline: Option[Deadline]): Apply.Map[Option[V]] =
                validate(key) {
                  dataKey =>
                    Apply.Map.toOption(function.apply(dataKey, deadline))
                }

              //use user function's functionId
              override def id: String =
                function.id
            }

          case function: swaydb.PureFunction.OnKeyValue[K, V, Apply.Map[V]] =>
            new swaydb.PureFunction.OnKeyValue[MultiMapKey[M, K], Option[V], Apply.Map[Option[V]]] {
              override def apply(key: MultiMapKey[M, K], value: Option[V], deadline: Option[Deadline]): Apply.Map[Option[V]] =
                validate(key, value) {
                  (dataKey, userValue) =>
                    Apply.Map.toOption(function.apply(dataKey, userValue, deadline))
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
   * Converts [[Prepare]] statements of this [[MultiMap]] to inner [[Map]]'s statements.
   */
  def toInnerPrepare[M, K, V, F](prepare: Transaction[M, K, V, F]): Prepare[MultiMapKey[M, K], Option[V], PureFunction[MultiMapKey[M, K], Option[V], Apply.Map[Option[V]]]] =
    toInnerPrepare(prepare.thisMapKey, prepare.defaultExpiration, prepare.prepare)

  /**
   * Converts [[Prepare]] statements of this [[MultiMap]] to inner [[Map]]'s statements.
   */
  def toInnerPrepare[M, K, V, F](thisMapKey: Iterable[M], defaultExpiration: Option[Deadline], prepare: Prepare[K, V, F]): Prepare[MultiMapKey[M, K], Option[V], PureFunction[MultiMapKey[M, K], Option[V], Apply.Map[Option[V]]]] =
    prepare match {
      case Prepare.Put(key, value, deadline) =>
        Prepare.Put(MapEntry(thisMapKey, key), Some(value), deadline earlier defaultExpiration)

      case Prepare.Remove(from, to, deadline) =>
        to match {
          case Some(to) =>
            Prepare.Remove(MapEntry(thisMapKey, from), Some(MapEntry(thisMapKey, to)), deadline earlier defaultExpiration)

          case None =>
            Prepare.Remove[MultiMapKey[M, K]](from = MapEntry(thisMapKey, from), to = None, deadline = deadline earlier defaultExpiration)
        }

      case Prepare.Update(from, to, value) =>
        to match {
          case Some(to) =>
            Prepare.Update[MultiMapKey[M, K], Option[V]](MapEntry(thisMapKey, from), Some(MapEntry(thisMapKey, to)), value = Some(value))

          case None =>
            Prepare.Update[MultiMapKey[M, K], Option[V]](key = MapEntry(thisMapKey, from), value = Some(value))
        }

      case Prepare.ApplyFunction(from, to, function) =>
        // Temporary solution: casted because the actual instance itself not used internally.
        // Core only uses the String value of function.id which is searched in functionStore to validate function.
        val castedFunction = function.asInstanceOf[PureFunction[MultiMapKey[M, K], Option[V], Apply.Map[Option[V]]]]

        to match {
          case Some(to) =>
            Prepare.ApplyFunction(from = MapEntry(thisMapKey, from), to = Some(MapEntry(thisMapKey, to)), function = castedFunction)

          case None =>
            Prepare.ApplyFunction(from = MapEntry(thisMapKey, from), to = None, function = castedFunction)
        }

      case Prepare.Add(elem, deadline) =>
        Prepare.Put(MapEntry(thisMapKey, elem), None, deadline earlier defaultExpiration)
    }

}

/**
 * [[MultiMap]] extends [[swaydb.Map]]'s API to allow storing multiple Maps withing a single Map.
 *
 * [[MultiMap]] is just a simple extension that uses custom data types ([[MultiMapKey]]) and
 * KeyOrder ([[MultiMapKey.ordering]]) for it's API.
 */
case class MultiMap[M, K, V, F, BAG[_]] private(private[swaydb] val innerMap: Map[MultiMapKey[M, K], Option[V], PureFunction[MultiMapKey[M, K], Option[V], Apply.Map[Option[V]]], BAG],
                                                thisMapKey: Iterable[M],
                                                private val from: Option[From[MultiMapKey.MapEntry[M, K]]] = None,
                                                private val reverseIteration: Boolean = false,
                                                defaultExpiration: Option[Deadline] = None)(implicit keySerializer: Serializer[K],
                                                                                            tableSerializer: Serializer[M],
                                                                                            valueSerializer: Serializer[V],
                                                                                            val bag: Bag[BAG]) extends MapT[K, V, F, BAG] { self =>

  override def path: Path =
    innerMap.path

  /**
   * APIs for managing child map of this [[MultiMap]].
   */
  val schema: Schema[M, K, V, F, BAG] =
    new Schema(
      innerMap = innerMap,
      thisMapKey = thisMapKey,
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
    innerMap.put(MapEntry(thisMapKey, key), Some(value), defaultExpiration)

  def put(key: K, value: V, expireAfter: FiniteDuration): BAG[OK] =
    put(key, value, expireAfter.fromNow)

  def put(key: K, value: V, expireAt: Deadline): BAG[OK] =
    innerMap.put(MapEntry(thisMapKey, key), Some(value), defaultExpiration earlier expireAt)

  override def put(keyValues: (K, V)*): BAG[OK] = {
    val innerKeyValues =
      keyValues map {
        case (key, value) =>
          Prepare.Put(MapEntry(thisMapKey, key), Some(value), defaultExpiration)
      }

    innerMap.commit(innerKeyValues)
  }

  override def put(keyValues: Stream[(K, V)]): BAG[OK] = {
    val stream: Stream[Prepare[MultiMapKey[M, K], Option[V], PureFunction[MultiMapKey[M, K], Option[V], Apply.Map[Option[V]]]]] =
      keyValues.map {
        case (key, value) =>
          Prepare.Put(MapEntry(thisMapKey, key), Some(value), defaultExpiration)
      }

    innerMap.commit(stream)
  }

  override def put(keyValues: Iterable[(K, V)]): BAG[OK] = {
    val stream =
      keyValues.map {
        case (key, value) =>
          Prepare.Put(MapEntry(thisMapKey, key), Some(value), defaultExpiration)
      }

    innerMap.commit(stream)
  }

  override def put(keyValues: Iterator[(K, V)]): BAG[OK] =
    put(keyValues.to(Iterable))

  def remove(key: K): BAG[OK] =
    innerMap.remove(MapEntry(thisMapKey, key))

  def remove(from: K, to: K): BAG[OK] =
    innerMap.remove(MapEntry(thisMapKey, from), MapEntry(thisMapKey, to))

  def remove(keys: K*): BAG[OK] =
    innerMap.remove {
      keys.map(key => MapEntry(thisMapKey, key))
    }

  def remove(keys: Stream[K]): BAG[OK] =
    bag.flatMap(keys.materialize)(remove)

  def remove(keys: Iterable[K]): BAG[OK] =
    remove(keys.iterator)

  def remove(keys: Iterator[K]): BAG[OK] =
    innerMap.remove(keys.map(key => MapEntry(thisMapKey, key)))

  def expire(key: K, after: FiniteDuration): BAG[OK] =
    innerMap.expire(MapEntry(thisMapKey, key), defaultExpiration.earlier(after.fromNow))

  def expire(key: K, at: Deadline): BAG[OK] =
    innerMap.expire(MapEntry(thisMapKey, key), defaultExpiration.earlier(at))

  def expire(from: K, to: K, after: FiniteDuration): BAG[OK] =
    innerMap.expire(MapEntry(thisMapKey, from), MapEntry(thisMapKey, to), defaultExpiration.earlier(after.fromNow))

  def expire(from: K, to: K, at: Deadline): BAG[OK] =
    innerMap.expire(MapEntry(thisMapKey, from), MapEntry(thisMapKey, to), defaultExpiration.earlier(at))

  def expire(keys: (K, Deadline)*): BAG[OK] =
    bag.suspend(expire(keys))

  def expire(keys: Stream[(K, Deadline)]): BAG[OK] =
    bag.flatMap(keys.materialize)(expire)

  def expire(keys: Iterable[(K, Deadline)]): BAG[OK] =
    expire(keys.iterator)

  def expire(keys: Iterator[(K, Deadline)]): BAG[OK] = {
    val iterable: Iterable[Prepare[MultiMapKey[M, K], Option[V], PureFunction[MultiMapKey[M, K], Option[V], Apply.Map[Option[V]]]]] =
      keys.map {
        case (key, deadline) =>
          Prepare.Expire(MapEntry(thisMapKey, key), deadline.earlier(defaultExpiration))
      }.to(Iterable)

    innerMap.commit(iterable)
  }

  def update(key: K, value: V): BAG[OK] =
    innerMap.update(MapEntry(thisMapKey, key), Some(value))

  def update(from: K, to: K, value: V): BAG[OK] =
    innerMap.update(MapEntry(thisMapKey, from), MapEntry(thisMapKey, to), Some(value))

  def update(keyValues: (K, V)*): BAG[OK] = {
    val updates =
      keyValues.map {
        case (key, value) =>
          Prepare.Update(MapEntry(thisMapKey, key), Some(value))
      }

    innerMap.commit(updates)
  }

  def update(keyValues: Stream[(K, V)]): BAG[OK] =
    bag.flatMap(keyValues.materialize)(update)

  def update(keyValues: Iterable[(K, V)]): BAG[OK] = {
    val updates =
      keyValues.map {
        case (key, value) =>
          Prepare.Update(MapEntry(thisMapKey, key), Some(value))
      }

    innerMap.commit(updates)
  }

  def update(keyValues: Iterator[(K, V)]): BAG[OK] =
    update(keyValues.to(Iterable))

  def clearKeyValues(): BAG[OK] = {
    val entriesStart = MapEntriesStart(thisMapKey)
    val entriesEnd = MapEntriesEnd(thisMapKey)

    val entries =
      Seq(
        Prepare.Remove(entriesStart, entriesEnd),
        Prepare.Put(entriesStart, None),
        Prepare.Put(entriesEnd, None)
      )

    innerMap.commit(entries)
  }

  /**
   * @note In other operations like [[expire]], [[remove]], [[put]] the input expiration value is compared with [[defaultExpiration]]
   *       to get the nearest expiration. But functions does not check if the custom logic within the function expires
   *       key-values earlier than [[defaultExpiration]].
   */
  def applyFunction[PF <: F](key: K, function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] = {
    val innerKey = innerMap.keySerializer.write(MapEntry(thisMapKey, key))
    val functionId = Slice.writeString(function.id)
    innerMap.core.function(innerKey, functionId)
  }

  /**
   * @note In other operations like [[expire]], [[remove]], [[put]] the input expiration value is compared with [[defaultExpiration]]
   *       to get the nearest expiration. But functions does not check if the custom logic within the function expires
   *       key-values earlier than [[defaultExpiration]].
   */
  def applyFunction[PF <: F](from: K, to: K, function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] = {
    val fromKey = innerMap.keySerializer.write(MapEntry(thisMapKey, from))
    val toKey = innerMap.keySerializer.write(MapEntry(thisMapKey, to))
    val functionId = Slice.writeString(function.id)
    innerMap.core.function(fromKey, toKey, functionId)
  }

  /**
   * Converts [[Prepare]] statement for this map into [[Prepare]] statement for this Map's parent Map so that
   * multiple [[MultiMap]] [[Prepare]] statements can be executed as a single transaction.
   *
   * @see [[MultiMap.commit]] to commit [[Transaction]]s.
   */
  def toTransaction[PF <: F](prepare: Prepare[K, V, PF]*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): Seq[Transaction[M, K, V, PF]] =
    prepare.map(prepare => new Transaction(thisMapKey, defaultExpiration, prepare))

  /**
   * Converts [[Prepare]] statement for this map into [[Prepare]] statement for this Map's parent Map so that
   * multiple [[MultiMap]] [[Prepare]] statements can be executed as a single transaction.
   *
   * @see [[MultiMap.commit]] to commit [[Transaction]]s.
   */
  def toTransaction[PF <: F](prepare: Stream[Prepare[K, V, PF]])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[Iterable[Transaction[M, K, V, PF]]] =
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
    prepare.map(prepare => new Transaction(thisMapKey, defaultExpiration, prepare))

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
    innerMap.commit(prepare.map(prepare => MultiMap.toInnerPrepare(thisMapKey, defaultExpiration, prepare)))

  def commit[PF <: F](prepare: Stream[Prepare[K, V, PF]])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    bag.flatMap(prepare.materialize) {
      prepares =>
        commit(prepares)
    }

  def commit[PF <: F](prepare: Iterable[Prepare[K, V, PF]])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK] =
    innerMap.commit(prepare.map(prepare => MultiMap.toInnerPrepare(thisMapKey, defaultExpiration, prepare)))

  def get(key: K): BAG[Option[V]] =
    bag.flatMap(innerMap.get(MultiMapKey.MapEntry(thisMapKey, key))) {
      case Some(value) =>
        value match {
          case some @ Some(_) =>
            bag.success(some)

          case None =>
            bag.failure(MultiMap.failure(classOf[MapEntry[_, _]], None.getClass))
        }

      case None =>
        bag.none
    }

  def getKey(key: K): BAG[Option[K]] =
    bag.map(innerMap.getKey(MapEntry(thisMapKey, key))) {
      case Some(MapEntry(_, key)) =>
        Some(key)

      case Some(entry) =>
        MultiMap.failure(MapEntry.getClass, entry.getClass)

      case None =>
        None
    }

  def getKeyValue(key: K): BAG[Option[(K, V)]] =
    bag.map(innerMap.getKeyValue(MapEntry(thisMapKey, key))) {
      case Some((MapEntry(_, key), Some(value))) =>
        Some((key, value))

      case Some((MapEntry(_, _), None)) =>
        MultiMap.failure("Value", "None")

      case Some(entry) =>
        MultiMap.failure(MapEntry.getClass, entry.getClass)

      case None =>
        None
    }

  override def getKeyDeadline(key: K): BAG[Option[(K, Option[Deadline])]] =
    getKeyDeadline(key, bag)

  def getKeyDeadline[BAG[_]](key: K, bag: Bag[BAG]): BAG[Option[(K, Option[Deadline])]] =
    bag.map(innerMap.getKeyDeadline(MapEntry(thisMapKey, key), bag)) {
      case Some((MapEntry(_, key), deadline)) =>
        Some((key, deadline))

      case Some(entry) =>
        MultiMap.failure(MapEntry.getClass, entry.getClass)

      case None =>
        None
    }

  def contains(key: K): BAG[Boolean] =
    innerMap.contains(MapEntry(thisMapKey, key))

  def mightContain(key: K): BAG[Boolean] =
    innerMap.mightContain(MapEntry(thisMapKey, key))

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
    innerMap.expiration(MapEntry(thisMapKey, key))

  def timeLeft(key: K): BAG[Option[FiniteDuration]] =
    bag.map(expiration(key))(_.map(_.timeLeft))

  def from(key: K): MultiMap[M, K, V, F, BAG] =
    copy(from = Some(From(key = MapEntry(thisMapKey, key), orBefore = false, orAfter = false, before = false, after = false)))

  def before(key: K): MultiMap[M, K, V, F, BAG] =
    copy(from = Some(From(key = MapEntry(thisMapKey, key), orBefore = false, orAfter = false, before = true, after = false)))

  def fromOrBefore(key: K): MultiMap[M, K, V, F, BAG] =
    copy(from = Some(From(key = MapEntry(thisMapKey, key), orBefore = true, orAfter = false, before = false, after = false)))

  def after(key: K): MultiMap[M, K, V, F, BAG] =
    copy(from = Some(From(key = MapEntry(thisMapKey, key), orBefore = false, orAfter = false, before = false, after = true)))

  def fromOrAfter(key: K): MultiMap[M, K, V, F, BAG] =
    copy(from = Some(From(key = MapEntry(thisMapKey, key), orBefore = false, orAfter = true, before = false, after = false)))

  def headOption: BAG[Option[(K, V)]] =
    stream.headOption

  def headOrNull: BAG[(K, V)] =
    stream.headOrNull

  //restricts this Stream to fetch entries of this Map only.
  private def boundStreamToMap(stream: Stream[(MultiMapKey[M, K], Option[V])]): Stream[(K, V)] =
    stream
      .takeWhile {
        case (MapEntry(parent, _), _) =>
          parent == thisMapKey

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
            innerMap.before(from.key)
          else if (from.after)
            innerMap.after(from.key)
          else if (from.orBefore)
            innerMap.fromOrBefore(from.key)
          else if (from.orAfter)
            innerMap.fromOrAfter(from.key)
          else
            innerMap.from(from.key)

        if (reverseIteration)
          boundStreamToMap(start.reverse.stream)
        else
          boundStreamToMap(start.stream)

      case None =>
        if (reverseIteration)
          boundStreamToMap {
            innerMap
              .before(MapEntriesEnd(thisMapKey))
              .reverse
              .stream
          }
        else
          boundStreamToMap {
            innerMap
              .after(MapEntriesStart(thisMapKey))
              .stream
          }
    }

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

  def reverse: MultiMap[M, K, V, F, BAG] =
    copy(reverseIteration = true)

  /**
   * Returns an Async API of type O where the [[Bag]] is known.
   */
  def toBag[X[_]](implicit bag: Bag[X]): MultiMap[M, K, V, F, X] =
    MultiMap(innerMap.toBag[X], thisMapKey, from, reverseIteration, defaultExpiration)

  def asScala: scala.collection.mutable.Map[K, V] =
    ScalaMap[K, V, F](toBag[Bag.Less](Bag.less))

  def close(): BAG[Unit] =
    innerMap.close()

  def delete(): BAG[Unit] =
    innerMap.delete()

  override def toString(): String =
    classOf[Map[_, _, _, BAG]].getSimpleName
}
