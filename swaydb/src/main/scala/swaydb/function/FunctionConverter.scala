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

package swaydb.function

import java.util.Optional

import swaydb.core.function.FunctionStore
import swaydb.data.Functions
import swaydb.multimap.{MultiKey, MultiValue}
import swaydb.serializers.Serializer
import swaydb.{Apply, Expiration, MultiMap, PureFunction, PureFunctionJava, PureFunctionScala, SwayDB}

import scala.concurrent.duration.Deadline

object FunctionConverter {

  def toCore[K, V, R <: Apply[V], F <: PureFunction[K, V, R]](function: F)(implicit keySerializer: Serializer[K],
                                                                           valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction =


    function match {
      case function: PureFunctionScala.OnEntry[K] =>
        SwayDB.toCoreFunctionKey[K, V](function)

      case function: PureFunctionScala.OnEntryDeadline[K] =>
        SwayDB.toCoreFunctionKeyDeadline[K, V](function)

      case function: PureFunctionScala.OnKey[K, V] =>
        SwayDB.toCoreFunctionKey[K, V](function)

      case function: PureFunctionScala.OnKeyDeadline[K, V] =>
        SwayDB.toCoreFunctionKeyDeadline[K, V](function)

      case function: PureFunctionScala.OnKeyValue[K, V] =>
        SwayDB.toCoreFunctionKeyValue[K, V](function)

      case function: PureFunctionScala.OnValue[V] =>
        SwayDB.toCoreFunctionValue[V](function)

      case function: PureFunctionScala.OnValueDeadline[V] =>
        SwayDB.toCoreFunctionValueDeadline[V](function)

      case function: PureFunctionScala.OnKeyValueDeadline[K, V] =>
        SwayDB.toCoreFunctionKeyValueDeadline[K, V](function)

      case function: PureFunctionJava.OnEntry[K] =>
        SwayDB.toCoreFunctionKey[K, V](
          key =>
            function.apply(key).asInstanceOf[Apply.Set[V]]
        )

      case function: PureFunctionJava.OnEntryExpiration[K] =>
        SwayDB.toCoreFunctionKeyExpiration[K, V](
          (key, expiration) =>
            function.apply(key, expiration).asInstanceOf[Apply.Set[V]]
        )

      case function: PureFunctionJava.OnKey[K, V] =>
        SwayDB.toCoreFunctionKey[K, V](function)

      case function: PureFunctionJava.OnKeyExpiration[K, V] =>
        SwayDB.toCoreFunctionKeyExpiration[K, V](function)

      case function: PureFunctionJava.OnValue[K, V] =>
        SwayDB.toCoreFunctionValue[V](function)

      case function: PureFunctionJava.OnValueExpiration[K, V] =>
        SwayDB.toCoreFunctionValueExpiration[V](function)

      case function: PureFunctionJava.OnKeyValue[K, V] =>
        SwayDB.toCoreFunctionKeyValue[K, V](function)

      case function: PureFunctionJava.OnKeyValueExpiration[K, V] =>
        SwayDB.toCoreFunctionKeyValueExpiration[K, V](function)
    }

  def toCore[K, V, R <: Apply[V], F <: PureFunction[K, V, R]](functions: Functions[F])(implicit keySerializer: Serializer[K],
                                                                                       valueSerializer: Serializer[V]): Functions[swaydb.core.data.SwayFunction] =
    Functions(functions.map(function => toCore[K, V, R, F](function)))

  def toFunctionsStore[K, V, R <: Apply[V], F <: PureFunction[K, V, R]](functions: Functions[F])(implicit keySerializer: Serializer[K],
                                                                                                 valueSerializer: Serializer[V]): FunctionStore = {
    val functionStore = FunctionStore.memory()

    functions foreach {
      function =>
        functionStore.put(function.id, toCore[K, V, R, F](function))
    }

    functionStore
  }

  /**
   * Register the function converting it to [[MultiMap.innerMap]]'s function type.
   */
  def toMultiMap[M, K, V, R <: Apply.Map[V], F <: PureFunction[K, V, R]](function: Functions[F]): Functions[PureFunction.Map[MultiKey[M, K], MultiValue[V]]] =
    Functions(function.map(function => toMultiMap[M, K, V, R, F](function)))

  def toMultiMap[M, K, V, R <: Apply.Map[V], F <: PureFunction[K, V, R]](function: F): PureFunction.Map[MultiKey[M, K], MultiValue[V]] = {

    /**
     * Validates [[MultiKey]] supplied to the function. Only [[MultiKey.Key]] is accepted
     * since functions are applied to Map's key-values only.
     */
    @inline def validateKey[O](key: MultiKey[M, K])(f: K => O): O =
      key match {
        case MultiKey.Key(_, dataKey) =>
          f(dataKey)

        case entry: MultiKey[_, _] =>
          throw new Exception(s"MapEntry expected but got ${entry.getClass.getName}")
      }

    /**
     * User values for inner map cannot be None.
     */
    @inline def validateValue[O](value: MultiValue[V])(f: V => O): O =
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
    @inline def validateKeyValue[O](key: MultiKey[M, K], value: MultiValue[V])(f: (K, V) => O): O =
      validateKey(key) {
        dataKey =>
          validateValue(value) {
            userValue =>
              f(dataKey, userValue)
          }
      }

    function match {

      /**
       * SCALA FUNCTIONS
       */

      case function: PureFunctionScala.OnKey[K, V] =>
        new PureFunctionScala.OnKey[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(elem: MultiKey[M, K]): Apply.Map[MultiValue[V]] =
            validateKey(elem) {
              dataKey =>
                function
                  .apply(dataKey)
                  .mapValue(value => MultiValue.Their(value))
            }
        }

      case function: PureFunctionScala.OnKeyDeadline[K, V] =>
        new PureFunctionScala.OnKeyDeadline[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(key: MultiKey[M, K], deadline: Option[Deadline]): Apply.Map[MultiValue[V]] =
            validateKey(key) {
              dataKey =>
                function
                  .apply(dataKey, deadline)
                  .mapValue(value => MultiValue.Their(value))
            }
        }

      case function: PureFunctionScala.OnKeyValue[K, V] =>
        new PureFunctionScala.OnKeyValue[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(key: MultiKey[M, K], value: MultiValue[V]): Apply.Map[MultiValue[V]] =
            validateKeyValue(key, value) {
              (dataKey, dataValue) =>
                function
                  .apply(dataKey, dataValue)
                  .mapValue(value => MultiValue.Their(value))
            }
        }

      case function: PureFunctionScala.OnValue[V] =>
        new PureFunctionScala.OnValue[MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(value: MultiValue[V]): Apply.Map[MultiValue[V]] =
            validateValue(value) {
              dataValue =>
                function
                  .apply(dataValue)
                  .mapValue(value => MultiValue.Their(value))
            }
        }

      case function: PureFunctionScala.OnValueDeadline[V] =>
        new PureFunctionScala.OnValueDeadline[MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(value: MultiValue[V], deadline: Option[Deadline]): Apply.Map[MultiValue[V]] =
            validateValue(value) {
              dataValue =>
                function
                  .apply(dataValue, deadline)
                  .mapValue(value => MultiValue.Their(value))
            }
        }

      case function: PureFunctionScala.OnKeyValueDeadline[K, V] =>
        new PureFunctionScala.OnKeyValueDeadline[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(key: MultiKey[M, K], value: MultiValue[V], deadline: Option[Deadline]): Apply.Map[MultiValue[V]] =
            validateKeyValue(key, value) {
              (dataKey, dataValue) =>
                function
                  .apply(dataKey, dataValue, deadline)
                  .mapValue(value => MultiValue.Their(value))
            }
        }

      /**
       * JAVA FUNCTIONS
       */
      case function: PureFunctionJava.OnKey[K, V] =>
        new PureFunctionJava.OnKey[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(key: MultiKey[M, K]): Apply.Map[MultiValue[V]] =
            validateKey(key) {
              dataKey =>
                function
                  .apply(dataKey)
                  .mapValue(value => MultiValue.Their(value))
            }
        }

      case function: PureFunctionJava.OnKeyExpiration[K, V] =>
        new PureFunctionJava.OnKeyExpiration[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(key: MultiKey[M, K], expiration: Optional[Expiration]): Apply.Map[MultiValue[V]] =
            validateKey(key) {
              dataKey =>
                function
                  .apply(dataKey, expiration)
                  .mapValue(value => MultiValue.Their(value))
            }
        }

      case function: PureFunctionJava.OnValue[K, V] =>
        new PureFunctionJava.OnValue[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(value: MultiValue[V]): Apply.Map[MultiValue[V]] =
            validateValue(value) {
              dataValue =>
                function
                  .apply(dataValue)
                  .mapValue(value => MultiValue.Their(value))
            }
        }

      case function: PureFunctionJava.OnValueExpiration[K, V] =>
        new PureFunctionJava.OnValueExpiration[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(value: MultiValue[V], expiration: Optional[Expiration]): Apply.Map[MultiValue[V]] =
            validateValue(value) {
              dataValue =>
                function
                  .apply(dataValue, expiration)
                  .mapValue(value => MultiValue.Their(value))
            }
        }

      case function: PureFunctionJava.OnKeyValue[K, V] =>
        new PureFunctionJava.OnKeyValue[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(key: MultiKey[M, K], value: MultiValue[V]): Apply.Map[MultiValue[V]] =
            validateKeyValue(key, value) {
              (dataKey, dataValue) =>
                function
                  .apply(dataKey, dataValue)
                  .mapValue(value => MultiValue.Their(value))
            }
        }

      case function: PureFunctionJava.OnKeyValueExpiration[K, V] =>
        new PureFunctionJava.OnKeyValueExpiration[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(key: MultiKey[M, K], value: MultiValue[V], expiration: Optional[Expiration]): Apply.Map[MultiValue[V]] =
            validateKeyValue(key, value) {
              (dataKey, dataValue) =>
                function
                  .apply(dataKey, dataValue, expiration)
                  .mapValue(value => MultiValue.Their(value))
            }
        }

      /**
       * The following Set's OnEntry functions are not expected in MultiMap.
       *
       * But if this function gets passed a Set key-value then it converts
       * it ot a Map function applying the same logic and output of the Set function.
       */
      case function: PureFunctionScala.OnEntry[K] =>
        new PureFunctionScala.OnKey[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(elem: MultiKey[M, K]): Apply.Map[MultiValue[V]] =
            validateKey(elem) {
              dataKey =>
                function
                  .apply(dataKey)
                  .toMap
            }
        }

      case function: PureFunctionScala.OnEntryDeadline[K] =>
        new PureFunctionScala.OnKeyDeadline[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(elem: MultiKey[M, K], deadline: Option[Deadline]): Apply.Map[MultiValue[V]] =
            validateKey(elem) {
              dataKey =>
                function
                  .apply(dataKey, deadline)
                  .toMap
            }
        }

      case function: PureFunctionJava.OnEntry[K] =>
        new PureFunctionJava.OnKey[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(key: MultiKey[M, K]): Apply.Map[MultiValue[V]] =
            validateKey(key) {
              dataKey =>
                function
                  .apply(dataKey)
                  .toMap
            }
        }

      case function: PureFunctionJava.OnEntryExpiration[K] =>
        new PureFunctionJava.OnKeyExpiration[MultiKey[M, K], MultiValue[V]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(key: MultiKey[M, K], expiration: Optional[Expiration]): Apply.Map[MultiValue[V]] =
            validateKey(key) {
              dataKey =>
                function
                  .apply(dataKey, expiration)
                  .toMap
            }
        }
    }
  }
}
