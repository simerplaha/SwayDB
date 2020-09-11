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

import swaydb.core.function.FunctionStore
import swaydb.data.Functions
import swaydb.multimap.{MultiKey, MultiValue}
import swaydb.serializers.Serializer
import swaydb.{Apply, MultiMap, PureFunction, SwayDB}

import scala.concurrent.duration.Deadline

object FunctionConverter {

  def toCore[K, V, R <: Apply[V], F <: PureFunction[K, V, R]](function: F)(implicit keySerializer: Serializer[K],
                                                                           valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction =
    function match {
      case function: PureFunction.OnValue[V, Apply.Map[V]] =>
        SwayDB.toCoreFunction(function)

      case function: PureFunction.OnKey[K, V, Apply.Map[V]] =>
        SwayDB.toCoreFunction(function)

      case function: PureFunction.OnKeyValue[K, V, Apply.Map[V]] =>
        SwayDB.toCoreFunction(function)
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
  def toMultiMap[M, K, V, R <: Apply[V], F <: PureFunction[K, V, R]](function: Functions[F]): Functions[PureFunction.Map[MultiKey[M, K], MultiValue[V]]] =
    Functions(function.map(function => toMultiMap[M, K, V, R, F](function)))

  def toMultiMap[M, K, V, R <: Apply[V], F <: PureFunction[K, V, R]](function: F): PureFunction.Map[MultiKey[M, K], MultiValue[V]] = {

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
      //convert all MultiMap Functions to Map functions that register them since MultiMap is
      //just a parent implementation over Map.
      case function: PureFunction.OnValue[V, Apply.Map[V]] =>
        new PureFunction.OnValue[MultiValue[V], Apply.Map[MultiValue[V]]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(value: MultiValue[V]): Apply.Map[MultiValue[V]] =
            validateValue(value) {
              value =>
                function
                  .apply(value)
                  .map(value => MultiValue.Their(value))
            }
        }

      case function: PureFunction.OnKey[K, V, Apply.Map[V]] =>
        new PureFunction.OnKey[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]] {
          //use user function's functionId
          override val id: String =
            function.id

          override def apply(key: MultiKey[M, K], deadline: Option[Deadline]): Apply.Map[MultiValue[V]] =
            validateKey(key) {
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
            validateKeyValue(key, value) {
              (dataKey, userValue) =>
                function
                  .apply(dataKey, userValue, deadline)
                  .map(value => MultiValue.Their(value))
            }
        }
    }
  }

}
