/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.function

import swaydb.core.data.SwayFunctionOutput
import swaydb.core.function.FunctionStore
import swaydb.data.Functions
import swaydb.multimap.{MultiKey, MultiValue}
import swaydb.serializers.Serializer
import swaydb.slice.{Slice, SliceOption}
import swaydb.{Apply, Expiration, MultiMap, PureFunction, PureFunctionJava, PureFunctionScala}

import java.util.Optional
import scala.concurrent.duration.Deadline

private[swaydb] case object FunctionConverter {

  def toCore[K, V, R <: Apply[V], F <: PureFunction[K, V, R]](function: F)(implicit keySerializer: Serializer[K],
                                                                           valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction =


    function match {
      case function: PureFunctionScala.OnEntry[K] =>
        toCoreFunctionKey[K, V](function)

      case function: PureFunctionScala.OnEntryDeadline[K] =>
        toCoreFunctionKeyDeadline[K, V](function)

      case function: PureFunctionScala.OnKey[K, V] =>
        toCoreFunctionKey[K, V](function)

      case function: PureFunctionScala.OnKeyDeadline[K, V] =>
        toCoreFunctionKeyDeadline[K, V](function)

      case function: PureFunctionScala.OnKeyValue[K, V] =>
        toCoreFunctionKeyValue[K, V](function)

      case function: PureFunctionScala.OnValue[V] =>
        toCoreFunctionValue[V](function)

      case function: PureFunctionScala.OnValueDeadline[V] =>
        toCoreFunctionValueDeadline[V](function)

      case function: PureFunctionScala.OnKeyValueDeadline[K, V] =>
        toCoreFunctionKeyValueDeadline[K, V](function)

      case function: PureFunctionJava.OnEntry[K] =>
        toCoreFunctionKey[K, V](
          key =>
            function.apply(key).asInstanceOf[Apply.Set[V]]
        )

      case function: PureFunctionJava.OnEntryExpiration[K] =>
        toCoreFunctionKeyExpiration[K, V](
          (key, expiration) =>
            function.apply(key, expiration).asInstanceOf[Apply.Set[V]]
        )

      case function: PureFunctionJava.OnKey[K, V] =>
        toCoreFunctionKey[K, V](function)

      case function: PureFunctionJava.OnKeyExpiration[K, V] =>
        toCoreFunctionKeyExpiration[K, V](function)

      case function: PureFunctionJava.OnValue[K, V] =>
        toCoreFunctionValue[V](function)

      case function: PureFunctionJava.OnValueExpiration[K, V] =>
        toCoreFunctionValueExpiration[V](function)

      case function: PureFunctionJava.OnKeyValue[K, V] =>
        toCoreFunctionKeyValue[K, V](function)

      case function: PureFunctionJava.OnKeyValueExpiration[K, V] =>
        toCoreFunctionKeyValueExpiration[K, V](function)
    }

  private def toCoreFunctionOutput[V](output: Apply[V])(implicit valueSerializer: Serializer[V]): SwayFunctionOutput =
    output match {
      case Apply.Nothing =>
        SwayFunctionOutput.Nothing

      case Apply.Remove =>
        SwayFunctionOutput.Remove

      case Apply.Expire(deadline) =>
        SwayFunctionOutput.Expire(deadline)

      case update: Apply.Update[V] =>
        val untypedValue: Slice[Byte] = valueSerializer.write(update.value)
        SwayFunctionOutput.Update(untypedValue, update.deadline)
    }

  private def toCoreFunctionKey[K, V](f: K => Apply[V])(implicit keySerializer: Serializer[K],
                                                        valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction = {
    import swaydb.serializers._

    def function(key: Slice[Byte]) =
      toCoreFunctionOutput(f(key.read[K]))

    swaydb.core.data.SwayFunction.Key(function)
  }

  private def toCoreFunctionKeyDeadline[K, V](f: (K, Option[Deadline]) => Apply[V])(implicit keySerializer: Serializer[K],
                                                                                    valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction = {
    import swaydb.serializers._

    def function(key: Slice[Byte], deadline: Option[Deadline]) =
      toCoreFunctionOutput(f(key.read[K], deadline))

    swaydb.core.data.SwayFunction.KeyDeadline(function)
  }

  private def toCoreFunctionKeyExpiration[K, V](f: (K, Optional[Expiration]) => Apply[V])(implicit keySerializer: Serializer[K],
                                                                                          valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction = {
    import swaydb.serializers._

    def function(key: Slice[Byte], deadline: Option[Deadline]) =
      toCoreFunctionOutput(f(key.read[K], Expiration(deadline)))

    swaydb.core.data.SwayFunction.KeyDeadline(function)
  }

  private def toCoreFunctionKeyValueExpiration[K, V](f: (K, V, Optional[Expiration]) => Apply[V])(implicit keySerializer: Serializer[K],
                                                                                                  valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction = {
    import swaydb.serializers._

    def function(key: Slice[Byte], value: SliceOption[Byte], deadline: Option[Deadline]) =
      toCoreFunctionOutput(f(key.read[K], value.read[V], Expiration(deadline)))

    swaydb.core.data.SwayFunction.KeyValueDeadline(function)
  }

  private def toCoreFunctionKeyValueDeadline[K, V](f: (K, V, Option[Deadline]) => Apply[V])(implicit keySerializer: Serializer[K],
                                                                                            valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction = {
    import swaydb.serializers._

    def function(key: Slice[Byte], value: SliceOption[Byte], deadline: Option[Deadline]) =
      toCoreFunctionOutput(f(key.read[K], value.read[V], deadline))

    swaydb.core.data.SwayFunction.KeyValueDeadline(function)
  }

  private def toCoreFunctionKeyValue[K, V](f: (K, V) => Apply[V])(implicit keySerializer: Serializer[K],
                                                                  valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction = {
    import swaydb.serializers._

    def function(key: Slice[Byte], value: SliceOption[Byte]) =
      toCoreFunctionOutput(f(key.read[K], value.read[V]))

    swaydb.core.data.SwayFunction.KeyValue(function)
  }

  private def toCoreFunctionValue[V](f: V => Apply[V])(implicit valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction = {
    import swaydb.serializers._

    def function(value: SliceOption[Byte]) =
      toCoreFunctionOutput(f(value.read[V]))

    swaydb.core.data.SwayFunction.Value(function)
  }

  private def toCoreFunctionValueExpiration[V](f: (V, Optional[Expiration]) => Apply[V])(implicit valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction = {
    import swaydb.serializers._

    def function(value: SliceOption[Byte], deadline: Option[Deadline]) =
      toCoreFunctionOutput(f(value.read[V], Expiration(deadline)))

    swaydb.core.data.SwayFunction.ValueDeadline(function)
  }

  private def toCoreFunctionValueDeadline[V](f: (V, Option[Deadline]) => Apply[V])(implicit valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction = {
    import swaydb.serializers._

    def function(value: SliceOption[Byte], deadline: Option[Deadline]) =
      toCoreFunctionOutput(f(value.read[V], deadline))

    swaydb.core.data.SwayFunction.ValueDeadline(function)
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
          throw new Exception(s"LogEntry expected but got ${entry.getClass.getName}")
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
