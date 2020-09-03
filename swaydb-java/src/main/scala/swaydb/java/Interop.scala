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

package swaydb.java

import swaydb.{Apply, Bag}

import scala.reflect.ClassTag

object Interop {

  /**
   * Experimental function that Converts a Scala [[swaydb.Map]] into [[swaydb.java.Map]].
   *
   * When working with Java and Scala both in the same application invoke [[swaydb.java.Map.asScala]] to access the
   * map is Scala. Converting from Scala to Java is not recommended since Java implementation is
   * dependant on Scala implementation and not the other way around (One way - Java -> Scala).
   */
  private class InteropImplicit[K, V, F, BAG[_]](map: swaydb.Map[K, V, F, BAG]) {
    @inline final def asJava(implicit classTag: ClassTag[F]): Map[K, V, swaydb.java.PureFunction[K, V, Return.Map[V]]] = {
      val scalaMap: swaydb.Map[K, V, F, Bag.Less] = map.toBag[Bag.Less]
      if (classTag == ClassTag.Nothing)
        Map[K, V, Void](scalaMap).asInstanceOf[Map[K, V, swaydb.java.PureFunction[K, V, Return.Map[V]]]]
      else
        Map[K, V, swaydb.java.PureFunction[K, V, Return.Map[V]]](scalaMap)
    }
  }

  /**
   * Converts a java Map function to Scala.
   */
  implicit class MapInterop[K, V, R <: Return.Map[V]](function: PureFunction[K, V, R]) {
    @inline final def asScala: swaydb.PureFunction[K, V, Apply.Map[V]] =
      PureFunction asScala function
  }

  /**
   * Converts java Set function to Scala.
   */
  implicit class SetInterop[K, R <: Return.Set[Void]](function: PureFunction.OnKey[K, Void, R]) {
    @inline final def asScala: swaydb.PureFunction.OnKey[K, Nothing, Apply.Set] =
      PureFunction asScala function
  }
}
