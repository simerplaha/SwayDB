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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.data

object MergeResult {

  @inline def apply[S, A](source: S,
                          result: A): MergeResult[S, A] =
    new MergeResult[S, A](
      source = source,
      result = result
    )

}

class MergeResult[+S, +A](val source: S,
                          val result: A) {
  @inline def map[B](f: A => B): MergeResult[S, B] =
    new MergeResult[S, B](
      source = source,
      result = f(result)
    )

  @inline def updateSource[S2](source: S2): MergeResult[S2, A] =
    new MergeResult[S2, A](
      source = source,
      result = result
    )

  @inline def updateResult[B](result: B): MergeResult[S, B] =
    new MergeResult[S, B](
      source = source,
      result = result
    )
}
