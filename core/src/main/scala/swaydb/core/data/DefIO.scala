/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

object DefIO {

  @inline def apply[I, O](input: I,
                          output: O): DefIO[I, O] =
    new DefIO[I, O](
      input = input,
      output = output
    )

}

/**
 * Stores the input and output of a function.
 */
class DefIO[+I, +O](val input: I,
                    val output: O) {

  @inline def map[B](f: O => B): DefIO[I, B] =
    new DefIO[I, B](
      input = input,
      output = f(output)
    )

  @inline def copyInput[I2](input: I2): DefIO[I2, O] =
    new DefIO[I2, O](
      input = input,
      output = output
    )

  @inline def copyOutput[O2](output: O2): DefIO[I, O2] =
    new DefIO[I, O2](
      input = input,
      output = output
    )
}
