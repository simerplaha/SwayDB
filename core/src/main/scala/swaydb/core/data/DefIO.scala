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

  @inline def withInput[I2](input: I2): DefIO[I2, O] =
    new DefIO[I2, O](
      input = input,
      output = output
    )

  @inline def withOutput[O2](output: O2): DefIO[I, O2] =
    new DefIO[I, O2](
      input = input,
      output = output
    )
}
