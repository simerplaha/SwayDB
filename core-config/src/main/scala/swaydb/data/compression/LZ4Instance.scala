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

package swaydb.data.compression

import swaydb.macros.Sealed

import scala.util.Random

sealed trait LZ4Instance
object LZ4Instance {
  def fastestInstance: LZ4Instance = Fastest
  case object Fastest extends LZ4Instance

  def fastestJavaInstance: LZ4Instance = FastestJava
  case object FastestJava extends LZ4Instance

  def nativeInstance: LZ4Instance = Native
  case object Native extends LZ4Instance

  def safeInstance: LZ4Instance = Safe
  case object Safe extends LZ4Instance

  def unsafeInstance: LZ4Instance = Unsafe
  case object Unsafe extends LZ4Instance

  def instances(): List[LZ4Instance] =
    Sealed.list[LZ4Instance]

  def random(): LZ4Instance =
    Random.shuffle(instances()).head
}
