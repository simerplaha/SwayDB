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

package swaydb.java
import swaydb.utils.StorageUnits._

object StorageUnits {

  @inline final def bytes(measure: Int): Int =
    measure

  @inline final def byte(measure: Int): Int =
    measure

  @inline final def mb(measure: Double): Int =
    measure.mb

  @inline final def gb(measure: Double): Int =
    measure.gb

  @inline final def kb(measure: Double): Int =
    measure.kb

  @inline final def mb_long(measure: Double): Long =
    measure.mb_long

  @inline final def gb_long(measure: Double): Long =
    measure.gb_long

  @inline final def kb_long(measure: Double): Long =
    measure.kb_long
}
