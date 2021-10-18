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

package swaydb.api

import swaydb.IOValues._
import swaydb._
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.testkit.RunThis._

class SwayDBReverse_Persistent_Spec extends SwayDBReverseSpec {
  implicit val order: KeyOrder[Slice[Byte]] = KeyOrder.reverseLexicographic

  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](dir = randomDir).right.value.sweep(_.delete().get)
}

class SwayDBReverse_Memory_Spec extends SwayDBReverseSpec {
  implicit val order: KeyOrder[Slice[Byte]] = KeyOrder.reverseLexicographic

  val keyValueCount: Int = 100000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value.sweep(_.delete().get)
}

sealed trait SwayDBReverseSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO]

  implicit val bag = Bag.apiIO

  "Do reverse ordering" in {
    runThis(times = repeatTest, log = true) {
      TestCaseSweeper {
        implicit sweeper =>

          val db = newDB()

          (1 to keyValueCount) foreach {
            i =>
              db.put(i, i.toString).right.value
          }

          db
            .keys
            .foldLeft(keyValueCount + 1) {
              case (expected, actual) =>
                actual shouldBe expected - 1
                actual
            }
      }
    }
  }
}
