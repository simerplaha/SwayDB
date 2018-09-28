/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package embedded

import swaydb.SwayDB
import swaydb.core.TestBase
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.types.SwayDBMap

class SwayDBReverse_Persistent_Spec extends SwayDBReverseSpec {
  implicit val order: Ordering[Slice[Byte]] = KeyOrder.reverse

  val keyValueCount: Int = 10000

  override def newDB(): SwayDBMap[Int, String] =
    SwayDB.persistent[Int, String](dir = randomDir).assertGet
}

class SwayDBReverse_Memory_Spec extends SwayDBReverseSpec {
  implicit val order: Ordering[Slice[Byte]] = KeyOrder.reverse

  val keyValueCount: Int = 100000

  override def newDB(): SwayDBMap[Int, String] =
    SwayDB.memory[Int, String]().assertGet
}

sealed trait SwayDBReverseSpec extends TestBase with TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): SwayDBMap[Int, String]

  "Do reverse ordering" in {
    val db = newDB()

    (1 to keyValueCount) foreach {
      i =>
        db.put(i, i.toString).assertGet
    }

    db.keys.foldLeft(keyValueCount + 1) {
      case (expected, actual) =>
        actual shouldBe expected - 1
        actual
    }
  }
}