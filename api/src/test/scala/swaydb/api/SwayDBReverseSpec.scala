/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb

import swaydb.api.TestBaseEmbedded
import swaydb.core.IOValues._
import swaydb.core.RunThis._
import swaydb.data.IO
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._

class SwayDBReverse_Persistent_Spec extends SwayDBReverseSpec {
  implicit val order: KeyOrder[Slice[Byte]] = KeyOrder.reverse

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, IO] =
    swaydb.persistent.Map[Int, String](dir = randomDir).runIO
}

class SwayDBReverse_Persistent_Zero_Spec extends SwayDBReverseSpec {
  implicit val order: KeyOrder[Slice[Byte]] = KeyOrder.reverse

  val keyValueCount: Int = 10000

  override def newDB(): Map[Int, String, IO] =
    swaydb.persistent.zero.Map[Int, String](dir = randomDir).runIO
}

class SwayDBReverse_Memory_Spec extends SwayDBReverseSpec {
  implicit val order: KeyOrder[Slice[Byte]] = KeyOrder.reverse

  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String, IO] =
    swaydb.memory.Map[Int, String]().runIO
}

class SwayDBReverse_Memory_Zero_Spec extends SwayDBReverseSpec {
  implicit val order: KeyOrder[Slice[Byte]] = KeyOrder.reverse

  val keyValueCount: Int = 100000

  override def newDB(): Map[Int, String, IO] =
    swaydb.memory.zero.Map[Int, String]().runIO
}

sealed trait SwayDBReverseSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): Map[Int, String, IO]

  "Do reverse ordering" in {
    val db = newDB()

    (1 to keyValueCount) foreach {
      i =>
        db.put(i, i.toString).runIO
    }

    db.keys.foldLeft(keyValueCount + 1) {
      case (expected, actual) =>
        actual shouldBe expected - 1
        actual
    }

    db.close().get
  }
}
