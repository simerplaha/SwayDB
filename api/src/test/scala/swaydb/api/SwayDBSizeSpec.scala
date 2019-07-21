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
import swaydb.serializers.Default._

class SwayDBSize_Persistent_Spec extends SwayDBSizeSpec {
  val keyValueCount: Int = 10000000

  override def newDB(): Map[Int, String, IO] =
    swaydb.persistent.Map[Int, String](dir = randomDir).runIO
}

class SwayDBSize_Memory_Spec extends SwayDBSizeSpec {
  val keyValueCount: Int = 10000000

  override def newDB(): Map[Int, String, IO] =
    swaydb.memory.Map[Int, String]().runIO
}

sealed trait SwayDBSizeSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  override def deleteFiles = false

  def newDB(): Map[Int, String, IO]

  "return the size of key-values" in {
    val db = newDB()

    (1 to keyValueCount) foreach {
      i =>
        db.put(i, i.toString).runIO
    }

    db.size shouldBe keyValueCount

    db.close().get
  }
}
