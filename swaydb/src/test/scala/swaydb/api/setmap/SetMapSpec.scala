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
 */

package swaydb.api.setmap

import org.scalatest.OptionValues._
import swaydb.Bag
import swaydb.core.TestBase
import swaydb.serializers.Default._

class SetMapSpec0 extends SetMapSpec {
  override def newDB(): swaydb.SetMap[Int, String, Nothing, Bag.Less] =
    swaydb.persistent.SetMap[Int, String, Nothing, Bag.Less](randomDir).get
}

class SetMapSpec3 extends SetMapSpec {
  override def newDB(): swaydb.SetMap[Int, String, Nothing, Bag.Less] =
    swaydb.memory.SetMap[Int, String, Nothing, Bag.Less]().get
}

sealed trait SetMapSpec extends TestBase {

  def newDB(): swaydb.SetMap[Int, String, Nothing, Bag.Less]

  "put" in {
    val map = newDB()

    (1 to 1000000) foreach {
      i =>
        map.put(i, i.toString)
    }

    (1 to 1000000) foreach {
      i =>
        map.get(i).value shouldBe i.toString
    }

    map.close()
  }
}
