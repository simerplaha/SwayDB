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

package swaydb.multimap

import org.scalatest.OptionValues._
import swaydb.api.TestBaseEmbedded
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.serializers.Default._
import swaydb.{Bag, MultiMap}

class MultiMapPutSpec0 extends MultiMapPutSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): swaydb.MultiMap[Int, Int, String, Nothing, Bag.Less] =
    swaydb.persistent.MultiMap[Int, Int, String, Nothing, Bag.Less](dir = randomDir).sweep()
}

class MultiMapPutSpec1 extends MultiMapPutSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): swaydb.MultiMap[Int, Int, String, Nothing, Bag.Less] =
    swaydb.memory.MultiMap[Int, Int, String, Nothing, Bag.Less]().sweep()
}

sealed trait MultiMapPutSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB()(implicit sweeper: TestCaseSweeper): swaydb.MultiMap[Int, Int, String, Nothing, Bag.Less]

  "Root" should {
    "Initialise a RootMap & SubMap from Root" in {
      TestCaseSweeper {
        implicit sweeper =>

          val root = newDB()

          var child1 = root.schema.init(1)
          var child2 = root.schema.init(2)

          if (randomBoolean()) child1 = root.schema.get(1).value
          if (randomBoolean()) child2 = root.schema.get(2).value

          child1.put(3, "three")
          child1.put(4, "four")
          child2.put(5, "five")
          child2.put(4, "four again")

          child1
            .stream
            .materialize[Bag.Less]
            .toList should contain inOrderOnly((3, "three"), (4, "four"))

          child2
            .stream
            .materialize[Bag.Less]
            .toList should contain inOrderOnly((4, "four again"), (5, "five"))
      }
    }

    "Initialise a RootMap & 2 SubMaps from Root" in {
      TestCaseSweeper {
        implicit sweeper =>

          val root = newDB()

          def insert(firstMap: MultiMap[Int, Int, String, Nothing, Bag.Less]) = {
            firstMap.put(3, "three")
            firstMap.put(4, "four")
            firstMap.put(5, "five")
            firstMap.put(4, "four again")
          }

          val child1 = root.schema.init(1)

          val child2 = child1.schema.init(2)
          insert(child2)

          val child3 = child1.schema.init(3)
          insert(child3)

          child1.isEmpty shouldBe true

          child2
            .stream
            .materialize[Bag.Less]
            .toList should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))

          child3
            .stream
            .materialize[Bag.Less]
            .toList should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
      }
    }

    "Initialise 2 RootMaps & 2 SubMaps under each SubMap" in {
      TestCaseSweeper {
        implicit sweeper =>

          val root = newDB()

          var root1 = root.schema.init(1)
          var root2 = root.schema.init(2)
          if (randomBoolean()) root1 = root.schema.get(1).value
          if (randomBoolean()) root2 = root.schema.get(2).value

          var sub11 = root1.schema.init(1)
          var sub12 = root1.schema.init(2)
          if (randomBoolean()) sub11 = root1.schema.get(1).value
          if (randomBoolean()) sub12 = root1.schema.get(2).value
          sub11.put(1, "one")
          sub12.put(2, "two")
          if (randomBoolean()) sub11 = root1.schema.get(1).value
          if (randomBoolean()) sub12 = root1.schema.get(2).value

          var sub21 = root2.schema.init(1)
          var sub22 = root2.schema.init(2)
          if (randomBoolean()) sub21 = root2.schema.get(1).value
          if (randomBoolean()) sub22 = root2.schema.get(2).value
          sub21.put(1, "1")
          sub22.put(2, "2")
          if (randomBoolean()) sub21 = root2.schema.get(1).value
          if (randomBoolean()) sub22 = root2.schema.get(2).value

          sub11.get(1).value shouldBe "one"
          sub12.get(2).value shouldBe "two"

          sub21.get(1).value shouldBe "1"
          sub22.get(2).value shouldBe "2"

          root.schema.keys.materialize[Bag.Less].toList should have size 2

          val rootSubMaps = root.schema.stream.materialize[Bag.Less].toList.flatten
          rootSubMaps.foreach(_.isEmpty shouldBe true) //has no map entries

          val subMaps = rootSubMaps.flatMap(_.schema.stream.materialize[Bag.Less].toList.flatten)
          subMaps should have size 4

          subMaps(0).get(1).value shouldBe "one"
          subMaps(1).get(2).value shouldBe "two"
          subMaps(2).get(1).value shouldBe "1"
          subMaps(3).get(2).value shouldBe "2"
      }
    }
  }
}
