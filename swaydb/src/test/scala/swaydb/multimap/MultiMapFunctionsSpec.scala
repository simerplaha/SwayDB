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
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.{Apply, Bag, MultiMap, Prepare, PureFunction}

import scala.concurrent.duration._

class MultiMapFunctionsSpec0 extends MultiMapFunctionsSpec {
  override def newDB()(implicit functions: swaydb.MultiMap.Functions[Int, Int, String, PureFunction[Int, String, Apply.Map[String]]],
                       sweeper: TestCaseSweeper) =
    swaydb.persistent.MultiMap[Int, Int, String, PureFunction[Int, String, Apply.Map[String]], Bag.Less](dir = randomDir).sweep()
}

class MultiMapFunctionsSpec1 extends MultiMapFunctionsSpec {
  override def newDB()(implicit functions: swaydb.MultiMap.Functions[Int, Int, String, PureFunction[Int, String, Apply.Map[String]]],
                       sweeper: TestCaseSweeper) =
    swaydb.persistent.MultiMap[Int, Int, String, PureFunction[Int, String, Apply.Map[String]], Bag.Less](dir = randomDir, mapSize = 1.byte).sweep()
}

class MultiMapFunctionsSpec2 extends MultiMapFunctionsSpec {
  override def newDB()(implicit functions: swaydb.MultiMap.Functions[Int, Int, String, PureFunction[Int, String, Apply.Map[String]]],
                       sweeper: TestCaseSweeper) =
    swaydb.memory.MultiMap[Int, Int, String, PureFunction[Int, String, Apply.Map[String]], Bag.Less]().sweep()
}

class MultiMapFunctionsSpec3 extends MultiMapFunctionsSpec {
  override def newDB()(implicit functions: swaydb.MultiMap.Functions[Int, Int, String, PureFunction[Int, String, Apply.Map[String]]],
                       sweeper: TestCaseSweeper) =
    swaydb.memory.MultiMap[Int, Int, String, PureFunction[Int, String, Apply.Map[String]], Bag.Less](mapSize = 1.byte).sweep()
}

sealed trait MultiMapFunctionsSpec extends TestBaseEmbedded {

  val keyValueCount: Int = 30

  def newDB()(implicit functions: swaydb.MultiMap.Functions[Int, Int, String, PureFunction[Int, String, Apply.Map[String]]],
              sweeper: TestCaseSweeper): MultiMap[Int, Int, String, PureFunction[Int, String, Apply.Map[String]], Bag.Less]

  implicit val bag = Bag.less

  //  implicit val mapKeySerializer = Key.serializer(IntSerializer)
  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default


  "apply and register function" when {
    //register all types of functions
    val onKeyValueFunction =
      new PureFunction.OnKeyValue[Int, String, Apply.Map[String]] {
        override def apply(key: Int, value: String, deadline: Option[Deadline]): Apply.Map[String] =
          Apply.Update("updated1")
      }

    val onValueFunction =
      new PureFunction.OnValue[String, Apply.Map[String]] {
        override def apply(value: String): Apply.Map[String] =
          Apply.Update("updated2")
      }

    val onKeyFunction =
      new PureFunction.OnKey[Int, String, Apply.Map[String]] {
        override def apply(key: Int, deadline: Option[Deadline]): Apply.Map[String] =
          Apply.Update("updated3")
      }

    //register all types of functions
    implicit val functions = swaydb.MultiMap.Functions[Int, Int, String, PureFunction[Int, String, Apply.Map[String]]](onKeyValueFunction, onValueFunction, onKeyFunction)

    "single" in {
      TestCaseSweeper {
        implicit sweeper =>

          val map = newDB()

          (1 to 30).foreach(i => map.put(i, i.toString))
          (1 to 30).foreach(i => map.get(i).value shouldBe i.toString)

          //apply functions individually
          map.applyFunction(1, 10, onKeyValueFunction)
          map.applyFunction(11, 19, onValueFunction)
          map.applyFunction(20, onValueFunction)
          map.applyFunction(21, 29, onKeyFunction)
          map.applyFunction(30, onKeyFunction)

          //assert
          (1 to 10).foreach(i => map.get(i).value shouldBe "updated1")
          (11 to 20).foreach(i => map.get(i).value shouldBe "updated2")
          (21 to 30).foreach(i => map.get(i).value shouldBe "updated3")
      }
    }

    "batch" in {
      TestCaseSweeper {
        implicit sweeper =>

          val map = newDB()

          (1 to 30).foreach(i => map.put(i, i.toString))
          (1 to 30).foreach(i => map.get(i).value shouldBe i.toString)

          //apply functions as batch
          map.commit(
            Prepare.ApplyFunction(1, 9, onKeyValueFunction),
            Prepare.ApplyFunction(10, onKeyValueFunction), //non range commit
            Prepare.ApplyFunction(11, 20, onValueFunction),
            Prepare.ApplyFunction(21, 29, onKeyFunction),
            Prepare.ApplyFunction(30, onKeyFunction) //non range
          )

          (1 to 10).foreach(i => map.get(i).value shouldBe "updated1")
          (11 to 20).foreach(i => map.get(i).value shouldBe "updated2")
          (21 to 30).foreach(i => map.get(i).value shouldBe "updated3")
      }
    }
  }
}
