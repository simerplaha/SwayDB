/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.api.multimap

import org.scalatest.OptionValues._
import swaydb.PureFunctionScala._
import swaydb.api.TestBaseEmbedded
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.data.Functions
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.utils.StorageUnits._
import swaydb.{Apply, Bag, Glass, MultiMap, Prepare, PureFunction}

import scala.concurrent.duration._

class MultiMapFunctionsSpec0 extends MultiMapFunctionsSpec {
  override def newDB()(implicit functions: Functions[PureFunction.Map[Int, String]],
                       sweeper: TestCaseSweeper) =
    swaydb.persistent.MultiMap[Int, Int, String, PureFunction.Map[Int, String], Glass](dir = randomDir).sweep(_.delete())
}

class MultiMapFunctionsSpec1 extends MultiMapFunctionsSpec {
  override def newDB()(implicit functions: Functions[PureFunction.Map[Int, String]],
                       sweeper: TestCaseSweeper) =
    swaydb.persistent.MultiMap[Int, Int, String, PureFunction.Map[Int, String], Glass](dir = randomDir, mapSize = 1.byte).sweep(_.delete())
}

class MultiMapFunctionsSpec2 extends MultiMapFunctionsSpec {
  override def newDB()(implicit functions: Functions[PureFunction.Map[Int, String]],
                       sweeper: TestCaseSweeper) =
    swaydb.memory.MultiMap[Int, Int, String, PureFunction.Map[Int, String], Glass]().sweep(_.delete())
}

class MultiMapFunctionsSpec3 extends MultiMapFunctionsSpec {
  override def newDB()(implicit functions: Functions[PureFunction.Map[Int, String]],
                       sweeper: TestCaseSweeper) =
    swaydb.memory.MultiMap[Int, Int, String, PureFunction.Map[Int, String], Glass](mapSize = 1.byte).sweep(_.delete())
}

sealed trait MultiMapFunctionsSpec extends TestBaseEmbedded {

  val keyValueCount: Int = 30

  def newDB()(implicit functions: Functions[PureFunction.Map[Int, String]],
              sweeper: TestCaseSweeper): MultiMap[Int, Int, String, PureFunction.Map[Int, String], Glass]

  implicit val bag = Bag.glass

  //  implicit val mapKeySerializer = Key.serializer(IntSerializer)
  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default


  "apply and register function" when {
    //register all types of functions
    val onKeyValueFunction: OnKeyValueDeadline[Int, String] =
      (key: Int, value: String, deadline: Option[Deadline]) =>
        Apply.Update("updated1")

    val onValueFunction: OnKeyValue[Int, String] =
      (key: Int, value: String) =>
        Apply.Update("updated2")

    val onKeyFunction: OnKey[Int, String] =
      (key: Int) =>
        Apply.Update("updated3")

    //register all types of functions
    implicit val functions = Functions[PureFunction.Map[Int, String]](onKeyValueFunction, onValueFunction, onKeyFunction)

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
