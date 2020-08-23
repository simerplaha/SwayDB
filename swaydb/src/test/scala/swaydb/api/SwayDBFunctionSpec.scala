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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.api

import java.util.concurrent.TimeUnit

import swaydb.IOValues._
import swaydb.core.TestCaseSweeper._
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.data.RunThis._
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.{Apply, IO, Map, Prepare, StorageIntImplicits}

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration.Deadline

protected sealed trait Key
protected object Key {

  import boopickle.Default._

  case class Id(id: Int) extends Key
  sealed trait Function extends Key

  case object IncrementValue extends Key.Function with swaydb.PureFunction.OnValue[Int, Apply.Map[Int]] {
    override def apply(value: Int): Apply.Map[Int] =
      Apply.Update[Int](value + 1)

    override def id: String =
      "1"
  }

  case object DoNothing extends Key.Function with swaydb.PureFunction.OnValue[Int, Apply.Map[Int]] {
    override def apply(value: Int): Apply.Map[Int] =
      Apply.Nothing

    override def id: String =
      "2"
  }

  implicit val deadlinePickler = transformPickler((nano: Long) => Deadline((nano, TimeUnit.NANOSECONDS)))(_.time.toNanos)

  implicit object KeySerializer extends swaydb.serializers.Serializer[Key] {
    override def write(data: Key): Slice[Byte] =
      Slice(Pickle.intoBytes(data).array())

    override def read(data: Slice[Byte]): Key =
      Unpickle[Key].fromBytes(data.toByteBufferWrap)
  }
}

class SwayDBFunctionSpec0 extends SwayDBFunctionSpec {

  override def newDB()(implicit functionStore: Map.Functions[Key, Int, Key.Function],
                       sweeper: TestCaseSweeper): Map[Key, Int, Key.Function, IO.ApiIO] =
    swaydb.persistent.Map[Key, Int, Key.Function, IO.ApiIO](randomDir).right.value.sweep()
}

class SwayDBFunctionSpec1 extends SwayDBFunctionSpec {

  override def newDB()(implicit functionStore: Map.Functions[Key, Int, Key.Function],
                       sweeper: TestCaseSweeper): Map[Key, Int, Key.Function, IO.ApiIO] =
    swaydb.persistent.Map[Key, Int, Key.Function, IO.ApiIO](randomDir, mapSize = 1.byte).right.value.sweep()
}

class SwayDBFunctionSpec2 extends SwayDBFunctionSpec {

  override def newDB()(implicit functionStore: Map.Functions[Key, Int, Key.Function],
                       sweeper: TestCaseSweeper): Map[Key, Int, Key.Function, IO.ApiIO] =
    swaydb.memory.Map[Key, Int, Key.Function, IO.ApiIO](mapSize = 1.byte).right.value.sweep()
}

class SwayDBFunctionSpec3 extends SwayDBFunctionSpec {
  override def newDB()(implicit functionStore: Map.Functions[Key, Int, Key.Function],
                       sweeper: TestCaseSweeper): Map[Key, Int, Key.Function, IO.ApiIO] =
    swaydb.memory.Map[Key, Int, Key.Function, IO.ApiIO]().right.value.sweep()
}

//class SwayDBFunctionSpec4 extends SwayDBFunctionSpec {
//
//  override def newDB(): Map[Key, Int, Key.Function, IO.ApiIO] =
//    swaydb.memory.zero.Map[Key, Int, Key.Function, IO.ApiIO](mapSize = 1.byte).right.value
//}
//
//class SwayDBFunctionSpec5 extends SwayDBFunctionSpec {
//  override def newDB(): Map[Key, Int, Key.Function, IO.ApiIO] =
//    swaydb.memory.zero.Map[Key, Int, Key.Function, IO.ApiIO]().right.value
//}

sealed trait SwayDBFunctionSpec extends TestBase {

  def newDB()(implicit functionStore: swaydb.Map.Functions[Key, Int, Key.Function],
              sweeper: TestCaseSweeper): swaydb.Map[Key, Int, Key.Function, IO.ApiIO]


  "SwayDB" should {
    implicit val functionsMap = swaydb.Map.Functions[Key, Int, Key.Function]()
    functionsMap.register(Key.IncrementValue, Key.DoNothing)

    "perform concurrent atomic updates to a single key" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            db.put(Key.Id(1), 0).get

            (1 to 1000).par foreach {
              _ =>
                db.applyFunction(Key.Id(1), Key.IncrementValue).get
            }

            db.get(Key.Id(1)).get should contain(1000)
        }
      }
    }

    "perform concurrent atomic updates to multiple keys" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            (1 to 1000) foreach {
              i =>
                db.put(Key.Id(i), 0).get
            }

            (1 to 100).par foreach {
              _ =>
                (1 to 1000).par foreach {
                  i =>
                    db.applyFunction(Key.Id(i), Key.IncrementValue).get
                }
            }

            (1 to 1000).par foreach {
              i =>
                db.get(Key.Id(i)).get should contain(100)
            }

        }
      }
    }

    "batch commit updates" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            val puts: List[Prepare[Key.Id, Int, Nothing]] =
              (1 to 1000).map(key => Prepare.Put(Key.Id(key), key)).toList

            db.commit(puts).get

            val prepareApplyFunction: List[Prepare[Key.Id, Nothing, Key.IncrementValue.type]] =
              (1 to 1000).map(key => Prepare.ApplyFunction(Key.Id(key), Key.IncrementValue)).toList

            db.commit(prepareApplyFunction).get

            (1 to 1000) foreach {
              key =>
                db.get(Key.Id(key)).get should contain(key + 1)
            }

        }
      }
    }

    "Nothing should not update data" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            (1 to 1000) foreach {
              i =>
                db.put(Key.Id(i), 0).get
            }

            (1 to 100).par foreach {
              _ =>
                (1 to 1000).par foreach {
                  i =>
                    db.applyFunction(Key.Id(i), Key.DoNothing).get
                }
            }

            (1 to 1000).par foreach {
              i =>
                db.get(Key.Id(i)).get should contain(0)
            }
        }
      }
    }
  }
}
