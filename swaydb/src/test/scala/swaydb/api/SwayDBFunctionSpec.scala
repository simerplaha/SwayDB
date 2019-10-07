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

import java.util.concurrent.TimeUnit

import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.data.slice.Slice
import swaydb.serializers.Default._

import scala.concurrent.duration.Deadline

protected sealed trait Key
protected object Key {

  import boopickle.Default._

  case class Id(id: Int) extends Key
  sealed trait Function extends Key

  case object IncrementValue extends Key.Function with swaydb.Function.GetValue[Int] {
    override def apply(value: Int): Apply.Map[Int] =
      Apply.Update[Int](value + 1)

    override def id: Slice[Byte] =
      Slice.writeInt(1)
  }

  case object DoNothing extends Key.Function with swaydb.Function.GetValue[Int] {
    override def apply(value: Int): Apply.Map[Int] =
      Apply.Nothing

    override def id: Slice[Byte] =
      Slice.writeInt(2)
  }

  implicit val deadlinePickler = transformPickler((nano: Long) => Deadline(nano, TimeUnit.NANOSECONDS))(_.time.toNanos)

  implicit object KeySerializer extends swaydb.serializers.Serializer[Key] {
    override def write(data: Key): Slice[Byte] =
      Slice(Pickle.intoBytes(data).array())

    override def read(data: Slice[Byte]): Key =
      Unpickle[Key].fromBytes(data.toByteBufferWrap)
  }
}

class SwayDBFunctionSpec0 extends SwayDBFunctionSpec {

  override def newDB(): Map[Key, Int, Key.Function, IO.ApiIO] =
    swaydb.persistent.Map[Key, Int, Key.Function](randomDir).right.value
}

class SwayDBFunctionSpec1 extends SwayDBFunctionSpec {

  override def newDB(): Map[Key, Int, Key.Function, IO.ApiIO] =
    swaydb.persistent.Map[Key, Int, Key.Function](randomDir, mapSize = 1.byte).right.value
}

class SwayDBFunctionSpec2 extends SwayDBFunctionSpec {

  override def newDB(): Map[Key, Int, Key.Function, IO.ApiIO] =
    swaydb.memory.Map[Key, Int, Key.Function](mapSize = 1.byte).right.value
}

class SwayDBFunctionSpec3 extends SwayDBFunctionSpec {
  override def newDB(): Map[Key, Int, Key.Function, IO.ApiIO] =
    swaydb.memory.Map[Key, Int, Key.Function]().right.value
}

class SwayDBFunctionSpec4 extends SwayDBFunctionSpec {

  override def newDB(): Map[Key, Int, Key.Function, IO.ApiIO] =
    swaydb.memory.zero.Map[Key, Int, Key.Function](mapSize = 1.byte).right.value
}

class SwayDBFunctionSpec5 extends SwayDBFunctionSpec {
  override def newDB(): Map[Key, Int, Key.Function, IO.ApiIO] =
    swaydb.memory.zero.Map[Key, Int, Key.Function]().right.value
}

sealed trait SwayDBFunctionSpec extends TestBase {

  def newDB(): Map[Key, Int, Key.Function, IO.ApiIO]

  "SwayDB" should {
    "perform concurrent atomic updates to a single key" in {

      val db = newDB()

      db.put(Key.Id(1), 0).get

      val incrementValue = db.registerFunction(Key.IncrementValue)

      (1 to 1000).par foreach {
        _ =>
          db.applyFunction(Key.Id(1), incrementValue).get
      }

      db.get(Key.Id(1)).get should contain(1000)

      db.close().get
    }

    "perform concurrent atomic updates to multiple keys" in {

      val db = newDB()

      (1 to 1000) foreach {
        i =>
          db.put(Key.Id(i), 0).get
      }

      val functionId = db.registerFunction(Key.IncrementValue)

      (1 to 100).par foreach {
        _ =>
          (1 to 1000).par foreach {
            i =>
              db.applyFunction(Key.Id(i), functionId).get
          }
      }

      (1 to 1000).par foreach {
        i =>
          db.get(Key.Id(i)).get should contain(100)
      }

      db.close().get
    }

    "Nothing should not update data" in {

      val db = newDB()

      (1 to 1000) foreach {
        i =>
          db.put(Key.Id(i), 0).get
      }

      val functionId = db.registerFunction(Key.DoNothing)

      (1 to 100).par foreach {
        _ =>
          (1 to 1000).par foreach {
            i =>
              db.applyFunction(Key.Id(i), functionId).get
          }
      }

      (1 to 1000).par foreach {
        i =>
          db.get(Key.Id(i)).get should contain(0)
      }

      db.close().get
    }
  }
}
