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

package swaydb.api

import swaydb.Bag
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import org.scalatest.OptionValues._
import swaydb.core.RunThis._
import scala.concurrent.duration._

object SwayDBPartialSetSpec {

  import boopickle.Default._

  implicit object Serialiser extends swaydb.serializers.Serializer[(Int, Option[String])] {
    override def write(data: (Int, Option[String])): Slice[Byte] =
      Slice(Pickle.intoBytes(data).array())

    override def read(data: Slice[Byte]): (Int, Option[String]) =
      Unpickle[(Int, Option[String])].fromBytes(data.toByteBufferWrap)
  }

  implicit val ordering =
    Right(
      new KeyOrder[(Int, Option[String])] {
        override def compare(x: (Int, Option[String]), y: (Int, Option[String])): Int =
          x._1 compare y._1

        private[swaydb] override def comparableKey(data: (Int, Option[String])): (Int, Option[String]) =
          (data._1, None)
      }
    )
}

class SwayDBPartialSet_Persistent_Spec extends SwayDBPartialSetSpec {

  import SwayDBPartialSetSpec._

  override def newDB(): swaydb.Set[(Int, Option[String]), Nothing, Bag.Less] =
    swaydb.persistent.Set[(Int, Option[String]), Nothing, Bag.Less](randomDir, mapSize = 10.bytes).get
}

class SwayDBPartialSet_Memory_Spec extends SwayDBPartialSetSpec {

  import SwayDBPartialSetSpec._

  override def newDB(): swaydb.Set[(Int, Option[String]), Nothing, Bag.Less] =
    swaydb.memory.Set[(Int, Option[String]), Nothing, Bag.Less](mapSize = 10.bytes).get
}

trait SwayDBPartialSetSpec extends TestBaseEmbedded {

  val keyValueCount = 1000

  def newDB(): swaydb.Set[(Int, Option[String]), Nothing, Bag.Less]

  "read partially ordered key-values" in {

    val set = newDB()

    val keyValues =
      (1 to 1000) map {
        i =>
          val keyValues = (i, Some(s"value $i"))
          set.add(keyValues)
          keyValues
      }

    def assertReads() = {
      (1 to 1000) foreach {
        i =>
          set.get((i, None)).value shouldBe ((i, Some(s"value $i")))
      }

      set.stream.materialize[Bag.Less].toList shouldBe keyValues
      set.reverse.stream.materialize[Bag.Less].toList shouldBe keyValues.reverse
    }

    assertReads()
    sleep(5.seconds)
    assertReads()
  }
}
