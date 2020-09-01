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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.api.multimap

import org.scalatest.OptionValues._
import swaydb.Bag
import swaydb.api.TestBaseEmbedded
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

class NestedOptionValueSpec extends TestBaseEmbedded {
  override val keyValueCount: Int = 1000
  implicit val bag = Bag.less

  "Option[Option[V]]" in {
    TestCaseSweeper {
      implicit sweeper =>

        import swaydb.serializers.Default._

        val root = swaydb.memory.MultiMap_Experimental[Int, Int, Option[String], Nothing, Bag.Less]().sweep()

        root.put(1, None)
        root.contains(1) shouldBe true
        root.get(1).value shouldBe None

        root.put(2, None)
        root.contains(2) shouldBe true
        root.get(2).value shouldBe None

        root.stream.materialize.toList should contain only((1, None), (2, None))
    }
  }

  "Option[Empty[V]]" in {
    TestCaseSweeper {
      implicit sweeper =>

        sealed trait Value
        object Value {
          case class NonEmpty(string: String) extends Value
          case object Empty extends Value
        }

        import swaydb.serializers.Default._

        implicit object OptionOptionStringSerializer extends Serializer[Option[Value]] {
          override def write(data: Option[Value]): Slice[Byte] =
            data match {
              case Some(value) =>
                value match {
                  case Value.NonEmpty(string) =>
                    val stringBytes = StringSerializer.write(string)
                    val slice = Slice.create[Byte](stringBytes.size + 1)

                    slice add 1
                    slice addAll stringBytes

                  case Value.Empty =>
                    Slice(0.toByte)
                }

              case None =>
                Slice.emptyBytes
            }

          override def read(data: Slice[Byte]): Option[Value] =
            if (data.isEmpty)
              None
            else if (data.head == 0)
              Some(Value.Empty)
            else
              Some(Value.NonEmpty(StringSerializer.read(data.dropHead())))
        }

        val root = swaydb.memory.MultiMap_Experimental[Int, Int, Option[Value], Nothing, Bag.Less]().sweep()

        root.put(1, Some(Value.Empty))
        root.put(2, Some(Value.NonEmpty("two")))
        root.put(3, None)

        root.getKeyValue(1).value shouldBe(1, Some(Value.Empty))
        root.getKeyValue(3).value shouldBe(3, None)

        root.getKeyDeadline(1).value shouldBe(1, None)
        root.getKeyDeadline(2).value shouldBe(2, None)
        root.getKeyDeadline(3).value shouldBe(3, None)

        root.get(1).value.value shouldBe Value.Empty
        root.get(3).value shouldBe None
        root.get(2).value.value shouldBe Value.NonEmpty("two")

        root.stream.materialize[Bag.Less].toList shouldBe List((1, Some(Value.Empty)), (2, Some(Value.NonEmpty("two"))), (3, None))
    }
  }
}
