///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package swaydb.api.multimap
//
//import org.scalatest.OptionValues._
//import swaydb.api.TestBaseAPI
//import swaydb.core.TestSweeper
//import swaydb.core.TestSweeper._
//import swaydb.serializers.Serializer
//import swaydb.slice.Slice
//import swaydb.{Bag, Glass}
//
//class NestedOptionValueSpec extends TestBaseAPI {
//  override val keyValueCount: Int = 1000
//  implicit val bag = Bag.glass
//
//  "Option[Option[V]]" in {
//    TestSweeper {
//      implicit sweeper =>
//
//        import swaydb.serializers.Default._
//
//        val root = swaydb.memory.MultiMap[Int, Int, Option[String], Nothing, Glass]().sweep(_.delete())
//
//        root.put(1, None)
//        root.contains(1) shouldBe true
//        root.get(1).value shouldBe None
//
//        root.put(2, None)
//        root.contains(2) shouldBe true
//        root.get(2).value shouldBe None
//
//        root.materialize.toList should contain only((1, None), (2, None))
//    }
//  }
//
//  "Option[Empty[V]]" in {
//    TestSweeper {
//      implicit sweeper =>
//
//        sealed trait Value
//        object Value {
//          case class NonEmpty(string: String) extends Value
//          case object Empty extends Value
//        }
//
//        import swaydb.serializers.Default._
//
//        implicit object OptionOptionStringSerializer extends Serializer[Option[Value]] {
//          override def write(data: Option[Value]): Slice[Byte] =
//            data match {
//              case Some(value) =>
//                value match {
//                  case Value.NonEmpty(string) =>
//                    val stringBytes = StringSerializer.write(string)
//                    val slice = Slice.allocate[Byte](stringBytes.size + 1)
//
//                    slice add 1
//                    slice addAll stringBytes
//
//                  case Value.Empty =>
//                    Slice(0.toByte)
//                }
//
//              case None =>
//                Slice.emptyBytes
//            }
//
//          override def read(slice: Slice[Byte]): Option[Value] =
//            if (slice.isEmpty)
//              None
//            else if (slice.head == 0)
//              Some(Value.Empty)
//            else
//              Some(Value.NonEmpty(StringSerializer.read(slice.dropHead())))
//        }
//
//        val root = swaydb.memory.MultiMap[Int, Int, Option[Value], Nothing, Glass]().sweep(_.delete())
//
//        root.put(1, Some(Value.Empty))
//        root.put(2, Some(Value.NonEmpty("two")))
//        root.put(3, None)
//
//        root.getKeyValue(1).value shouldBe(1, Some(Value.Empty))
//        root.getKeyValue(3).value shouldBe(3, None)
//
//        root.getKeyDeadline(1).value shouldBe(1, None)
//        root.getKeyDeadline(2).value shouldBe(2, None)
//        root.getKeyDeadline(3).value shouldBe(3, None)
//
//        root.get(1).value.value shouldBe Value.Empty
//        root.get(3).value shouldBe None
//        root.get(2).value.value shouldBe Value.NonEmpty("two")
//
//        root.materialize.toList shouldBe List((1, Some(Value.Empty)), (2, Some(Value.NonEmpty("two"))), (3, None))
//    }
//  }
//}
