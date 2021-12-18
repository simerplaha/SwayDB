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
//package swaydb.core.level.seek
//
//import org.scalamock.scalatest.MockFactory
//import org.scalatest.OptionValues
//import org.scalatest.matchers.should.Matchers._
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.effect.IOValues._
//import swaydb.core.CoreTestData._
//import swaydb.core.level.LevelSeek
//import swaydb.core.segment.data.merge.FixedMerger
//import swaydb.core.segment.data.{KeyValue, Memory}
//import swaydb.core.{CoreTestData, TestTimer}
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.slice.Slice
//import swaydb.slice.order.{KeyOrder, TimeOrder}
//import swaydb.testkit.RunThis._
//import swaydb.testkit.TestKit._
//
//class HigherFixedSomeSpec extends AnyWordSpec with OptionValues {
//
//  implicit val keyOrder = KeyOrder.default
//  implicit val timeOrder = TimeOrder.long
//  implicit val functionStore = CoreTestData.functionStore
//
//  "return Some" when {
//    implicit val testTimer = TestTimer.Decremental()
//    import swaydb.Error.Level.ExceptionHandler
//
//    //   0
//    //     1
//    //     x
//    "1" in {
//      runThis(100.times) {
//
//        implicit val current = mock[CurrentWalker]
//        implicit val next = mock[NextWalker]
//
//        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(false))
//
//        inSequence {
//          //@formatter:off
//          current.higher        _ expects (0: Slice[Byte], *)  returning LevelSeek.Some(1, put)
//          next.higher           _ expects (0: Slice[Byte], *)  returning KeyValue.Put.Null
//          //@formatter:on
//        }
//        Higher(0: Slice[Byte]).runRandomIO.get.value shouldBe put
//      }
//    }
//
//    //   0
//    //     x
//    //     1
//    "2" in {
//
//      runThis(100.times) {
//
//        implicit val current = mock[CurrentWalker]
//        implicit val next = mock[NextWalker]
//
//        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(false))
//
//        inSequence {
//          //@formatter:off
//          current.higher        _ expects (0: Slice[Byte], *)  returning LevelSeek.None
//          next.higher           _ expects (0: Slice[Byte], *)  returning put
//          //@formatter:on
//        }
//        Higher(0: Slice[Byte]).runRandomIO.get.value shouldBe put
//      }
//    }
//
//
//    //   0
//    //     1
//    //     1
//    "3" in {
//      runThis(100.times) {
//
//        implicit val current = mock[CurrentWalker]
//        implicit val next = mock[NextWalker]
//
//        val upperKeyValue = randomFixedKeyValue(1, includeRemoves = false, deadline = randomDeadlineOption(false), functionOutput = randomUpdateFunctionOutput())
//        val lowerKeyValue = randomPutKeyValue(1, deadline = None)
//        val expected = FixedMerger(upperKeyValue, lowerKeyValue).runRandomIO
//
//        inSequence {
//          //@formatter:off
//          current.higher        _ expects (0: Slice[Byte], *)  returning LevelSeek.Some(1, upperKeyValue)
//          next.higher           _ expects (0: Slice[Byte], *)  returning lowerKeyValue
//          //@formatter:on
//        }
//        Higher(0: Slice[Byte]).runRandomIO.get.value shouldBe expected.right.value
//      }
//    }
//
//
//    //   0
//    //     1
//    //       2
//    "4" in {
//
//      runThis(100.times) {
//
//        implicit val current = mock[CurrentWalker]
//        implicit val next = mock[NextWalker]
//
//        val upperKeyValue = randomFixedKeyValue(1)
//        val lowerKeyValue = randomPutKeyValue(2, deadline = randomDeadlineOption(false))
//
//        val isUpperExpected =
//          upperKeyValue match {
//            case put: Memory.Put if put.hasTimeLeft() =>
//              true
//            case _ =>
//              false
//          }
//
//        val expected =
//          if (isUpperExpected) upperKeyValue else lowerKeyValue
//
//        inSequence {
//          //@formatter:off
//          current.higher        _ expects (0: Slice[Byte], *)  returning LevelSeek.Some(1, upperKeyValue)
//          next.higher           _ expects (0: Slice[Byte], *)  returning lowerKeyValue
//          if(!isUpperExpected) {
//            current.higher        _ expects (1: Slice[Byte], *)  returning LevelSeek.None
//          }
//          //@formatter:on
//        }
//        Higher(0: Slice[Byte]).runRandomIO.get.value shouldBe expected
//      }
//    }
//
//    //   0
//    //       2
//    //     1
//    "5" in {
//      runThis(100.times) {
//
//        implicit val current = mock[CurrentWalker]
//        implicit val next = mock[NextWalker]
//
//        val upperKeyValue = randomFixedKeyValue(2)
//        val lowerKeyValue = randomPutKeyValue(1)
//
//        inSequence {
//          //@formatter:off
//          current.higher        _ expects (0: Slice[Byte], *)  returning LevelSeek.Some(1, upperKeyValue)
//          next.higher           _ expects (0: Slice[Byte], *)  returning lowerKeyValue
//          //@formatter:on
//        }
//        Higher(0: Slice[Byte]).runRandomIO.get.value shouldBe lowerKeyValue
//      }
//    }
//  }
//}
