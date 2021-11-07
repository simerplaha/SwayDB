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
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestData._
//import swaydb.core.data.{KeyValue, SwayFunctionOutput, Value}
//import swaydb.core.segment.ref.search.ThreadReadState
//import swaydb.core.{TestData, TestTimer}
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.testkit.RunThis._
//
//class GetNoneSpec extends AnyWordSpec with Matchers with MockFactory with OptionValues {
//
//  implicit val keyOrder = KeyOrder.default
//  implicit val timeOrder = TimeOrder.long
//  implicit val functionStore = TestData.functionStore
//
//  "return None" when {
//    "put is expired" in {
//      runThis(100.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1, deadline = Some(expiredDeadline()))
//
//        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
//      }
//    }
//
//    "removed" in {
//      runThis(100.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomRemoveKeyValue(1, randomExpiredDeadlineOption())
//
//        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
//      }
//    }
//
//    "remove has time left but next Level returns None" in {
//      runThis(100.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomRemoveKeyValue(1, Some(randomDeadline(expired = false)))
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning KeyValue.Put.Null
//
//        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
//      }
//    }
//
//    "remove has time left but next Level Put is expired" in {
//      runThis(100.times) {
//        implicit val testTimer = TestTimer.Decremental()
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomRemoveKeyValue(1, Some(randomDeadline(expired = false)))
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1, deadline = Some(expiredDeadline()))
//
//        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
//      }
//    }
//
//    "update has no next Level put" in {
//      runThis(100.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomUpdateKeyValue(1, deadline = randomDeadlineOption(false))
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning KeyValue.Put.Null
//
//        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
//      }
//    }
//
//    "update has next Level expired put" in {
//      runThis(100.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomUpdateKeyValue(1, deadline = randomDeadlineOption(false))
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1, deadline = Some(expiredDeadline()))
//
//        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
//      }
//    }
//
//    "function has no next Level put" in {
//      runThis(100.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomFunctionKeyValue(1)
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning KeyValue.Put.Null
//
//        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
//      }
//    }
//
//    "function has next Level expired put" in {
//      runThis(100.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomFunctionKeyValue(1)
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1, deadline = Some(expiredDeadline()))
//
//        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
//      }
//    }
//
//    "pending applies has no next Level put" in {
//      runThis(100.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomPendingApplyKeyValue(1)
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning KeyValue.Put.Null
//
//        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
//      }
//    }
//
//    "pending applies has next Level expired put" in {
//      runThis(100.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomPendingApplyKeyValue(1)
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1, deadline = Some(expiredDeadline()))
//
//        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
//      }
//    }
//
//    "pending applies has Level put that resulted in expiry" in {
//      runThis(100.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        val pendingApply =
//          randomPendingApplyKeyValue(
//            key = 1,
//            deadline = Some(expiredDeadline()),
//            functionOutput =
//              eitherOne(
//                SwayFunctionOutput.Remove,
//                SwayFunctionOutput.Expire(expiredDeadline()),
//                SwayFunctionOutput.Update(randomStringOption, Some(expiredDeadline()))
//              )
//          )
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning pendingApply
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1, deadline = Some(expiredDeadline()))
//
//        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
//      }
//    }
//
//    "range's fromValue is removed or expired" in {
//      runThis(100.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        val fromValue =
//          eitherOne(
//            Value.remove(None),
//            Value.update(Slice.Null, Some(expiredDeadline())),
//            Value.put(Slice.Null, Some(expiredDeadline()))
//          )
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomRangeKeyValue(1, 10, fromValue)
//
//        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
//      }
//    }
//
//    "range's fromValue or/and rangeValue is a function that removes or expired" in {
//      runThis(30.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        val functionValue =
//          randomKeyValueFunctionKeyValue(
//            key = 1,
//            output =
//              eitherOne(
//                SwayFunctionOutput.Remove,
//                SwayFunctionOutput.Expire(expiredDeadline()),
//                SwayFunctionOutput.Update(randomStringOption, Some(expiredDeadline()))
//              )
//          ).toRangeValue()
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomRangeKeyValue(1, 10, eitherOne(Value.FromValue.Null, functionValue), functionValue)
//        //next level can return anything it will be removed.
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1)
//
//        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
//      }
//    }
//  }
//}
