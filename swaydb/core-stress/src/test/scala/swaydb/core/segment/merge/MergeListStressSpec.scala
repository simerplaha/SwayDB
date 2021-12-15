/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//
//package swaydb.core.segment.data.merge
//
//import org.scalatest.matchers.should.Matchers
//import swaydb.core.CommonAssertions._
//import swaydb.testkit.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.TestTimer
//import swaydb.core.segment.data.{KeyValue, Memory, Value}
//import swaydb.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//import scala.collection.mutable.ListBuffer
//import scala.util.Random
//
//class MergeListStressSpec extends AnyWordSpec with Matchers {
//
//  implicit def testTimer: TestTimer = TestTimer.random
//
//  implicit def toPut(key: Int): Memory.Put =
//    Memory.put(key)
//
//  "MergeList" should {
//    "stress" in {
//      val initialKeyValues = Slice[KeyValue](1, 2, 3)
//      var list = MergeList[KeyValue.Range, KeyValue](initialKeyValues)
//      val range = Memory.Range(1, 2, None, Value.update(1))
//
//      val stateExpected = ListBuffer.empty[KeyValue] ++ initialKeyValues
//
//      var int = 4
//
//      def nextInt(): Int = {
//        int += 1
//        int
//      }
//
//      runThis(1000.times) {
//        //Append
//        if (Random.nextBoolean()) {
//          val keyValues = Slice[KeyValue](nextInt(), nextInt())
//          list = list append MergeList(keyValues)
//          stateExpected ++= keyValues
//        }
//
//        //drop head
//        if (stateExpected.nonEmpty && Random.nextBoolean()) {
//          list = list.dropHead()
//          stateExpected.remove(0)
//        }
//
//        //drop prepend
//        if (stateExpected.nonEmpty && Random.nextBoolean()) {
//          list = list.dropPrepend(range)
//          stateExpected.remove(0)
//          range +=: stateExpected
//        }
//
//        //assert
//        list shouldBe stateExpected
//      }
//    }
//  }
//}
