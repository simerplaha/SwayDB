///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.segment.merge
//
//import org.scalatest.{Matchers, WordSpec}
//import swaydb.core.CommonAssertions._
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.TestTimer
//import swaydb.core.data.{KeyValue, Memory, Value}
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//import scala.collection.mutable.ListBuffer
//import scala.util.Random
//
//class MergeListStressSpec extends WordSpec with Matchers {
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
