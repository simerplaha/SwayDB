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
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.queue
//
//import swaydb.core.RunThis._
//import swaydb.core.TestBase
//
//import scala.concurrent.Future
//import scala.concurrent.duration._
//
//class CacheActorStressSpec extends TestBase {
//
//  override def inMemoryStorage: Boolean = true
//
//  "Limiter" should {
//    "performance eviction on a large queue" in {
//      //increase this val to test the queue on a larger number of items
//      val itemCount = 100
//
//      @volatile var evictedItems = 0
//      val limitQueue =
//        CacheActor[Int](maxWeight = 10, 2.second, _ => 1) {
//          _ =>
//            evictedItems += 1
//        }
//
//      //concurrent submit items to the queue
//      (1 to itemCount) foreach {
//        i =>
//          Future(limitQueue ! i)
//      }
//      //eventually the queue drop the overflown items
//      eventual(10.seconds) {
//        evictedItems shouldBe (itemCount - 10)
//      }
//    }
//
//    //    "performance insert 10 million items to queue" in {
//    //      import swaydb.serializers._
//    //      import swaydb.serializers.Default._
//    //      //increase this val to test the queue on a larger number of items
//    //      val itemCount = 10000000
//    //
//    //      val limitQueue = MemorySweeper(100.mb, 5.seconds)
//    //      val segment = TestSegment().get
//    //
//    //      val skipList = SkipList.concurrent[Slice[Byte], Int](KeyOrder.default)
//    //
//    //      (1 to itemCount) foreach {
//    //        i =>
//    //          skipList.put(i, i)
//    //      }
//    //
//    //      (1 to itemCount) foreach {
//    //        i =>
//    //          if (i % 10000 == 0) {
//    //            println(s"Key: $i")
//    //          }
//    //          //          val item = CreatedReadOnly(i, Reader.emptyReader, 0, 0, 0, 10, 0)
//    //          limitQueue.add(Persistent.Put(i, None, Reader.empty, 0, 0, 0, 0, 0), skipList)
//    //      }
//    //    }
//  }
//}
