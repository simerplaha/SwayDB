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
//package swaydb.core.sweeper
//
//import swaydb.testkit.RunThis._
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
//        CacheActor[Int](stash = 10, 2.second, _ => 1) {
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
