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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.actor

import java.util.concurrent.ConcurrentSkipListSet

import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.data.config.ActorConfig

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.ref.WeakReference

case class Item(i: Int)
class CacheActorSpec extends TestBase {

  //  "Limiter.nextDelay" in {
  //    //if the limiter is overflown, next delay is halved
  //    LimitQueue.nextDelay(size = 100, limit = 100, currentDelay = 10.seconds, defaultDelay = 10.seconds) shouldBe 10.seconds
  //    LimitQueue.nextDelay(size = 150, limit = 100, currentDelay = 10.seconds, defaultDelay = 10.seconds) shouldBe 10.seconds
  //    LimitQueue.nextDelay(size = 199, limit = 100, currentDelay = 10.seconds, defaultDelay = 10.seconds) shouldBe 10.seconds
  //    LimitQueue.nextDelay(size = 200, limit = 100, currentDelay = 10.seconds, defaultDelay = 10.seconds) shouldBe 5.seconds
  //    LimitQueue.nextDelay(size = 400, limit = 100, currentDelay = 10.seconds, defaultDelay = 10.seconds) shouldBe 2.5.seconds
  //    LimitQueue.nextDelay(size = 500, limit = 100, currentDelay = 10.seconds, defaultDelay = 10.seconds) shouldBe 2.seconds
  //    //large overflows return a minimum of 1.second delay and no less.
  //    LimitQueue.nextDelay(size = 100000, limit = 100, currentDelay = 10.seconds, defaultDelay = 10.seconds) shouldBe 1.second
  //    //    //if not overflow, next delay remains the same
  //    LimitQueue.nextDelay(size = 100, limit = 200, currentDelay = 10.seconds, defaultDelay = 10.seconds) shouldBe 10.seconds
  //    LimitQueue.nextDelay(size = 50, limit = 200, currentDelay = 10.seconds, defaultDelay = 10.seconds) shouldBe 10.seconds
  //    LimitQueue.nextDelay(size = 0, limit = 200, currentDelay = 10.seconds, defaultDelay = 10.seconds) shouldBe 10.seconds
  //  }

  "CacheActor" should {
    "evicted overflown items on concurrent writes" in {
      val evictedItems = new ConcurrentSkipListSet[String]()
      val limitQueue =
        CacheActor[String](maxWeight = 10, ActorConfig.TimeLoop(10000, 10000, 2.seconds, ec), _ => 1) {
          evictedItem =>
            evictedItems.add(evictedItem)
            println(s"Evicted: $evictedItem")
        }

      (1 to 10000) foreach {
        i =>
          Future(limitQueue ! i.toString)
      }
      eventual(10.seconds) {
        evictedItems should have size 9990
      }
      limitQueue.terminate()
    }

    "evicted WeaklyReferenced items in FIFO manner" in {

      val evictedItems = ListBuffer.empty[WeakReference[Item]]
      val limitQueue = CacheActor[WeakReference[Item]](maxWeight = 5, ActorConfig.TimeLoop(10000, 10000, 1.second, ec), _ => 1) {
        evictedItem =>
          evictedItems += evictedItem
          println(s"Evicted: ${evictedItem.get}")
      }

      var items: Seq[Item] = (1 to 10).map(Item)
      //send 10 times
      items foreach {
        item =>
          limitQueue ! new WeakReference(item)
      }
      //drop the first 5 Items for GC to pick up and clear.
      items = items.drop(5)
      System.gc()
      //wait for the next loop to occur for LimitQueue.
      sleep(2.seconds)
      evictedItems should have size 5
      //since the 10 items are GC'ed. They should return None
      evictedItems.map(_.get shouldBe empty)
      //clear and ready for second eviction
      evictedItems.clear()

      //after the previous eviction submit another 5 times so another 5 value evicted.
      //but this time the second half if items (first items) is evicted.
      val items2: Seq[Item] = (11 to 15).map(Item)
      items2 foreach {
        item =>
          limitQueue ! new WeakReference[Item](item)
      }
      sleep(4.seconds)
      evictedItems should have size 5
      evictedItems.flatMap(_.get) should contain inOrderOnly(Item(6), Item(7), Item(8), Item(9), Item(10))
      evictedItems.clear()

      //a third eviction should not occur since the LimitQueue is not overflown
      sleep(4.seconds)
      evictedItems shouldBe empty

      //add 1 item
      limitQueue ! new WeakReference[Item](Item(16))
      //sleep to wait for eviction
      sleep(4.seconds)
      //only one item is evicted. Which is the first Item for the second insertion
      evictedItems should have size 1
      evictedItems.flatMap(_.get) should contain only Item(11)

      limitQueue.terminate()
    }

    "stop loop if terminated" in {
      val evictedItems = new ConcurrentSkipListSet[String]()
      val limitQueue =
        CacheActor[String](maxWeight = 1, ActorConfig.TimeLoop(10000, 10000, 2.seconds, ec), _ => 1) {
          evictedItem =>
            evictedItems.add(evictedItem)
            println(s"Evicted: $evictedItem")
        }

      (1 to 1000) foreach {
        i =>
          limitQueue ! i.toString
          if (i == 100)
            limitQueue.terminate()
      }

      def assertNotLooping() = {
        sleep(3.second)
        evictedItems shouldBe empty
      }

      assertNotLooping()
      assertNotLooping()
      limitQueue.terminate()
    }

    //    "benchmark" in {
    //
    //      //1.859036011 seconds.
    //      val limitQueue =
    //        LimitQueue[String](limit = 10000, 5.second, _ => 1) {
    //          evictedItem =>
    ////            println(s"Evicted: $evictedItem")
    //        }
    //
    //      //
    //
    //      //9.428285501 seconds.
    //      Benchmark("LimitQueue") {
    //        (1 to 10000000) foreach {
    //          i =>
    //            limitQueue ! i.toString
    //        }
    //      }
    //    }
  }
}
