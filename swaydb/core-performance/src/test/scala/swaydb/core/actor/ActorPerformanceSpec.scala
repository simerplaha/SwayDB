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
//package swaydb.actor
//
//import org.scalatest.matchers.should.Matchers._
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.ActorConfig.QueueOrder
//import swaydb.core.TestExecutionContext
//import swaydb.{Actor, ActorRef, Benchmark}
//
//import java.util.concurrent.ConcurrentLinkedQueue
//import scala.collection.parallel.CollectionConverters._
//import scala.concurrent.duration._
//
//class ActorPerformanceSpec extends AnyWordSpec {
//
//  implicit val ec = TestExecutionContext.executionContext
//  implicit val ordering = QueueOrder.FIFO
//
//  "performance test" in {
//    //0.251675378 seconds.
//    //    val actor =
//    //      Actor.timerLoopCache[Int](100000, _ => 1, 5.second) {
//    //        (_: Int, self: ActorRef[Int, Unit]) =>
//    //      }
//
//    val actor =
//      Actor.timerCache[Int]("", 100000, _ => 1, 5.second) {
//        (_: Int, self: ActorRef[Int, Unit]) =>
//      }.start()
//
//
//    //0.111304334 seconds.
//    //    val actor =
//    //      Actor.cache[Int](100000, _ => 1) {
//    //        (_: Int, self: ActorRef[Int, Unit]) =>
//    //      }
//
//    //0.186314412 seconds.
//    //    val actor =
//    //      Actor[Int] {
//    //        (_: Int, self: ActorRef[Int, Unit]) =>
//    //      }
//
//    val queue = new ConcurrentLinkedQueue[Int]()
//
//    Benchmark("") {
//      (1 to 1000000).par foreach {
//        i =>
//          actor send i
//        //          queue.add(i)
//      }
//    }
//  }
//}
