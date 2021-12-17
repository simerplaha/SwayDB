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
//package swaydb.core.util
//
//import org.scalatest.concurrent.Eventually
//import org.scalatest.matchers.should.Matchers._
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.Error.Segment.ExceptionHandler
//import swaydb.core.CoreTestSweeper._
//import swaydb.core.{CoreTestSweeper, TestExecutionContext}
//import swaydb.{IO, Scheduler}
//
//import scala.concurrent.duration._
//
//class SchedulerSpec extends AnyWordSpec with Eventually {
//
//  implicit val ec = TestExecutionContext.executionContext
//
//  //  "Delay.cancelTimer" should {
//  //    "cancel all existing scheduled tasks" in {
//  //      @volatile var tasksExecuted = 0
//  //
//  //      //create 5 tasks
//  //      Delay.future(1.second)(tasksExecuted += 1)
//  //      Delay.future(1.second)(tasksExecuted += 1)
//  //      Delay.future(1.second)(tasksExecuted += 1)
//  //      Delay.future(2.second)(tasksExecuted += 1)
//  //      Delay.future(2.second)(tasksExecuted += 1)
//  //
//  //      //after 1.5 seconds cancel timer
//  //      Thread.sleep(1.5.seconds.toMillis)
//  ////      Delay.cancelTimer()
//  //
//  //      //the remaining two tasks did not value executed.
//  //      tasksExecuted shouldBe 3
//  //
//  //      //after 2.seconds the remaining two tasks are still not executed.
//  //      Thread.sleep(2.seconds.toMillis)
//  //      tasksExecuted shouldBe 3
//  //    }
//  //  }
//
//  "Delay.task" should {
//    "run tasks and cancel tasks" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//
//          @volatile var tasksExecuted = 0
//
//          val scheduler = Scheduler().sweep()
//
//          scheduler.task(1.seconds)(tasksExecuted += 1)
//          scheduler.task(2.seconds)(tasksExecuted += 1)
//          scheduler.task(3.seconds)(tasksExecuted += 1)
//          scheduler.task(4.seconds)(tasksExecuted += 1)
//          scheduler.task(5.seconds)(tasksExecuted += 1)
//
//          eventually(timeout(8.seconds)) {
//            tasksExecuted shouldBe 5
//          }
//
//          scheduler.task(1.seconds)(tasksExecuted += 1).cancel()
//          scheduler.task(1.seconds)(tasksExecuted += 1)
//
//          Thread.sleep(5.seconds.toMillis)
//
//          tasksExecuted shouldBe 6
//      }
//    }
//  }
//
//  "futureFromIO" should {
//    "run in future and return result" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          @volatile var tryThread = ""
//
//          val scheduler = Scheduler().sweep()
//
//          scheduler.futureFromIO(100.millisecond) {
//            IO {
//              tryThread = Thread.currentThread().getName
//            }
//          }
//
//          val currentThread = Thread.currentThread().getName
//
//          eventually(timeout(2.seconds)) {
//            tryThread should not be empty
//            tryThread should not be currentThread
//          }
//      }
//    }
//  }
//
//  "future" should {
//    "run in future" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          @volatile var futureThread = ""
//
//          val scheduler = Scheduler().sweep()
//
//          scheduler.future(100.millisecond) {
//            futureThread = Thread.currentThread().getName
//          }
//
//          val currentThread = Thread.currentThread().getName
//
//          eventually(timeout(2.seconds)) {
//            futureThread should not be empty
//            futureThread should not be currentThread
//          }
//      }
//    }
//  }
//}
