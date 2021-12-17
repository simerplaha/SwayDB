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
//package swaydb.core.level
//
//import swaydb.IO
//import swaydb.core.CommonAssertions._
//import swaydb.effect.IOValues._
//import swaydb.testkit.RunThis._
//import swaydb.core.TestBase
//import swaydb.core.TestData._
//import swaydb.core.group.compression.data.GroupByInternal.KeyValues
//import swaydb.Benchmark
//import swaydb.slice.order.KeyOrder
//import swaydb.config.util.StorageUnits._
//
//import scala.collection.immutable
//import scala.concurrent.Future
//import scala.concurrent.duration._
//import scala.util.Random
//
//class LevelStressSpec0 extends LevelStressSpec
//
//class LevelStressSpec1 extends LevelStressSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegmentsOnWrite = true
//  override def mmapSegmentsOnRead = true
//  override def level0MMAP = MMAP.Enabled(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def appendixStorageMMAP = MMAP.Enabled(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//}
//
//class LevelStressSpec2 extends LevelStressSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegmentsOnWrite = false
//  override def mmapSegmentsOnRead = false
//  override def level0MMAP = MMAP.Disabled(forceSave = TestForceSave.standard())
//  override def appendixStorageMMAP = MMAP.Disabled(forceSave = TestForceSave.standard())
//}
//
//class LevelStressSpec3 extends LevelStressSpec {
//  override def inMemoryStorage = true
//}
//
//sealed trait LevelStressSpec extends TestBase with Benchmark {
//
//  implicit val keyOrder = KeyOrder.default
//
//  "Concurrent writing and reading a Level" in {
//    runThis(10.times) {
//      val keyValueCount = 100000
//      val writeIterations = 100
//      val readIterations = 200
//
//      implicit val groupBy: Option[GroupByInternal.KeyValues] = Some(randomGroupBy(keyValueCount))
//
//      val level = TestLevel(segmentSize = 500.kb)
//
//      val keyValues = randomPutKeyValues(keyValueCount, startId = Some(0))
//      val segment1 = TestSegment(keyValues.toTransient).runIO
//      val segment2 = TestSegment(keyValues.take(randomIntMax(keyValues.size) max 1).toTransient).runIO
//      val segment3 = TestSegment(keyValues.take(randomIntMax(keyValues.size) max 1).toTransient).runIO
//      val segment4 = TestSegment(keyValues.take(randomIntMax(keyValues.size) max 1).toTransient).runIO
//      val segment5 = TestSegment(keyValues.take(randomIntMax(keyValues.size) max 1).toTransient).runIO
//
//      val segments = Seq(segment1, segment2, segment3, segment4, segment5)
//
//      def randomSegment = Random.shuffle(segments)
//
//      level.put(segment1).runIO
//
//      def doPut(index: Int): Future[Unit] =
//        Future() flatMap {
//          _ =>
//            level.put(randomSegment.head) match {
//              case IO.Right(_) =>
//                println(s"$index: End put.")
//                Future.unit
//
//              case later @ IO.Defer(_, _) =>
//                println(s"$index: Later")
//                later.runInFuture flatMap {
//                  _ =>
//                    println(s"$index: Later received pull. Trying again!")
//                    doPut(index)
//                } recoverWith {
//                  case error =>
//                    error.printStackTrace()
//                    System.exit(0) //failure should never occur.
//                    throw error
//                }
//
//              case IO.Left(error) =>
//                error.exception.printStackTrace()
//                System.exit(0) //failure should never occur.
//                fail(error.exception)
//            }
//        }
//
//      val writes: Future[immutable.IndexedSeq[Unit]] =
//        Future.sequence {
//          (1 to writeIterations) map {
//            i =>
//              println(s"$i: Starting put.")
//              doPut(i)
//          }
//        }
//
//      val reads: Future[immutable.IndexedSeq[Unit]] =
//        Future.sequence {
//          (1 to readIterations) map {
//            i =>
//              Future {
//                println(s"$i: Starting read.")
//                try
//                  assertReads(keyValues, level)
//                catch {
//                  case exception: Throwable =>
//                    exception.printStackTrace()
//                    System.exit(0) //failure should never occur.
//                }
//                println(s"$i: Finished read.")
//              }
//          }
//        }
//
//      val testResult =
//        for {
//          writesResult <- writes
//          readsResult <- reads
//        } yield {
//          writesResult should have size writeIterations
//          readsResult should have size readIterations
//        }
//
//      testResult await 10.minutes
//
//      level.delete.runIO
//      segments.foreach(_.delete.runIO)
//
//      println("TEST COMPLETE!")
//      println("STARTING NEXT TEST ITERATION.")
//    }
//  }
//}
