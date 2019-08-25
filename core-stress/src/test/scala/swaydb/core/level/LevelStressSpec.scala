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
//package swaydb.core.level
//
//import swaydb.IO
//import swaydb.core.CommonAssertions._
//import swaydb.IOValues._
//import swaydb.core.RunThis._
//import swaydb.core.TestBase
//import swaydb.core.TestData._
//import swaydb.core.group.compression.data.GroupByInternal.KeyValues
//import swaydb.core.util.Benchmark
//import swaydb.data.order.KeyOrder
//import swaydb.data.util.StorageUnits._
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
//  override def level0MMAP = true
//  override def appendixStorageMMAP = true
//}
//
//class LevelStressSpec2 extends LevelStressSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegmentsOnWrite = false
//  override def mmapSegmentsOnRead = false
//  override def level0MMAP = false
//  override def appendixStorageMMAP = false
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
//              case IO.Success(_) =>
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
//              case IO.Failure(error) =>
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
