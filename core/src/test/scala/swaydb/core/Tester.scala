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
//package swaydb.core
//
//import java.nio.file.Paths
//import scala.concurrent.ExecutionContext.Implicits.global
//import swaydb.core.function.FunctionStore
//import swaydb.core.segment.Segment
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.storage.LevelStorage
//import swaydb.serializers._
//import swaydb.serializers.Default._
//
//class Tester extends TestBase {
//
//  implicit val keyOrder = KeyOrder.default
//  implicit val timeOrder = TimeOrder.long
//  implicit val functionStore = FunctionStore.memory()
//  implicit val timerReader = TestLimitQueues.fileOpenLimiter
//  implicit val timerWriter = TestLimitQueues.keyValueLimiter
//  implicit val grouping = None
//
//  "dsadsadx" in {
//    val path = Paths.get("/Users/simerplaha/IdeaProjects/SwayDB.benchmark/speedDB/7")
//    val level = TestLevel(levelStorage = LevelStorage.Persistent(true, true, path, Seq.empty))
//
//    (0L to 1000000000L) foreach {
//      i =>
//        val keyValue = level.get(i).safeGetBlocking.get
//        if (keyValue.isEmpty) {
//          println(s"Last: $i")
//          System.exit(0)
//        }
//
//        if (i % 10000 == 0)
//          println(keyValue.get.key.readLong() + " -> " + keyValue.get.getOrFetchValue.get.get.readString())
//    }
//
//    //    println(level.lastKey.safeGetBlocking.get.get.readLong())
//
//  }
//
//}
