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
//package swaydb.api
//
//import swaydb.effect.IOValues._
//import swaydb._
//import swaydb.core.CoreTestSweeper
//import swaydb.core.CoreTestSweeper._
//import swaydb.serializers.Default._
//import swaydb.testkit.RunThis._
//import swaydb.core.file.CoreFileTestKit._
//
//class SwayDBSize_Persistent_Spec extends SwayDBSizeSpec {
//  val keyValueCount: Int = 10000
//
//  override def newDB()(implicit sweeper: CoreTestSweeper): Map[Int, String, Nothing, IO.ApiIO] =
//    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](dir = randomDir()).right.value.sweep(_.delete().get)
//}
//
//class SwayDBSize_Memory_Spec extends SwayDBSizeSpec {
//  val keyValueCount: Int = 10000
//
//  override def newDB()(implicit sweeper: CoreTestSweeper): Map[Int, String, Nothing, IO.ApiIO] =
//    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value.sweep(_.delete().get)
//}
//
//class MultiMapSizeSpec4 extends SwayDBSizeSpec {
//  val keyValueCount: Int = 10000
//
//  override def newDB()(implicit sweeper: CoreTestSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
//    generateRandomNestedMaps(swaydb.persistent.MultiMap[Int, Int, String, Nothing, IO.ApiIO](dir = randomDir()).get).sweep(_.delete().get)
//}
//
//class MultiMapSizeSpec5 extends SwayDBSizeSpec {
//  val keyValueCount: Int = 10000
//
//  override def newDB()(implicit sweeper: CoreTestSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
//    generateRandomNestedMaps(swaydb.memory.MultiMap[Int, Int, String, Nothing, IO.ApiIO]().get).sweep(_.delete().get)
//}
//
//sealed trait SwayDBSizeSpec extends TestBaseAPI {
//
//  val keyValueCount: Int
//
//  override def deleteFiles = false
//
//  def newDB()(implicit sweeper: CoreTestSweeper): MapT[Int, String, Nothing, IO.ApiIO]
//
//  "return the size of key-values" in {
//    runThis(times = repeatTest, log = true) {
//      CoreTestSweeper {
//        implicit sweeper =>
//
//          val db = newDB()
//
//          (1 to keyValueCount) foreach {
//            i =>
//              db.put(i, i.toString).right.value
//          }
//
//          db.count.value shouldBe keyValueCount
//      }
//    }
//  }
//}
