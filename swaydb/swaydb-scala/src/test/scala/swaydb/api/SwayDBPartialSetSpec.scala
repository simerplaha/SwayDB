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
//import org.scalatest.OptionValues._
//import swaydb.Glass
//import swaydb.core.CoreTestSweeper
//import swaydb.core.CoreTestSweeper._
//import swaydb.slice.Slice
//import swaydb.slice.order.KeyOrder
//import swaydb.testkit.RunThis._
//import swaydb.utils.StorageUnits._
//
//import scala.concurrent.duration._
//import swaydb.core.file.CoreFileTestKit._
//
//object SwayDBPartialSetSpec {
//
//  import boopickle.Default._
//
//  implicit object Serialiser extends swaydb.serializers.Serializer[(Int, Option[String])] {
//    override def write(data: (Int, Option[String])): Slice[Byte] =
//      Slice.wrap(Pickle.intoBytes(data).array())
//
//    override def read(slice: Slice[Byte]): (Int, Option[String]) =
//      Unpickle[(Int, Option[String])].fromBytes(slice.toByteBufferWrap())
//  }
//
//  implicit val ordering =
//    new KeyOrder[(Int, Option[String])] {
//      override def compare(x: (Int, Option[String]), y: (Int, Option[String])): Int =
//        x._1 compare y._1
//
//      private[swaydb] override def comparableKey(key: (Int, Option[String])): (Int, Option[String]) =
//        (key._1, None)
//    }
//}
//
//class SwayDBPartialSet_Persistent_Spec extends SwayDBPartialSetSpec {
//
//  import SwayDBPartialSetSpec._
//
//  override def newDB()(implicit sweeper: CoreTestSweeper): swaydb.Set[(Int, Option[String]), Nothing, Glass] =
//    swaydb.persistent.Set[(Int, Option[String]), Nothing, Glass](randomDir(), logSize = 10.bytes).sweep(_.delete())
//}
//
//class SwayDBPartialSet_Memory_Spec extends SwayDBPartialSetSpec {
//
//  import SwayDBPartialSetSpec._
//
//  override def newDB()(implicit sweeper: CoreTestSweeper): swaydb.Set[(Int, Option[String]), Nothing, Glass] =
//    swaydb.memory.Set[(Int, Option[String]), Nothing, Glass](logSize = 10.bytes).sweep(_.delete())
//}
//
//trait SwayDBPartialSetSpec extends TestBaseAPI {
//
//  val keyValueCount = 1000
//
//  def newDB()(implicit sweeper: CoreTestSweeper): swaydb.Set[(Int, Option[String]), Nothing, Glass]
//
//  "read partially ordered key-values" in {
//    runThis(times = repeatTest, log = true) {
//      CoreTestSweeper {
//        implicit sweeper =>
//
//          val set = newDB()
//
//          val keyValues =
//            (1 to 1000) map {
//              i =>
//                val keyValues = (i, Some(s"value $i"))
//                set.add(keyValues)
//                keyValues
//            }
//
//          def assertReads() = {
//            (1 to 1000) foreach {
//              i =>
//                set.get((i, None)).value shouldBe ((i, Some(s"value $i")))
//            }
//
//            set.materialize.toList shouldBe keyValues
//            set.reverse.materialize.toList shouldBe keyValues.reverse
//          }
//
//          assertReads()
//          sleep(5.seconds)
//          assertReads()
//      }
//    }
//  }
//}
