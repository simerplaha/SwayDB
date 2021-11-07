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
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.core.util.Times._
//import swaydb.data.slice.Slice
//
//import scala.concurrent.duration._
//
//class TimesSpec extends AnyWordSpec with Matchers {
//
//  "toNanos" should {
//    "convert deadline to nanos" in {
//      val duration = 10.seconds
//      val deadline = Deadline(duration)
//      deadline.toNanos shouldBe duration.toNanos
//
//      Some(deadline).toNanos shouldBe duration.toNanos
//    }
//
//    "convert none deadline to 0" in {
//      Option.empty[Deadline].toNanos shouldBe 0L
//    }
//  }
//
//  "toBytes" should {
//    "convert deadline long bytes" in {
//      val duration = 10.seconds
//      val deadline = Deadline(duration)
//      deadline.toUnsignedBytes shouldBe Slice.writeUnsignedLong[Byte](duration.toNanos)
//      deadline.toBytes shouldBe Slice.writeLong[Byte](duration.toNanos)
//    }
//  }
//
//  "toDeadline" should {
//    "convert long to deadline" in {
//      val duration = 10.seconds
//      duration.toNanos.toDeadlineOption should contain(Deadline(duration))
//    }
//
//    "convert 0 to None" in {
//      0L.toDeadlineOption shouldBe empty
//    }
//  }
//
//  "earlier" should {
//    "return the earliest deadline" in {
//      val deadline1 = 10.seconds.fromNow
//      val deadline2 = 20.seconds.fromNow
//
//      deadline1.earlier(Some(deadline2)) shouldBe deadline1
//      deadline2.earlier(Some(deadline1)) shouldBe deadline1
//
//      deadline1.earlier(None) shouldBe deadline1
//      deadline2.earlier(None) shouldBe deadline2
//    }
//  }
//}
