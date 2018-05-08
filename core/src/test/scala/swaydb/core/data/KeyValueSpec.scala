///*
// * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
//package swaydb.core.data
//
//import org.scalatest.{Matchers, WordSpec}
//import swaydb.core.CommonAssertions
//import swaydb.serializers._
//import swaydb.serializers.Default._
//import scala.concurrent.duration._
//
//class KeyValueSpec extends WordSpec with Matchers with CommonAssertions {
//
//  "Transient.Put" should {
//    "update Stats" in {
//      val one = Transient.Put(1, 1, 10.seconds.fromNow)
//      val two = Transient.Put(2, 2, 20.seconds.fromNow)
//
//      val updated = one.updateStats(0.1, Some(two))
//
//      updated.hasTimeLeft() shouldBe true
//      updated.isRange shouldBe false
//      updated.isRemoveRange shouldBe false
//    }
//  }
//
//}
