///*
// * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// *
// * Additional permission under the GNU Affero GPL version 3 section 7:
// * If you modify this Program, or any covered work, by linking or combining
// * it with other code, such other code is not for that reason alone subject
// * to any of the requirements of the GNU Affero GPL version 3.
// */
//
//package swaydb.core.util.series
//
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.core.TestData._
//import swaydb.core.util
//import swaydb.core.util.Benchmark
//import swaydb.data.RunThis._
//
//class SeriesGrowableSpec extends AnyWordSpec with Matchers {
//
//  "it" should {
//    "increment on overflow" in {
//      runThis(10.times, log = true) {
//        val series = SeriesGrowable.volatile[Integer](randomIntMax(20))
//
//        val range = 0 to 10000
//        range foreach {
//          i =>
//            series add i
//            series.get(i) shouldBe i
//        }
//
//        range foreach {
//          i =>
//            series.get(i) shouldBe i
//        }
//      }
//    }
//
//    "benchamr" in {
//      val series = SeriesGrowable.volatile[Integer](100000)
//
//      val range = 0 to 1000000
//
//      Benchmark("") {
//        range foreach {
//          i =>
//            series add i
//        }
//      }
//
//      Benchmark("") {
//        range foreach {
//          i =>
//            series.get(i) shouldBe i
//        }
//      }
//
////      series.iteratorFlatten.foreach(println)
//    }
//  }
//
//}
