/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.benchmark

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.level.zero.LevelZero
import swaydb.core.segment.ThreadReadState
import swaydb.core.util.Benchmark
import swaydb.data.slice.Slice
import swaydb.serializers.Default.{LongSerializer, StringSerializer}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.Random

case class RunnerZero(test: Test) extends LazyLogging {

  private val map: LevelZero = test.map.core.zero
  private val randomWrite: Boolean = test.randomWrite
  private val randomRead: Boolean = test.randomRead
  private val keyValueCount: Long = test.keyValueCount

  def run(): Unit = {
    println(s"\nCreating $keyValueCount test key-values.\n")

    val stringValue = "Test value of 60 bytes for benchmarking SwayDB's performance"
    val valueBytes = StringSerializer.write(stringValue)

    val testValue =
      if (test.useMap)
        valueBytes
      else
        Slice.Null

    val keys =
      if (test.useMap)
        (0L to keyValueCount).map(LongSerializer.write)
      else
        (0L to keyValueCount) map {
          key =>
            LongSerializer.write(key) ++ valueBytes
        }

    lazy val shuffledKeys = Random.shuffle(keys)

    val writeKeys = if (randomWrite) shuffledKeys else keys

    Benchmark("Write benchmark") {
      writeKeys foreach {
        key =>
          map.put(key, testValue)
      }
    }
    println(s"mapsCount: ${map.levelZeroMeter.mapsCount}")
    val readKeys = if (randomRead) shuffledKeys else keys
    val readState = ThreadReadState.limitHashMap(10, 1)
    Benchmark("Read benchmark during compaction") {
      readKeys foreach {
        key =>
          map.get(key, readState)
      }
    }

    def pluralSegment(count: Int) = if (count == 1) "Segment" else "Segments"

    @tailrec
    def areTopLevelsEmpty(levelNumber: Int): Unit =
      map.meterFor(levelNumber) match {
        case Some(meter) if map.meterFor(levelNumber + 1).nonEmpty =>
          if (meter.segmentsCount == 0) {
            println(s"Level $levelNumber is empty.")
            areTopLevelsEmpty(levelNumber + 1)
          } else {
            val interval = (levelNumber * 3).seconds
            println(s"Level $levelNumber contains ${meter.segmentsCount} ${pluralSegment(meter.segmentsCount)}. Will check again after $interval.")
            Thread.sleep(interval.toMillis) //<-- For test case so it's ok :)
            areTopLevelsEmpty(levelNumber)
          }
        case _ =>
          val segmentsCount = map.meterFor(levelNumber).map(_.segmentsCount) getOrElse -1
          println(s"Compaction completed. Level ${levelNumber + 1} contains all $segmentsCount ${pluralSegment(segmentsCount)}.\n")
      }

    println("Waiting for compaction to complete before executing after compaction benchmark.")
    areTopLevelsEmpty(1)

    println(s"mapsCount: ${map.levelZeroMeter.mapsCount}")
    val readState2 = ThreadReadState.limitHashMap(10, 1)
    Benchmark("Read benchmark after compaction") {
      readKeys foreach {
        key =>
          map.get(key, readState2)
      }
    }
  }
}
