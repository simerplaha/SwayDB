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
import swaydb.{Bag, IO}
import swaydb.core.util.Benchmark
import swaydb.data.slice.Slice
import swaydb.serializers.Default.{LongSerializer, StringSerializer}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.Random

case class RunnerAPI(test: Test) extends LazyLogging {

  private val map: swaydb.Map[Slice[Byte], Option[Slice[Byte]], Nothing, Bag.Id] = test.map
  private val randomWrite: Boolean = test.randomWrite
  private val randomRead: Boolean = test.randomRead
  private val forwardIteration: Boolean = test.forwardIteration
  private val reverseIteration: Boolean = test.reverseIteration
  private val keyValueCount: Long = test.keyValueCount

  implicit val bag = Bag.idBag

  def run = {
    println(s"\nCreating $keyValueCount test key-values.\n")

    val stringValue = "Test value of 60 bytes for benchmarking SwayDB's performance"
    val valueBytes = StringSerializer.write(stringValue)

    val testValue =
      if (test.useMap)
        Some(valueBytes)
      else
        None

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

    println("Warming up 100 keys ...")
    writeKeys.take(100) foreach {
      key =>
        map.get(key)
    }

    if (forwardIteration)
      Benchmark("Forward iteration benchmark during compaction") {
        map
          .stream
          .foreach {
            keyValue =>
            //              val key = keyValue._1.readLong()
            //              if (key % 10000 == 0)
            //                println(key + " -> " + keyValue._2.map(_.readString()))
          }
          .materialize
      }
    else if (reverseIteration)
      Benchmark("Reverse iteration benchmark during compaction") {
        map
          .reverse
          .stream
          .foreach {
            case (key, _) =>
            //              println(s"${LongSerializer.read(key)}")
          }
          .materialize[Bag.Id]
      }
    else {
      println(s"mapsCount: ${map.levelZeroMeter.mapsCount}")
      val readKeys = if (randomRead) shuffledKeys else keys
      Benchmark("Read benchmark during compaction") {
        //        (1 to 5).par foreach {
        //          _ =>
        readKeys foreach {
          key =>
            //                try {
            map.get(key)
          //                  val value = map.get(key).get.get
          //                  val longKey = key.readLong()
          //                  if (longKey % 10000 == 0) {
          //                    val valueString = value.map(_.readString())
          //                    println(longKey + " -> " + valueString)
          //                    assert(valueString.contains(stringValue))
          //                  }
          //                } catch {
          //                  case ex: Exception =>
          //                    println("Key not found 1:" + key.readLong())
          //                    ex.printStackTrace()
          //                    System.exit(0)
          //                }
          //        }
        }
      }
    }

    def pluralSegment(count: Int) = if (count == 1) "Segment" else "Segments"

    @tailrec
    def areTopLevelsEmpty(levelNumber: Int): Unit =
      map.levelMeter(levelNumber) match {
        case Some(meter) if map.levelMeter(levelNumber + 1).nonEmpty =>
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
          val segmentsCount = map.levelMeter(levelNumber).map(_.segmentsCount) getOrElse -1
          println(s"Compaction completed. Level ${levelNumber + 1} contains all $segmentsCount ${pluralSegment(segmentsCount)}.\n")
      }

    println("Waiting for compaction to complete before executing after compaction benchmark.")
    areTopLevelsEmpty(1)

    if (forwardIteration)
      Benchmark("Forward iteration benchmark after compaction") {
        map
          .stream
          .foreach { case (_, _) => }
          .materialize
      }
    else if (reverseIteration)
      Benchmark("Reverse iteration benchmark after compaction") {
        map
          .reverse
          .stream
          .foreach { case (_, _) => }
          .materialize
      }
    else {
      println(s"mapsCount: ${map.levelZeroMeter.mapsCount}")
      val readKeys = if (randomRead) shuffledKeys else keys
      Benchmark("Read benchmark after compaction") {
        readKeys foreach {
          key =>
            //            try {
            map.get(key)
          //              val value = map.get(key).get.get
          //              val longKey = key.readLong()
          //              if (longKey % 10000 == 0)
          //                println(longKey + " -> " + value.map(_.readString()))
          //            } catch {
          //              case ex: Exception =>
          //                println("Key not found 2:" + key.readLong())
          //                ex.printStackTrace()
          //                System.exit(0)
          //            }
        }
      }
    }
  }
}
