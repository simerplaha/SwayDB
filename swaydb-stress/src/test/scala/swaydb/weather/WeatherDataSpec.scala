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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.weather

import com.typesafe.scalalogging.LazyLogging
import swaydb.IOValues._
import swaydb.data.RunThis._
import swaydb.core.TestData._
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext}
import swaydb.data.accelerate.Accelerator
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.{Bag, IO}

import scala.concurrent.Future
import scala.concurrent.duration._
import TestCaseSweeper._

class Memory_WeatherDataSpec extends WeatherDataSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper) = swaydb.memory.Map[Int, WeatherData, Nothing, IO.ApiIO]().get.sweep()
}

class Memory_MultiMap_WeatherDataSpec extends WeatherDataSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper) = swaydb.memory.MultiMap_EAP[Int, Int, WeatherData, Nothing, IO.ApiIO]().get.sweep()
}

class Persistent_WeatherDataSpec extends WeatherDataSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper) =
    swaydb.persistent.Map[Int, WeatherData, Nothing, IO.ApiIO](randomDir, memoryCache = swaydb.persistent.DefaultConfigs.memoryCache.copy(cacheCapacity = 10.mb), acceleration = Accelerator.brake()).get.sweep()
}

class Persistent_MultiMap_WeatherDataSpec extends WeatherDataSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper) =
    swaydb.persistent.MultiMap_EAP[Int, Int, WeatherData, Nothing, IO.ApiIO](randomDir, memoryCache = swaydb.persistent.DefaultConfigs.memoryCache.copy(cacheCapacity = 10.mb), acceleration = Accelerator.brake()).get.sweep()
}

class Persistent_SetMap_WeatherDataSpec extends WeatherDataSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper) =
    swaydb.persistent.SetMap[Int, WeatherData, Nothing, IO.ApiIO](randomDir, memoryCache = swaydb.persistent.DefaultConfigs.memoryCache.copy(cacheCapacity = 10.mb), acceleration = Accelerator.brake()).get.sweep()
}

class Memory_SetMap_WeatherDataSpec extends WeatherDataSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper) =
    swaydb.memory.SetMap[Int, WeatherData, Nothing, IO.ApiIO]().get.sweep()
}

class EventuallyPersistent_WeatherDataSpec extends WeatherDataSpec {
  //  override def newDB()(implicit sweeper: TestCaseSweeper) = swaydb.eventually.persistent.Map[Int, WeatherData, Nothing, IO.ApiIO](randomDir, maxOpenSegments = 10, memoryCacheSize = 10.mb, maxMemoryLevelSize = 500.mb).get
  override def newDB()(implicit sweeper: TestCaseSweeper) =
    swaydb.eventually.persistent.Map[Int, WeatherData, Nothing, IO.ApiIO](randomDir).get.sweep()
}

sealed trait WeatherDataSpec extends TestBase with LazyLogging {

  def newDB()(implicit sweeper: TestCaseSweeper): swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]

  implicit val ec = TestExecutionContext.executionContext
  implicit val bag = Bag.apiIO

  val keyValueCount = 1000000

  def doPut(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) =
    (1 to keyValueCount) foreach {
      key =>
        if (key % 10000 == 0)
          println(s"Put: Key = $key.")
        db.put(key, WeatherData(Water(key, Direction.East, key), Wind(key, Direction.West, key, key), Location.Sydney)).get
    }

  //batch writes all key-values inBatchesOf
  def doBatch(inBatchesOf: Int)(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) =
    (1 to keyValueCount) foreach {
      key =>
        if (key % 10000 == 0)
          println(s"Batch: Key = $key.")

        if (key % inBatchesOf == 0) {
          val keyValues =
            (key - (inBatchesOf - 1) to key) map {
              key =>
                (key, WeatherData(Water(key, Direction.East, key), Wind(key, Direction.West, key, key), Location.Sydney))
            }
          db.put(keyValues).get
        }
    }

  def doBatchRandom(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) = {
    val from = randomNextInt(keyValueCount) min (keyValueCount - 100)
    val keyValues =
      (from to from + 10) map {
        key =>
          if (key % 10000 == 0)
            println(s"Batch random: Key = $key.")
          (key, WeatherData(Water(key, Direction.East, key), Wind(key, Direction.West, key, key), Location.Sydney))
      }
    db.put(keyValues).get
  }

  def doGet(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) =
    (1 to keyValueCount) foreach {
      key =>
        val value = db.get(key).get
        if (key % 10000 == 0)
          println(s"Get: Key = $key. Value = $value")
        value should contain(WeatherData(Water(key, Direction.East, key), Wind(key, Direction.West, key, key), Location.Sydney))
    }

  def doFoldLeft(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) =
    db
      .stream
      .foldLeft(Option.empty[Int]) {
        case (previousKey, (key, value)) =>
          previousKey map {
            previousKey =>
              if (key % 10000 == 0)
                println(s"FoldLeft: previousKey: $previousKey == key = $key. Value = $value")
              key shouldBe (previousKey + 1)
              value shouldBe WeatherData(Water(key, Direction.East, key), Wind(key, Direction.West, key, key), Location.Sydney)
              key
          } orElse Some(key)
      }.get should contain(keyValueCount)

  def doForeach(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) =
    db
      .stream
      .foreach {
        case (key, value) =>
          if (key % 10000 == 0)
            println(s"Foreach: key = $key. Value = $value")
          value shouldBe WeatherData(Water(key, Direction.East, key), Wind(key, Direction.West, key, key), Location.Sydney)
      }

  def doTakeWhile(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) = {
    //start from anywhere but take at least 100 keyValues
    val startFrom = randomNextInt(keyValueCount) min (keyValueCount - 100)
    val took =
      db
        .from(startFrom)
        .stream
        .takeWhile {
          case (key, _) =>
            if (key % 10000 == 0)
              println(s"doTakeWhile: key = $key")
            key < (startFrom + 100)
        }

    took.materialize.runRandomIO.right.value should have size 100
    took.headOption.get.get._1 shouldBe startFrom
    took.lastOption.get.get._1 shouldBe (startFrom + 99)
  }

  def doHeadAndLast(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) = {
    val (headKey, headValue) = db.headOption.get.get
    headKey shouldBe 1
    headValue shouldBe WeatherData(Water(headKey, Direction.East, headKey), Wind(headKey, Direction.West, headKey, headKey), Location.Sydney)
    println(s"headKey: $headKey -> headValue: $headValue")

    val (lastKey, lastValue) = db.lastOption.get.get
    lastKey shouldBe keyValueCount
    lastValue shouldBe WeatherData(Water(lastKey, Direction.East, lastKey), Wind(lastKey, Direction.West, lastKey, lastKey), Location.Sydney)
    println(s"lastKey: $lastKey -> lastValue: $lastValue")
  }

  def doMapRight(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) = {
    //start from anywhere but take at least 100 keyValues
    val startFrom = randomNextInt(keyValueCount) min (keyValueCount - 100)
    val took =
      db
        .from(startFrom)
        .reverse
        .stream
        .takeWhile {
          case (key, _) =>
            key > startFrom - 100
        }
        .map {
          case (key, _) =>
            if (key % 10000 == 0)
              println(s"mapRight: key = $key")
            key
        }.materialize.runRandomIO.right.value

    val expected = (0 until 100) map (startFrom - _)
    took should have size 100
    took shouldBe expected
  }

  def doTake(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) = {
    db
      .stream
      .take(100)
      .map {
        case (key, value) =>
          if (key % 10000 == 0)
            println(s"take: key = $key")
          key
      }.materialize.runRandomIO.right.value shouldBe (1 to 100)

    db
      .fromOrAfter(0)
      .stream
      .take(100)
      .map {
        case (key, value) =>
          if (key % 10000 == 0)
            println(s"take: key = $key")
          key
      }.materialize.runRandomIO.right.value shouldBe (1 to 100)
  }

  def doDrop(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) =
    db
      .from(keyValueCount - 200)
      .stream
      .drop(100)
      .map {
        case (key, value) =>
          if (key % 10000 == 0)
            println(s"take: key = $key")
          key
      }.materialize.runRandomIO.right.value shouldBe (keyValueCount - 100 to keyValueCount)

  def doTakeRight(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) =
    db
      .fromOrBefore(Int.MaxValue)
      .reverse
      .stream
      .take(100)
      .map {
        case (key, value) =>
          if (key % 10000 == 0)
            println(s"take: key = $key")
          key
      }.materialize.runRandomIO.right.value shouldBe (keyValueCount - 99 to keyValueCount).reverse

  def doCount(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) =
    db.stream.size.get should be >= keyValueCount

  def doDeleteAll(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) = {
    (1 to keyValueCount / 2) foreach {
      key =>
        if (key % 10000 == 0)
          println(s"Remove: Key = $key.")
        db.remove(key).get
    }

    db.remove(keyValueCount / 2, keyValueCount + 1).get
  }

  def putRequest(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) =
    Future(doPut)

  def batchRandomRequest(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) =
    Future(doBatchRandom)

  def batchRequest(inBatchesOf: Int = 100)(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]) =
    Future(doBatch(inBatchesOf))

  def tryOrExit[F](f: => F) =
    try
      f
    catch {
      case ex: Exception =>
        ex.printStackTrace()
        System.exit(0)
    }

  def readRequests(implicit db: swaydb.SetMapT[Int, WeatherData, Nothing, IO.ApiIO]): Future[Seq[Any]] =
    Future.sequence(
      Seq(
        Future(tryOrExit(doForeach)),
        Future(tryOrExit(doGet)),
        Future(tryOrExit(doHeadAndLast)),
        Future(tryOrExit(doFoldLeft)),
        Future(tryOrExit(doTakeWhile)),
        Future(tryOrExit(doMapRight)),
        Future(tryOrExit(doTake)),
        Future(tryOrExit(doDrop)),
        Future(tryOrExit(doTakeRight)),
        Future(tryOrExit(doCount))
      )
    )

  "concurrently write 1 million weather data entries using BookPickle and read using multiple APIs concurrently" in {
    TestCaseSweeper {
      implicit sweeper =>
        implicit val db = newDB()
        //do initial put or batch (whichever one) to ensure that data exists for readRequests.
        //    doPut
        doBatch(inBatchesOf = 100000 min keyValueCount)
        putRequest runThis 4.times
        batchRandomRequest runThis 2.times
        batchRequest(inBatchesOf = 10000 min keyValueCount)
        //    Future {
        //      while (true) {
        //        println("db.level0Meter.mapsCount:     " + db.level0Meter.mapsCount)
        //        println("db.level1Meter.segmentsCount: " + db.level1Meter.segmentsCount)
        //        sleep(5.seconds)
        //      }
        //    }

        readRequests runThis 10.times await 1.hour
        println("************************* DONE *************************")
    }
  }
}
