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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.api

import org.scalatest.exceptions.TestFailedException
import swaydb.IO.ApiIO
import swaydb.IOValues._
import swaydb._
import swaydb.core.CommonAssertions.eitherOne
import swaydb.core.{TestBase, TestExecutionContext}
import swaydb.data.RunThis._
import swaydb.data.slice.Slice
import swaydb.multimap.MultiKey

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random
import swaydb.data.slice.Slice

trait TestBaseEmbedded extends TestBase {

  val keyValueCount: Int

  def printMap[BAG[_]](root: MultiMap[_, _, _, _, BAG]): Unit = {
    root.innerMap.toBag[Bag.Less].stream.materialize.foreach {
      map =>
        println(map)
        map._1 match {
          case MultiKey.End(_) => println //new line
          case _ =>
        }
    }

    println("-" * 100)
  }

  /**
   * Randomly adds child Maps to [[MultiMap]] and returns the last added Map.
   */
  def generateRandomNestedMaps(root: MultiMap[Int, Int, String, Nothing, IO.ApiIO]): MultiMap[Int, Int, String, Nothing, ApiIO] = {
    val range = 1 to Random.nextInt(100)

    val sub =
      range.foldLeft(root) {
        case (root, id) =>
          val sub =
            if (Random.nextBoolean())
              root.schema.child(id).value
            else
              root

          if (Random.nextBoolean())
            root
          else
            sub
      }

    sub
  }

  def doAssertEmpty[V](db: SetMapT[Int, V, IO.ApiIO]) =
    (1 to keyValueCount) foreach {
      i =>
        try
          db.get(i).right.value shouldBe empty
        catch {
          case _: TestFailedException =>
            //if it's not empty then check if the expiration was going to occur in near millisecond time.
            db.expiration(i).value match {
              case Some(deadline) =>
                //print out for debugging
                //                import swaydb.data.util.FiniteDurations._
                //                println("Time-left: " + deadline.timeLeft.asString)

                //if timeLeft is false then read again should return empty now
                deadline.hasTimeLeft() shouldBe false
                db.get(i).right.value shouldBe empty

              case None =>
                //if expiration is none then the key-value should return empty.
                db.get(i).right.value shouldBe empty
            }
        }
    }

  def pluralSegment(count: Int) = if (count == 1) "Segment" else "Segments"

  //recursively go through all levels and assert they do no have any Segments.
  //Note: Could change this test to use Future with delays instead of blocking but the blocking code is probably more easier to read.

  def assertLevelsAreEmpty(db: SetMapT[Int, String, IO.ApiIO], submitUpdates: Boolean) = {
    println("Checking levels are empty.")

    @tailrec
    def checkEmpty(levelNumber: Int, expectedLastLevelEmpty: Boolean): Unit = {
      db.levelMeter(levelNumber) match {
        case Some(meter) if db.levelMeter(levelNumber + 1).nonEmpty => //is not the last Level. Check if this level contains no Segments.
          //          db.isEmpty shouldBe true //isEmpty will always return true since all key-values were removed.
          if (meter.segmentsCount == 0) { //this Level is empty, jump to next Level.
            println(s"Level $levelNumber is empty.")
            checkEmpty(levelNumber + 1, expectedLastLevelEmpty)
          } else {
            val interval = (levelNumber * 3).seconds //Level is not empty, try again with delay.
            println(s"Level $levelNumber contains ${meter.segmentsCount} ${pluralSegment(meter.segmentsCount)}. Will check again after $interval.")
            sleep(interval)
            checkEmpty(levelNumber, expectedLastLevelEmpty)
          }
        case _ => //is the last Level which will contains Segments.
          if (!expectedLastLevelEmpty) {
            if (submitUpdates) {
              println(s"Level $levelNumber. Submitting updated to trigger remove.")
              (1 to 500000) foreach { //submit multiple update range key-values so that a map gets submitted for compaction and to trigger merge on copied Segments in last Level.
                i =>
                  db match {
                    case map @ Map(core) =>
                      map.update(1, 1000000, value = "just triggering update to assert remove").right.value

                    case SetMap(set) =>
                      set.core.update(fromKey = Slice.writeInt[Byte](1), to = Slice.writeInt[Byte](1000000), value = Slice.Null).right.value
                  }

                  if (i == 100000) sleep(2.seconds)
              }
            }
            //update submitted, now expect the merge to unsafeGet triggered on the Segments in the last Level and Compaction to remove all key-values.
          }

          //          db.isEmpty shouldBe true //isEmpty will always return true since all key-values were removed.

          val segmentsCount = db.levelMeter(levelNumber).map(_.segmentsCount) getOrElse -1
          if (segmentsCount != 0) {
            println(s"Level $levelNumber contains $segmentsCount ${pluralSegment(segmentsCount)}. Will check again after 8.seconds.")
            sleep(8.seconds)
            checkEmpty(levelNumber, true)
          } else {
            println(s"Compaction completed. Level $levelNumber is empty.\n")
          }
      }
    }

    implicit val ec = TestExecutionContext.executionContext
    //this test might take a while depending on the Compaction speed but it should not run for too long hence the timeout.
    Future(checkEmpty(1, false)).await(10.minutes)
  }

  def doExpire(from: Int, to: Int, deadline: Deadline, db: SetMapT[Int, String, IO.ApiIO]): Unit =
    db match {
      case db @ Map(_) =>
        eitherOne(
          left = (from to to) foreach (i => db.expire(i, deadline).right.value),
          right = db.expire(from, to, deadline).right.value
        )

      case _ =>
        (from to to) foreach (i => db.expire(i, deadline).right.value)
    }

  def doRemove(from: Int, to: Int, db: SetMapT[Int, String, IO.ApiIO]): Unit =
    db match {
      case db @ Map(_) =>
        eitherOne(
          left = (from to to) foreach (i => db.remove(i).right.value),
          right = db.remove(from = from, to = to).right.value
        )

      case _ =>
        (from to to) foreach (i => db.remove(i).right.value)
    }

  def doUpdateOrIgnore(from: Int, to: Int, value: String, db: SetMapT[Int, String, IO.ApiIO]): Unit =
    db match {
      case db @ Map(_) =>
        eitherOne(
          left = (from to to) foreach (i => db.update(i, value = value).right.value),
          right = db.update(from, to, value = value).right.value
        )

      case _ =>
        ()
    }
}
