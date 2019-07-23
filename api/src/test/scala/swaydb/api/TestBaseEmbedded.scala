/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

import swaydb.{IO, _}
import swaydb.core.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.data.io.Tag.SIO

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration._

trait TestBaseEmbedded extends TestBase {

  val keyValueCount: Int

  def doAssertEmpty[V](db: Map[Int, V, SIO]) =
    (1 to keyValueCount) foreach {
      i =>
        db.expiration(i).value match {
          case Some(value) =>
            value.hasTimeLeft() shouldBe false

          case None =>
        }
        db.get(i).value shouldBe empty
    }

  def pluralSegment(count: Int) = if (count == 1) "Segment" else "Segments"

  //recursively go through all levels and assert they do no have any Segments.
  //Note: Could change this test to use Future with delays instead of blocking but the blocking code is probably more easier to read.

  def assertLevelsAreEmpty(db: Map[Int, String, SIO], submitUpdates: Boolean) = {
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
                  db.update(1, 1000000, value = "just triggering update to assert remove").value
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
    //this test might take a while depending on the Compaction speed but it should not run for too long hence the timeout.
    Future(checkEmpty(1, false)).await(10.minutes)
  }
}
