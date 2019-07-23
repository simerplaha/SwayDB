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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core

import java.util.concurrent.atomic.AtomicLong

import swaydb.IO
import swaydb.core.data.Time
import swaydb.core.map.timer.Timer
import swaydb.data.slice.Slice
import swaydb.macros.SealedList

import scala.util.Random

sealed trait TestTimer extends Timer

object TestTimer {

  def single(time: Time): TestTimer =
    new TestTimer {
      override def next: Time = time
      override def close: IO[IO.Error, Unit] = IO.unit
    }

  case class Incremental(startTime: Long = 0) extends TestTimer {
    val timer = new AtomicLong(startTime)

    override def next: Time =
      Time(timer.incrementAndGet())

    override def close: IO[IO.Error, Unit] =
      IO.unit
  }

  object IncrementalRandom extends TestTimer {
    val startTime: Long = 0
    private val timer = new AtomicLong(startTime)

    override def next: Time =
      if (Random.nextBoolean())
        Time(timer.incrementAndGet())
      else
        Time.empty

    override def close: IO[IO.Error, Unit] =
      IO.unit
  }

  case class Decremental(startTime: Long = 100) extends TestTimer {
    val timer = new AtomicLong(startTime)

    override def next: Time =
      Time(timer.decrementAndGet())

    override def close: IO[IO.Error, Unit] =
      IO.unit
  }

  object DecrementalRandom extends TestTimer {
    val startTime: Long = 100
    private val timer = new AtomicLong(startTime)

    override def next: Time =
      if (Random.nextBoolean())
        Time(timer.decrementAndGet())
      else
        Time.empty

    override def close: IO[IO.Error, Unit] =
      IO.unit
  }

  object Empty extends TestTimer {
    val startTime: Long = 0

    override val next: Time =
      Time(Slice.emptyBytes)

    override def close: IO[IO.Error, Unit] =
      IO.unit
  }

  val all =
    SealedList.list[TestTimer]

  def random: TestTimer =
    Random.shuffle(all).head

  def randomNonEmpty: TestTimer =
    Random.shuffle(Seq(Incremental(), Decremental())).head
}
