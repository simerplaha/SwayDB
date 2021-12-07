/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core

import swaydb.core.log.timer.Timer
import swaydb.core.segment.data.Time
import swaydb.macros.Sealed
import swaydb.slice.Slice

import java.util.concurrent.atomic.AtomicLong
import scala.util.Random

sealed trait TestTimer extends Timer

object TestTimer {

  def single(time: Time): TestTimer =
    new TestTimer {
      override val isEmptyTimer: Boolean = false
      override def next: Time = time
      override def close(): Unit = ()
    }

  case class Incremental(startTime: Long = 0) extends TestTimer {
    override val isEmptyTimer: Boolean = false

    val timer = new AtomicLong(startTime)

    override def next: Time =
      Time(timer.incrementAndGet())

    override def close(): Unit = ()

  }

  object IncrementalRandom extends TestTimer {
    val startTime: Long = 0
    private val timer = new AtomicLong(startTime)
    override val isEmptyTimer: Boolean = false

    override def next: Time =
      if (Random.nextBoolean())
        Time(timer.incrementAndGet())
      else
        Time.empty

    override def close(): Unit = ()
  }

  case class Decremental(startTime: Long = Int.MaxValue) extends TestTimer {
    val timer = new AtomicLong(startTime)
    override val isEmptyTimer: Boolean = false

    override def next: Time =
      Time(timer.decrementAndGet())

    override def close(): Unit = ()
  }

  object DecrementalRandom extends TestTimer {
    val startTime: Long = Int.MaxValue
    private val timer = new AtomicLong(startTime)
    override val isEmptyTimer: Boolean = false

    override def next: Time =
      if (Random.nextBoolean())
        Time(timer.decrementAndGet())
      else
        Time.empty

    override def close(): Unit = ()
  }

  object Empty extends TestTimer {
    val startTime: Long = 0
    override val isEmptyTimer: Boolean = true

    override val next: Time =
      Time(Slice.emptyBytes)

    override def close(): Unit = ()
  }

  def all =
    Sealed.list[TestTimer]

  def random: TestTimer =
    Random.shuffle(all).head

  def randomNonEmpty: TestTimer =
    Random.shuffle(Seq(Incremental(), Decremental())).head
}
