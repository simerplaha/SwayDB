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

package swaydb.core

import java.util.concurrent.atomic.AtomicLong
import scala.util.Random
import swaydb.core.data.Time
import swaydb.data.slice.Slice
import swaydb.macros.SealedList
import swaydb.serializers.Default.LongSerializer
import swaydb.serializers._

sealed trait TestTimeGenerator {
  def nextTime: Time

  def startTime: Long
}

object TestTimeGenerator {

  case class Incremental(startTime: Long = 0) extends TestTimeGenerator {
    val timer = new AtomicLong(startTime)

    override def nextTime: Time =
      Time(timer.incrementAndGet())
  }

  object IncrementalRandom extends TestTimeGenerator {
    override val startTime: Long = 0
    private val timer = new AtomicLong(startTime)

    override def nextTime: Time =
      if (Random.nextBoolean())
        Time(timer.incrementAndGet())
      else
        Time.empty

  }

  case class Decremental(startTime: Long = 100) extends TestTimeGenerator {
    val timer = new AtomicLong(startTime)

    override def nextTime: Time =
      Time(timer.decrementAndGet())
  }

  object DecrementalRandom extends TestTimeGenerator {
    override val startTime: Long = 100
    private val timer = new AtomicLong(startTime)

    override def nextTime: Time =
      if (Random.nextBoolean())
        Time(timer.decrementAndGet())
      else
        Time.empty
  }

  object Empty extends TestTimeGenerator {
    override val startTime: Long = 0
    override val nextTime: Time =
      Time(Slice.emptyBytes)
  }

  val all =
    SealedList.list[TestTimeGenerator]

  def random: TestTimeGenerator =
    Random.shuffle(all).head

}
