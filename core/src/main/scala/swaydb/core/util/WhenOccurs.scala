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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.util

import com.typesafe.scalalogging.LazyLogging

object WhenOccurs {

  @inline def apply(mod: Int)(f: Int => Unit): WhenOccurs =
    new WhenOccurs(mod)(f)

}

/**
 * Run something if it's occurs [[interval]] number of times.
 */
class WhenOccurs(interval: Int)(f: Int => Unit) extends LazyLogging {

  @volatile private var count = 0

  def occurs() = {
    //initial count is 0 so it's always invoked initially.
    if (count % interval == 0)
      try
        f(count + 1) //it's occurred at least once.
      catch {
        case exception: Exception =>
          logger.error("Failed apply occurrence", exception)
      }

    count += 1
  }

}
