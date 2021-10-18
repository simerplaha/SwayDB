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
