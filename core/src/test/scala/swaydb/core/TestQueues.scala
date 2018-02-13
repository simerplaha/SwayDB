/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import scala.concurrent.duration._
import swaydb.data.util.StorageUnits._

object TestQueues {

  implicit val level0PushDownPool = TestExecutionContext.executionContext

  val keyValueLimiter = LimitQueues.keyValueLimiter(10.mb, 5.seconds)
  val fileOpenLimiter = LimitQueues.segmentOpenLimiter(100, 5.seconds)

}
