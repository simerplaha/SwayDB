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

import java.util.concurrent.ConcurrentLinkedQueue

import swaydb.core.io.file.DBFile
import swaydb.core.queue.KeyValueLimiter
import swaydb.data.util.StorageUnits._

import scala.concurrent.duration._

object TestLimitQueues {

  implicit val level0PushDownPool = TestExecutionContext.executionContext

  val queue = new ConcurrentLinkedQueue[DBFile]()
  @volatile var queueSize = queue.size()

  val keyValueLimiter = KeyValueLimiter(10.mb, 5.seconds)

  val fileOpenLimiter: DBFile => Unit =
    file => {
      queue.add(file)
      queueSize += 1
      if (queueSize > 1000) {
        queue.poll().close
      }
    }
}
