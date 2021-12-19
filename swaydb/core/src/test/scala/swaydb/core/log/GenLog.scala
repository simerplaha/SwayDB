/*
 * Copyright (c) 19/12/21, 5:47 pm Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package swaydb.core.log

import swaydb.config.{Atomic, GenForceSave, MMAP, OptimiseWrites}
import swaydb.config.CoreConfigTestKit._
import swaydb.core.{CoreSpecType, CoreTestSweeper}
import swaydb.core.CoreTestSweeper._
import swaydb.core.level.zero.LevelZero.LevelZeroLog
import swaydb.core.level.zero.LevelZeroLogCache
import swaydb.core.log.LogTestKit.genLogFile
import swaydb.core.segment.data.Memory
import swaydb.effect.IOValues._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.utils.OperatingSystem
import swaydb.utils.StorageUnits._

object GenLog {
  def apply(keyValues: Slice[Memory],
            fileSize: Int = 4.mb,
            flushOnOverflow: Boolean = false,
            mmap: MMAP.Log = MMAP.On(OperatingSystem.isWindows(), GenForceSave.mmap()))(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                        timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                                        sweeper: CoreTestSweeper,
                                                                                        coreSpecType: CoreSpecType): LevelZeroLog = {
    import swaydb.core.log.serialiser.MemoryLogEntryWriter._
    import sweeper._

    implicit val optimiseWrites: OptimiseWrites = OptimiseWrites.random
    implicit val atomic: Atomic = Atomic.random

    val testLog =
      if (coreSpecType.isMemory)
        Log.memory[Slice[Byte], Memory, LevelZeroLogCache](
          fileSize = fileSize,
          flushOnOverflow = flushOnOverflow
        )
      else
        Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
          folder = genLogFile(),
          mmap = mmap,
          flushOnOverflow = flushOnOverflow,
          fileSize = fileSize
        ).runRandomIO.get

    keyValues foreach {
      keyValue =>
        testLog.writeSync(LogEntry.Put(keyValue.key, keyValue))
    }

    testLog.sweep()
  }
}
