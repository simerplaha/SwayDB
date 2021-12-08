package swaydb.core.log

import swaydb.config.{Atomic, MMAP, OptimiseWrites}
import swaydb.core.level.zero.LevelZero.LevelZeroLog
import swaydb.core.level.zero.LevelZeroLogCache
import swaydb.core.segment.data.Memory
import swaydb.core.{ACoreSpec, TestSweeper, TestForceSave}
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.utils.OperatingSystem
import swaydb.utils.StorageUnits._
import swaydb.core.CoreTestData._
import swaydb.IOValues._
import TestSweeper._
import swaydb.effect.Effect

import java.nio.file.Path

trait ALogSpec extends ACoreSpec {

  def testLogFile: Path =
//    if (isMemorySpec)
//      randomIntDirectory.resolve(nextId.toString + ".map")
//    else
//      Effect.createDirectoriesIfAbsent(randomIntDirectory).resolve(nextId.toString + ".map")
  ???


  object TestLog {
    def apply(keyValues: Slice[Memory],
              fileSize: Int = 4.mb,
              path: Path = testLogFile,
              flushOnOverflow: Boolean = false,
              mmap: MMAP.Log = MMAP.On(OperatingSystem.isWindows, TestForceSave.mmap()))(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                         timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                                         sweeper: TestSweeper): LevelZeroLog = {
      import swaydb.core.log.serialiser.LevelZeroLogEntryWriter._
      import sweeper._

      implicit val optimiseWrites = OptimiseWrites.random
      implicit val atomic = Atomic.random

      val testLog =
        if (isMemorySpec)
          Log.memory[Slice[Byte], Memory, LevelZeroLogCache](
            fileSize = fileSize,
            flushOnOverflow = flushOnOverflow
          )
        else
          Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
            folder = path,
            mmap = mmap,
            flushOnOverflow = flushOnOverflow,
            fileSize = fileSize
          ).runRandomIO.right.value

      keyValues foreach {
        keyValue =>
          testLog.writeSync(LogEntry.Put(keyValue.key, keyValue))
      }

      testLog.sweep()
    }
  }

}
