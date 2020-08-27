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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.io.file

import java.nio.MappedByteBuffer
import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.util.Benchmark
import swaydb.data.config.ForceSave

/**
 * Applies force save before
 */
object ForceSaveApplier extends LazyLogging {

  @inline def beforeClean(path: Path,
                          buffer: MappedByteBuffer,
                          forceSave: ForceSave.MMAPFiles): Unit =
    if (forceSave.enabledBeforeClean)
      if (forceSave.logBenchmark)
        Benchmark(s"ForceSave: $path", useLazyLogging = true)(buffer.force())
      else
        buffer.force()

  @inline def beforeCopy(file: DBFile,
                         toPath: Path,
                         forceSave: ForceSave): Unit =
    if (forceSave.enableBeforeCopy)
      if (forceSave.logBenchmark)
        Benchmark(s"ForceSave before copy from: '${file.path}' to: '$toPath'", useLazyLogging = true)(file.forceSave())
      else
        file.forceSave()

  @inline def beforeClose[F <: DBFileType](file: F,
                                           forceSave: ForceSave): Unit =
    if (forceSave.logBenchmark)
      Benchmark(s"ForceSave before close: '${file.path}", useLazyLogging = true)(file.forceSave())
    else
      file.forceSave()
}
