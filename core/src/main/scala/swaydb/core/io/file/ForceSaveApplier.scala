/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.io.file

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.util.Benchmark
import swaydb.data.config.ForceSave

import java.nio.MappedByteBuffer
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Applies [[ForceSave]] configuration for applied forceSave to files.
 */
trait ForceSaveApplier {
  def beforeClean(path: Path,
                  buffer: MappedByteBuffer,
                  forced: AtomicBoolean,
                  forceSave: ForceSave.MMAPFiles): Unit

  def beforeCopy(file: DBFile,
                 toPath: Path,
                 forceSave: ForceSave): Unit

  def beforeClose[F <: DBFileType](file: F,
                                   forceSave: ForceSave): Unit
}

object ForceSaveApplier extends LazyLogging {

  object Off extends ForceSaveApplier {
    override def beforeClean(path: Path, buffer: MappedByteBuffer, forced: AtomicBoolean, forceSave: ForceSave.MMAPFiles): Unit =
      logger.error(s"Disabled ForceSaveApplier beforeClean - $path", new Exception("Disabled ForceSaveApplier"))

    override def beforeCopy(file: DBFile, toPath: Path, forceSave: ForceSave): Unit =
      logger.error(s"Disabled ForceSaveApplier beforeCopy - ${file.path} - toPath - $toPath", new Exception("Disabled ForceSaveApplier"))

    override def beforeClose[F <: DBFileType](file: F, forceSave: ForceSave): Unit =
      logger.error(s"Disabled ForceSaveApplier beforeClose - ${file.path}", new Exception("Disabled ForceSaveApplier"))
  }

  object On extends ForceSaveApplier {

    /**
     * Applies forceSave condition for before cleaning the file.
     *
     * @param path      Path to the file.
     * @param buffer    Buffer to clean
     * @param forced    AtomicBoolean indicates if force is already applied for this MappedByteBuffer.
     * @param forceSave [[ForceSave]] configuration
     */
    def beforeClean(path: Path,
                    buffer: MappedByteBuffer,
                    forced: AtomicBoolean,
                    forceSave: ForceSave.MMAPFiles): Unit =
      if (forceSave.enabledBeforeClean && forced.compareAndSet(false, true))
        try
          if (forceSave.logBenchmark)
            Benchmark(s"ForceSave before clean: '$path''", useLazyLogging = true)(buffer.force())
          else
            buffer.force()
        catch {
          case throwable: Throwable =>
            forced.set(false)
            logger.error("Unable to ForceSave before clean", throwable)
            throw throwable
        }

    /**
     * Applies forceSave condition for before copying the file.
     *
     * @param file      Path to the file.
     * @param toPath    Path of copy-to location.
     * @param forceSave [[ForceSave]] configuration.
     */
    def beforeCopy(file: DBFile,
                   toPath: Path,
                   forceSave: ForceSave): Unit =
      if (forceSave.enableBeforeCopy)
        if (forceSave.logBenchmark)
          Benchmark(s"ForceSave before copy from: '${file.path}' to: '$toPath'", useLazyLogging = true)(file.forceSave())
        else
          file.forceSave()

    /**
     * Applies forceSave condition for before closing the file.
     *
     * @param file      File to close
     * @param forceSave [[ForceSave]] configuration.
     */
    def beforeClose[F <: DBFileType](file: F,
                                     forceSave: ForceSave): Unit =
      if (forceSave.enabledBeforeClose)
        if (forceSave.logBenchmark)
          Benchmark(s"ForceSave before close: '${file.path}", useLazyLogging = true)(file.forceSave())
        else
          file.forceSave()
  }
}
