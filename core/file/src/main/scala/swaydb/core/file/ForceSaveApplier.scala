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

package swaydb.core.file

import com.typesafe.scalalogging.LazyLogging
import swaydb.Benchmark
import swaydb.config.ForceSave

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

  def beforeCopy(file: CoreFile,
                 toPath: Path,
                 forceSave: ForceSave): Unit

  def beforeClose[F <: CoreFileType](file: F,
                                     forceSave: ForceSave): Unit
}

object ForceSaveApplier extends LazyLogging {

  object Off extends ForceSaveApplier {
    override def beforeClean(path: Path, buffer: MappedByteBuffer, forced: AtomicBoolean, forceSave: ForceSave.MMAPFiles): Unit =
      logger.error(s"Disabled ForceSaveApplier beforeClean - $path", new Exception("Disabled ForceSaveApplier"))

    override def beforeCopy(file: CoreFile, toPath: Path, forceSave: ForceSave): Unit =
      logger.error(s"Disabled ForceSaveApplier beforeCopy - ${file.path} - toPath - $toPath", new Exception("Disabled ForceSaveApplier"))

    override def beforeClose[F <: CoreFileType](file: F, forceSave: ForceSave): Unit =
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
    def beforeCopy(file: CoreFile,
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
    def beforeClose[F <: CoreFileType](file: F,
                                       forceSave: ForceSave): Unit =
      if (forceSave.enabledBeforeClose)
        if (forceSave.logBenchmark)
          Benchmark(s"ForceSave before close: '${file.path}", useLazyLogging = true)(file.forceSave())
        else
          file.forceSave()
  }
}
