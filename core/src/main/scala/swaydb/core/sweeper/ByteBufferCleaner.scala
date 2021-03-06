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

package swaydb.core.sweeper

import java.lang.invoke.{MethodHandle, MethodHandles, MethodType}
import java.nio.file.Path
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.IO.ExceptionHandler
import swaydb.core.io.file.ForceSaveApplier
import swaydb.data.config.ForceSave

private[core] case object ByteBufferCleaner extends LazyLogging {

  object Cleaner {
    def apply(handle: MethodHandle): Cleaner =
      new Cleaner(handle)
  }

  class Cleaner(handle: MethodHandle) {
    def clean(buffer: MappedByteBuffer,
              path: Path,
              forced: AtomicBoolean,
              forceSave: ForceSave.MMAPFiles)(implicit forceSaveApplier: ForceSaveApplier): Unit = {
      forceSaveApplier.beforeClean(
        path = path,
        buffer = buffer,
        forced = forced,
        forceSave = forceSave
      )

      handle.invoke(buffer)
    }
  }

  private def java9Cleaner(): MethodHandle = {
    val unsafeClass = Class.forName("sun.misc.Unsafe")
    val theUnsafe = unsafeClass.getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)

    MethodHandles
      .lookup
      .findVirtual(unsafeClass, "invokeCleaner", MethodType.methodType(classOf[Unit], classOf[ByteBuffer]))
      .bindTo(theUnsafe.get(null))
  }

  private def java8Cleaner(): MethodHandle = {
    val directByteBuffer = Class.forName("java.nio.DirectByteBuffer")
    val cleanerMethod = directByteBuffer.getDeclaredMethod("cleaner")
    cleanerMethod.setAccessible(true)
    val cleaner = MethodHandles.lookup.unreflect(cleanerMethod)

    val cleanerClass = Class.forName("sun.misc.Cleaner")
    val cleanMethod = cleanerClass.getDeclaredMethod("clean")
    cleanerMethod.setAccessible(true)
    val clean = MethodHandles.lookup.unreflect(cleanMethod)
    val cleanDroppedArgument = MethodHandles.dropArguments(clean, 1, directByteBuffer)

    MethodHandles.foldArguments(cleanDroppedArgument, cleaner)
  }

  def initialiseCleaner[E](buffer: MappedByteBuffer,
                           path: Path,
                           forced: AtomicBoolean,
                           forceSave: ForceSave.MMAPFiles)(implicit exceptionHandler: ExceptionHandler[E],
                                                           forceSaveApplier: ForceSaveApplier): IO[E, Cleaner] =
    IO {
      forceSaveApplier.beforeClean(
        path = path,
        buffer = buffer,
        forced = forced,
        forceSave = forceSave
      )

      val method = java9Cleaner()
      method.invoke(buffer)
      logger.info(s"Initialised Java 9 ${this.productPrefix}.")
      Cleaner(method)
    } orElse {
      IO {
        val method = java8Cleaner()
        method.invoke(buffer)
        logger.info(s"Initialised Java 8 ${this.productPrefix}.")
        Cleaner(method)
      }
    }
}
