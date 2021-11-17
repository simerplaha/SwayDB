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

package swaydb.core.sweeper

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.IO.ExceptionHandler
import swaydb.core.file.ForceSaveApplier
import swaydb.config.ForceSave

import java.lang.invoke.{MethodHandle, MethodHandles, MethodType}
import java.nio.file.Path
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.atomic.AtomicBoolean

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
