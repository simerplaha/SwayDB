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

package swaydb.core.log

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.config.MMAP
import swaydb.utils.IDGenerator

import java.nio.file.Path

private[core] object Log extends LazyLogging {

  /**
   * Used to assign unique numbers for each [[Log]] instance.
   */
  private[log] val uniqueFileNumberGenerator = IDGenerator()

}

private[core] trait Log[K, V, C <: LogCache[K, V]] {

  def cache: C

  def mmap: MMAP.Log

  val fileSize: Int

  def writeSync(logEntry: LogEntry[K, V]): Boolean

  def writeNoSync(logEntry: LogEntry[K, V]): Boolean

  def writeSafe[E: IO.ExceptionHandler](logEntry: LogEntry[K, V]): IO[E, Boolean] =
    IO[E, Boolean](writeSync(logEntry))

  def delete(): Unit

  def exists(): Boolean

  def pathOption: Option[Path]

  def close(): Unit

  def uniqueFileNumber: Long

  override def equals(obj: Any): Boolean =
    obj match {
      case other: Log[_, _, _] =>
        other.uniqueFileNumber == this.uniqueFileNumber

      case _ =>
        false
    }

  override def hashCode(): Int =
    uniqueFileNumber.hashCode()

  override def toString: String =
    this.pathOption match {
      case Some(value) =>
        s"${this.getClass.getSimpleName} - Path: ${value.toString} - FileNumber: ${this.uniqueFileNumber}"

      case None =>
        s"${this.getClass.getSimpleName} - FileNumber: ${this.uniqueFileNumber}"
    }
}
