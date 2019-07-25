/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.data

import java.io.FileNotFoundException
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.nio.file.Paths

import swaydb.data.io.Core
import swaydb.data.io.Core.Exception.NullMappedByteBuffer

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random

object Base {

  implicit class AwaitImplicits[T](f: Future[T]) {
    def await: T =
      Await.result(f, 10.seconds)
  }

  def busyErrors(busyBoolean: Reserve[Unit] = Reserve()): List[Core.Error.Reserved] =
    List(
      Core.Error.OpeningFile(Paths.get("/some/path"), busyBoolean),
      Core.Error.NoSuchFile(Some(Paths.get("/some/path")), None),
      Core.Error.FileNotFound(new FileNotFoundException("")),
      Core.Error.AsynchronousClose(new AsynchronousCloseException()),
      Core.Error.ClosedChannel(new ClosedChannelException),
      Core.Error.NullMappedByteBuffer(NullMappedByteBuffer(new NullPointerException, busyBoolean)),
      Core.Error.DecompressingIndex(busyBoolean),
      Core.Error.DecompressingValues(busyBoolean),
      Core.Error.ReadingHeader(busyBoolean),
      Core.Error.ReservedValue(busyBoolean)
    )

  def randomBusyException(busyBoolean: Reserve[Unit] = Reserve()): Core.Error.Reserved =
    Random.shuffle(busyErrors(busyBoolean)).head
}
