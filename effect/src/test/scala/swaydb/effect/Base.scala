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

package swaydb.effect

import swaydb.Exception.NullMappedByteBuffer

import java.io.FileNotFoundException
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.nio.file.Paths
import scala.util.Random

object Base {

  def busyErrors(busyBoolean: Reserve[Unit] = Reserve.free(name = "busyError")): List[swaydb.Error.Recoverable] =
    List(
      swaydb.Error.OpeningFile(Paths.get("/some/path"), busyBoolean),
      swaydb.Error.NoSuchFile(Some(Paths.get("/some/path")), None),
      swaydb.Error.FileNotFound(new FileNotFoundException("")),
      swaydb.Error.AsynchronousClose(new AsynchronousCloseException()),
      swaydb.Error.ClosedChannel(new ClosedChannelException),
      swaydb.Error.NullMappedByteBuffer(NullMappedByteBuffer(new NullPointerException, busyBoolean)),
      swaydb.Error.ReservedResource(busyBoolean)
    )

  def randomBusyError(busyBoolean: Reserve[Unit] = Reserve.free(name = "randomBusyError")): swaydb.Error.Recoverable =
    Random.shuffle(busyErrors(busyBoolean)).head

}
