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

package swaydb.data.io

import java.io.FileNotFoundException
import java.nio.ReadOnlyBufferException
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException, OverlappingFileLockException}
import java.nio.file.{NoSuchFileException, Path}

import swaydb.ErrorHandler
import swaydb.data.Reserve
import swaydb.data.slice.Slice

object Core {

}
