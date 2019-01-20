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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.request

import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

private[swaydb] sealed trait Batch

private[swaydb] object Batch {

  private[swaydb] case class Put(key: Slice[Byte],
                                 value: Option[Slice[Byte]],
                                 expire: Option[Deadline]) extends Batch

  private[swaydb] case class Remove(key: Slice[Byte],
                                    expire: Option[Deadline]) extends Batch

  private[swaydb] case class Update(key: Slice[Byte],
                                    value: Option[Slice[Byte]]) extends Batch

  private[swaydb] case class UpdateRange(fromKey: Slice[Byte],
                                         toKey: Slice[Byte],
                                         value: Option[Slice[Byte]]) extends Batch

  private[swaydb] case class RemoveRange(fromKey: Slice[Byte],
                                         toKey: Slice[Byte],
                                         expire: Option[Deadline]) extends Batch
}