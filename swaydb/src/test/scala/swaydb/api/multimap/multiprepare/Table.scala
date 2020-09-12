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

package swaydb.api.multimap.multiprepare

import boopickle.Default.{Pickle, Unpickle, _}
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice.Sliced
import swaydb.serializers.Serializer

/**
 * All Tables for [[MultiMapMultiPrepareSpec]].
 */
sealed trait Table
object Table {
  sealed trait UserTables extends Table
  sealed trait User extends UserTables
  case object User extends User
  sealed trait Activity extends UserTables
  case object Activity extends Activity

  sealed trait ProductTables extends Table
  sealed trait Product extends ProductTables
  case object Product extends Product
  sealed trait Order extends ProductTables
  case object Order extends Order

  implicit val serializer = new Serializer[Table] {
    override def write(data: Table): Sliced[Byte] =
      Slice(Pickle.intoBytes(data).array())

    override def read(data: Sliced[Byte]): Table =
      Unpickle[Table].fromBytes(data.toByteBufferWrap)
  }
}
