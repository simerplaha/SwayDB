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
import swaydb.serializers.Serializer
/**
 * All Primary Keys for [[Table]]s.
 */
sealed trait PrimaryKey

object PrimaryKey {

  sealed trait UserPrimaryKeys extends PrimaryKey
  case class Email(email: String) extends UserPrimaryKeys
  case class Activity(id: Int) extends UserPrimaryKeys

  sealed trait ProductPrimaryKey extends PrimaryKey
  case class SKU(number: Int) extends ProductPrimaryKey
  case class Order(id: Int) extends ProductPrimaryKey

  implicit val serializer = new Serializer[PrimaryKey] {
    override def write(data: PrimaryKey): Slice[Byte] =
      Slice(Pickle.intoBytes(data).array())

    override def read(slice: Slice[Byte]): PrimaryKey =
      Unpickle[PrimaryKey].fromBytes(slice.toByteBufferWrap)
  }
}
