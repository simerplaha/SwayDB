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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.api.multimap.multiprepare

import boopickle.Default.{Pickle, Unpickle, _}
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

/**
 * All row types for [[Table]]
 */
sealed trait Row
object Row {
  sealed trait UserRows extends Row
  case class User(name: String, address: String) extends UserRows
  case class Activity(string: String) extends UserRows

  sealed trait ProductRows extends Row
  case class Product(sku: Int) extends ProductRows
  case class Order(sku: Int, price: Double) extends ProductRows

  implicit val serializer = new Serializer[Row] {
    override def write(data: Row): Slice[Byte] =
      Slice(Pickle.intoBytes(data).array())

    override def read(slice: Slice[Byte]): Row =
      Unpickle[Row].fromBytes(slice.toByteBufferWrap)
  }
}
