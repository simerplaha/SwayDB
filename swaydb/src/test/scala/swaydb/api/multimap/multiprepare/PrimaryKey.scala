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

package swaydb.api.multimap.multiprepare

import boopickle.Default.{Pickle, Unpickle, _}
import swaydb.serializers.Serializer
import swaydb.slice.Slice

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
      Slice.wrap(Pickle.intoBytes(data).array())

    override def read(slice: Slice[Byte]): PrimaryKey =
      Unpickle[PrimaryKey].fromBytes(slice.toByteBufferWrap)
  }
}
