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

package swaydb.extensions

import swaydb.api.TestBaseEmbedded
import swaydb.core.IOAssert._
import swaydb.core.RunThis._
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

class NestedOptionValueSpec extends TestBaseEmbedded {
  override val keyValueCount: Int = 1000

  "Option[Option[V]]" in {

    import swaydb.serializers.Default._

    implicit object OptionOptionStringSerializer extends Serializer[Option[Option[String]]] {
      override def write(data: Option[Option[String]]): Slice[Byte] =
        data.flatten map {
          string =>
            StringSerializer.write(string)
        } getOrElse Slice.emptyBytes

      override def read(data: Slice[Byte]): Option[Option[String]] =
        if (data.isEmpty)
          None
        else
          Some(Some(StringSerializer.read(data)))
    }

    val rootMap = swaydb.extensions.memory.Map[Int, Option[String]]().assertGet

    rootMap.put(1, None).assertGet

    rootMap.materialize.get should contain only ((1, None))
    rootMap.keys.stream.materialize.get should contain only 1

  }

  "Option[Empty[V]]" in {

    sealed trait Value
    object Value {
      case class NonEmpty(string: String) extends Value
      case object Empty extends Value
    }

    import swaydb.serializers.Default._

    implicit object OptionOptionStringSerializer extends Serializer[Option[Value]] {
      override def write(data: Option[Value]): Slice[Byte] =
        data map {
          case Value.NonEmpty(string) =>
            StringSerializer.write(string)
          case Value.Empty =>
            Slice.emptyBytes
        } getOrElse Slice.emptyBytes

      override def read(data: Slice[Byte]): Option[Value] =
        if (data.isEmpty)
          Some(Value.Empty)
        else
          Some(Value.NonEmpty(StringSerializer.read(data)))
    }

    val rootMap = swaydb.extensions.memory.Map[Int, Option[Value]]().assertGet

    rootMap.put(1, Some(Value.Empty)).assertGet
    rootMap.put(2, Some(Value.NonEmpty("two"))).assertGet
    rootMap.put(3, None).assertGet

    rootMap.materialize.get should contain inOrderOnly((1, None), (2, Some(Value.NonEmpty("two"))), (3, None))
    rootMap.keys.stream.materialize.get should contain inOrderOnly(1, 2, 3)
  }

}
