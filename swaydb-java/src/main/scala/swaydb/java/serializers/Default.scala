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

package swaydb.java.serializers

import swaydb.java.serializers.SerializerConverter.toJava

object Default {

  def intSerializer(): Serializer[java.lang.Integer] =
    toJava(swaydb.serializers.Default.javaIntSerializer())

  def longSerializer(): Serializer[java.lang.Long] =
    toJava(swaydb.serializers.Default.javaLongSerializer())

  def charSerializer(): Serializer[java.lang.Character] =
    toJava(swaydb.serializers.Default.javaCharSerializer())

  def doubleSerializer(): Serializer[java.lang.Double] =
    toJava(swaydb.serializers.Default.javaDoubleSerializer())

  def floatSerializer(): Serializer[java.lang.Float] =
    toJava(swaydb.serializers.Default.javaFloatSerializer())

  def shortSerializer(): Serializer[java.lang.Short] =
    toJava(swaydb.serializers.Default.javaShortSerializer())

  def stringSerializer(): Serializer[java.lang.String] =
    toJava(swaydb.serializers.Default.javaStringSerializer())
}