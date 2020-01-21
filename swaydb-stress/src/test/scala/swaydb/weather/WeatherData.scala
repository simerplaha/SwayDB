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
 */

package swaydb.weather

import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

sealed trait Direction
object Direction {
  case object East extends Direction
  case object West extends Direction
  case object North extends Direction
  case object South extends Direction
}

sealed trait Location
object Location {
  case object Sydney extends Location
}

case class Water(temp: Double,
                 waveDirection: Direction,
                 waveDegrees: Double)

case class Wind(speed: Double,
                direction: Direction,
                degrees: Int,
                pressure: Double)

case class WeatherData(water: Water,
                       wind: Wind,
                       location: Location)

object WeatherData {

  import boopickle.Default._

  implicit val weatherDataSerializer = new Serializer[WeatherData] {
    override def write(data: WeatherData): Slice[Byte] =
      Slice(Pickle.intoBytes(data).array())

    override def read(data: Slice[Byte]): WeatherData =
      Unpickle[WeatherData].fromBytes(data.toByteBufferWrap)
  }
}