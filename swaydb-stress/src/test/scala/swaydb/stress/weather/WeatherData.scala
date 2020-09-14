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

package swaydb.stress.weather

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

  implicit val weatherDataSerializer = swaydb.serializers.BooPickle[WeatherData]
}
