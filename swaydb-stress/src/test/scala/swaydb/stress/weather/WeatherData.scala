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
