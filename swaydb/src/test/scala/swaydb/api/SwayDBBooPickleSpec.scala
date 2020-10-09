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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.api

import org.scalatest.OptionValues._
import swaydb._
import swaydb.core.TestCaseSweeper
import swaydb.core.TestData._
import swaydb.data.RunThis.runThis
import swaydb.core.CommonAssertions._
import scala.util.Random
import TestCaseSweeper._

class SwayDBBooPickleSpec extends TestBaseEmbedded {

  case class User(email: String, id: Int, string: String, optional: Option[String])

  case class Address(someDouble: Double)
  case class Info(firstName: String, lastName: String, address: Address)

  override val keyValueCount: Int = 100

  import boopickle.Default._

  implicit val keySerializer = serializers.BooPickle[User]
  implicit val valueSerializer = serializers.BooPickle[Info]

  "SwayDB" should {

    "remove all but first and last" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = swaydb.persistent.Map[User, Info, Nothing, Bag.Glass](randomDir).sweep(_.delete())

            val keyValues =
              (1 to 10000) map {
                _ =>
                  val key = User(randomString, randomIntMax(), randomString, eitherOne(None, Some(randomString)))
                  val value = Info(randomString, randomString, Address(randomIntMax() * Random.nextDouble()))
                  db.put(key, value)
                  (key, value)
              }

            keyValues foreach {
              case (key, value) =>
                db.get(key).value shouldBe value
            }
        }
      }
    }
  }
}
