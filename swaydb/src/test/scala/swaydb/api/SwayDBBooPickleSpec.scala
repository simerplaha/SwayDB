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

package swaydb.api

import org.scalatest.OptionValues._
import swaydb._
import swaydb.core.CommonAssertions._
import swaydb.core.TestSweeper
import swaydb.core.TestSweeper._
import swaydb.core.CoreTestData._
import swaydb.testkit.RunThis.runThis

import scala.util.Random
import swaydb.testkit.TestKit._
import swaydb.core.file.CoreFileTestKit._

class SwayDBBooPickleSpec extends TestBaseAPI {

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
        TestSweeper {
          implicit sweeper =>

            val db = swaydb.persistent.Map[User, Info, Nothing, Glass](randomDir()).sweep(_.delete())

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
