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

import org.scalatest.OptionValues._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import swaydb.api.TestBaseEmbedded
import swaydb.api.multimap.multiprepare.PrimaryKey._
import swaydb.api.multimap.multiprepare.Row._
import swaydb.api.multimap.multiprepare.Table._
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.multimap.MultiPrepare
import swaydb.{Bag, Glass, Prepare}

import scala.concurrent.duration._

class MultiMapMultiPrepareSpec extends TestBaseEmbedded {
  override val keyValueCount: Int = 1000

  "transaction" when {
    "nested map hierarchy" in {
      TestCaseSweeper {
        implicit sweeper =>

          implicit val bag = Bag.glass

          //Create a memory database
          val root = swaydb.memory.MultiMap[Table, PrimaryKey, Row, Nothing, Glass]().sweep(_.delete())

          //create sibling1 UserMap and it's child UserActivity
          val userMap = root.child(Table.User: Table.UserTables, classOf[PrimaryKey.UserPrimaryKeys], classOf[Row.UserRows])
          val userActivityMap = userMap.child(Table.Activity, classOf[PrimaryKey.Activity], classOf[Row.Activity])

          //create sibling2 ProductMap and it's child ProductOrderMap
          val productMap = root.child(Table.Product: Table.ProductTables, classOf[PrimaryKey.ProductPrimaryKey], classOf[Row.ProductRows])
          val productOrderMap = productMap.child(Table.Order, classOf[PrimaryKey.Order], classOf[Row.Order])

          //create a transaction to write into userActivity and User
          val transaction =
            MultiPrepare(userActivityMap, Prepare.Put(PrimaryKey.Activity(1), Row.Activity("act1"))) ++
              MultiPrepare(userMap, Prepare.Put(PrimaryKey.Email("email1"), Row.User("name1", "address1")))

          //commit under too UserMap
          userMap.commitMultiPrepare(transaction)

          //assert commit
          def assertTransaction1() = {
            userMap.get(PrimaryKey.Email("email1")).value shouldBe Row.User("name1", "address1")
            userMap.get(PrimaryKey.Activity(1)) shouldBe empty
            userActivityMap.get(PrimaryKey.Activity(1)).value shouldBe Row.Activity("act1")
          }

          assertTransaction1()

          //product is still is empty
          productMap.materialize.toList shouldBe empty
          productOrderMap.materialize.toList shouldBe empty

          //crete transaction2 which uses all maps
          val transaction2 =
            MultiPrepare(userActivityMap, Prepare.Put(PrimaryKey.Activity(2), Row.Activity("act2"))) ++
              MultiPrepare(userMap, Prepare.Put(PrimaryKey.Email("email2"), Row.User("name2", "address2"))) ++
              MultiPrepare(productMap, Prepare.Put(PrimaryKey.SKU(1), Row.Product(1))) ++
              MultiPrepare(productOrderMap, Prepare.Put(PrimaryKey.Order(1), Row.Order(1, 1))) ++
              //expire order 2 in 2.seconds
              MultiPrepare(productOrderMap, Prepare.Put(PrimaryKey.Order(2), Row.Order(2, 2), 2.seconds))

          //all map can only committed under root map
          root.commitMultiPrepare(transaction2)

          def assertTransaction2() = {
            userActivityMap.get(PrimaryKey.Activity(2)).value shouldBe Row.Activity("act2")
            userMap.get(PrimaryKey.Email("email2")).value shouldBe Row.User("name2", "address2")
            productMap.get(PrimaryKey.SKU(1)).value shouldBe Row.Product(1)
            productOrderMap.get(PrimaryKey.Order(1)).value shouldBe Row.Order(1, 1)
            productOrderMap.get(PrimaryKey.Order(2)).value shouldBe Row.Order(2, 2)
            productOrderMap.expiration(PrimaryKey.Order(2)) shouldBe defined
          }

          //assert all maps
          assertTransaction2()
          assertTransaction1()

          //products are non empty
          productMap.materialize.toList should not be empty
          productOrderMap.materialize.toList should not be empty

          //order2 expires after 2 seconds
          eventually(Timeout(2.seconds)) {
            productOrderMap.get(PrimaryKey.Order(2)) shouldBe empty
          }
      }
    }
  }
}
