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

package swaydb.multimap.transaction

import org.scalatest.OptionValues._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import swaydb.api.TestBaseEmbedded
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.multimap.transaction.PrimaryKey._
import swaydb.multimap.transaction.Row._
import swaydb.multimap.transaction.Table._
import swaydb.{Bag, Prepare}

import scala.concurrent.duration._

class MultiMapTransactionSpec extends TestBaseEmbedded {
  override val keyValueCount: Int = 1000

  "transaction" when {
    TestCaseSweeper {
      implicit sweeper =>

        implicit val bag = Bag.less

        //Create a memory database
        val root = swaydb.memory.MultiMap[Table, PrimaryKey, Row, Nothing, Bag.Less]().sweep()

        //create sibling1 UserMap and it's child UserActivity
        val userMap = root.schema.init(Table.User: Table.UserTables, classOf[PrimaryKey.UserPrimaryKeys], classOf[Row.UserRows])
        val userActivityMap = userMap.schema.init(Table.Activity, classOf[PrimaryKey.Activity], classOf[Row.Activity])

        //create sibling2 ProductMap and it's child ProductOrderMap
        val productMap = root.schema.init(Table.Product: Table.ProductTables, classOf[PrimaryKey.ProductPrimaryKey], classOf[Row.ProductRows])
        val productOrderMap = productMap.schema.init(Table.Order, classOf[PrimaryKey.Order], classOf[Row.Order])

        "nested map hierarchy" in {

          //create a transaction to write into userActivity and User
          val transaction =
            userActivityMap.toTransaction(Prepare.Put(PrimaryKey.Activity(1), Row.Activity("act1"))) ++
              userMap.toTransaction(Prepare.Put(PrimaryKey.Email("email1"), Row.User("name1", "address1")))

          //commit under too UserMap
          userMap.commit(transaction)

          //assert commit
          def assertTransaction1() = {
            userMap.get(PrimaryKey.Email("email1")).value shouldBe Row.User("name1", "address1")
            userMap.get(PrimaryKey.Activity(1)) shouldBe empty
            userActivityMap.get(PrimaryKey.Activity(1)).value shouldBe Row.Activity("act1")
          }

          assertTransaction1()

          //product is still is empty
          productMap.stream.materialize.toList shouldBe empty
          productOrderMap.stream.materialize.toList shouldBe empty

          //crete transaction2 which uses all maps
          val transaction2 =
            userActivityMap.toTransaction(Prepare.Put(PrimaryKey.Activity(2), Row.Activity("act2"))) ++
              userMap.toTransaction(Prepare.Put(PrimaryKey.Email("email2"), Row.User("name2", "address2"))) ++
              productMap.toTransaction(Prepare.Put(PrimaryKey.SKU(1), Row.Product(1))) ++
              productOrderMap.toTransaction(Prepare.Put(PrimaryKey.Order(1), Row.Order(1, 1))) ++
              //expire order 2 in 2.seconds
              productOrderMap.toTransaction(Prepare.Put(PrimaryKey.Order(2), Row.Order(2, 2), 2.seconds))

          //all map can only committed under root map
          root.commit(transaction2)

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
          productMap.stream.materialize.toList should not be empty
          productOrderMap.stream.materialize.toList should not be empty

          //order2 expires after 2 seconds
          eventually(Timeout(2.seconds)) {
            productOrderMap.get(PrimaryKey.Order(2)) shouldBe empty
          }
        }
    }
  }
}
