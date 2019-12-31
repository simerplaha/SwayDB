/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb

import scala.collection.compat.IterableOnce
import scala.collection.mutable

/**
 * Scala collections are blocking and requires an IO Map from SwayDB to build a Map.
 */
private[swaydb] object ScalaMap {

  def apply[K, V, F](db: Map[K, V, F, IO.ApiIO]): mutable.Map[K, V] =
    new ScalaMapBase[K, V, F](db) {

      override def addOne(kv: (K, V)): this.type = {
        db.put(kv._1, kv._2).get
        this
      }

      override def subtractOne(key: K): this.type = {
        db.remove(key).get
        this
      }

      override def subtractAll(xs: IterableOnce[K]): this.type = {
        db.remove(xs.iterator).get
        this
      }

      override def addAll(xs: IterableOnce[(K, V)]): this.type = {
        db.put(xs.iterator).get
        this
      }
    }
}
