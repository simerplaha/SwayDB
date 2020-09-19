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

package swaydb.java;

import org.junit.jupiter.api.Test;
import swaydb.data.java.TestBase;
import swaydb.java.serializers.Serializer;
import swaydb.java.table.domain.table.key.Key;
import swaydb.java.table.domain.table.key.KeySerializer;
import swaydb.java.table.domain.table.key.ProductKey;
import swaydb.java.table.domain.table.key.UserKey;
import swaydb.java.table.domain.table.mapKey.MapKey;
import swaydb.java.table.domain.table.mapKey.MapKeySerializer;
import swaydb.java.table.domain.table.mapKey.ProductsMap;
import swaydb.java.table.domain.table.mapKey.UsersMap;
import swaydb.java.table.domain.table.value.ProductValue;
import swaydb.java.table.domain.table.value.UserValue;
import swaydb.java.table.domain.table.value.Value;
import swaydb.java.table.domain.table.value.ValueSerializer;

import java.io.IOException;

import static java.util.Arrays.asList;
import static swaydb.data.java.JavaTest.*;
import static swaydb.java.serializers.Default.intSerializer;
import static swaydb.java.serializers.Default.stringSerializer;


abstract class MultiMapFunctionsOffTest extends TestBase {

  public abstract <M, K, V> MultiMap<M, K, V, Void> createMap(Serializer<M> mapKeySerializer,
                                                              Serializer<K> keySerializer,
                                                              Serializer<V> valueSerializer) throws IOException;

  public abstract <M, K, V> MultiMap<M, K, V, Void> createMap(Serializer<M> mapKeySerializer,
                                                              Serializer<K> keySerializer,
                                                              Serializer<V> valueSerializer,
                                                              KeyComparator<K> keyComparator) throws IOException;


  @Test
  void putInRootMap() throws IOException {
    MultiMap<String, Integer, String, Void> root =
      createMap(stringSerializer(), intSerializer(), stringSerializer());

    root.put(1, "root value");
    shouldContain(root.get(1), "root value");
  }

  @Test
  void createChildUnderRootMap() throws IOException {
    MultiMap<String, Integer, String, Void> root = createMap(stringSerializer(), intSerializer(), stringSerializer());
    root.put(1, "root value");

    MultiMap<String, Integer, String, Void> child1 = root.child("child1");

    shouldContain(root.getChild("child1"), child1);
    shouldContainOnly(root.children(), child1);

    shouldContain(root.get(1), "root value");
    shouldBeTrue(child1.isEmpty());
  }

  @Test
  void createNestedChildren() throws IOException {
    MultiMap<String, Integer, String, Void> root = createMap(stringSerializer(), intSerializer(), stringSerializer());
    root.put(1, "root value");

    MultiMap<String, Integer, String, Void> child1 =
      root
        .child("child1")
        .child("child2")
        .child("child3")
        .child("child4")
        .child("child5");

    shouldBe(root.childrenFlatten().map(MultiMap::mapKey), asList("child1", "child2", "child3", "child4", "child5"));
    shouldHaveSize(root.childrenFlatten(), 5);

    shouldContain(root.get(1), "root value");
    shouldBeTrue(child1.isEmpty());
  }

  @Test
  void removeChildren() throws IOException {
    MultiMap<String, Integer, String, Void> root = createMap(stringSerializer(), intSerializer(), stringSerializer());
    root.put(1, "root value");

    MultiMap<String, Integer, String, Void> child1 = root.child("child1");
    MultiMap<String, Integer, String, Void> child2 = root.child("child2");
    MultiMap<String, Integer, String, Void> child3 = root.child("child3");

    shouldBe(root.childrenFlatten().map(MultiMap::mapKey), asList("child1", "child2", "child3"));

    root.removeChild(child1.mapKey());
    shouldBe(root.childrenFlatten().map(MultiMap::mapKey), asList("child2", "child3"));

    root.removeChild(child2.mapKey());
    shouldBe(root.childrenFlatten().map(MultiMap::mapKey), asList("child3"));

    root.removeChild(child3.mapKey());
    shouldBeEmpty(root.childrenFlatten());
  }

  /**
   * Demos how MultiMap can be used to create nested tables. The following creates two tables Users and Products
   * under the root table and bounds specific type types.
   */
  @Test
  void multiMapDemo() throws IOException {
    //First lets create a root Map with serializers set.
    MultiMap<MapKey, Key, Value, Void> root =
      createMap(MapKeySerializer.instance, KeySerializer.instance, ValueSerializer.instance);

    //Create two child sibling tables under the root map - Users and Products
    MultiMap<MapKey, UserKey, UserValue, Void> users = root.child(UsersMap.instance, UserKey.class, UserValue.class);
    MultiMap<MapKey, ProductKey, ProductValue, Void> products = root.child(ProductsMap.instance, ProductKey.class, ProductValue.class);

    //assert that the root table contains children.
    shouldContain(root.getChild(UsersMap.instance, UserKey.class, UserValue.class), users);
    shouldContain(root.getChild(ProductsMap.instance, ProductKey.class, ProductValue.class), products);

    //print all child tables
    root.children().forEach(table -> System.out.println(table.mapKey()));

    //insert data into User table
    foreachRange(1, 10, i -> users.put(UserKey.of(i + "@email.com"), UserValue.of("First-" + i, "Last-" + i)));
    System.out.println("Users\n"); //print data from user table
    users.forEach(System.out::println);

    //Insert data into product table
    foreachRange(1, 10, i -> products.put(ProductKey.of(i), ProductValue.of(i)));
    System.out.println("Products\n"); //print that data.
    products.forEach(System.out::println);
  }
}
