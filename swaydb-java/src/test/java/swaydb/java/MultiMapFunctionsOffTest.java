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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.java;

import org.junit.jupiter.api.Test;
import swaydb.Prepare;
import swaydb.data.java.TestBase;
import swaydb.java.multimap.MultiPrepareBuilder;
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
import swaydb.multimap.MultiPrepare;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

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

    root.delete();
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

    root.delete();
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

    root.delete();
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

    root.delete();
  }

  @Test
  void multiPrepare() throws IOException {
    MultiMap<String, Integer, String, Void> root = createMap(stringSerializer(), intSerializer(), stringSerializer());
    root.put(1, "root value");

    MultiMap<String, Integer, String, Void> child1 = root.child("child1");
    MultiMap<String, Integer, String, Void> child2 = root.child("child2");
    MultiMap<String, Integer, String, Void> child3 = root.child("child3");

    //create multiPrepare using either one of the MultiPrepareBuilder functions.
    Iterator<MultiPrepare<String, Integer, String, Void>> multiPrepare =
      eitherOne(
        () ->
          Arrays.asList(
            MultiPrepareBuilder.of(child1, Prepare.put(1, "one")),
            MultiPrepareBuilder.of(child2, Prepare.put(2, "two")),
            MultiPrepareBuilder.of(child3, Prepare.put(3, "three"))
          ).iterator(),

        () -> {
          ArrayList<MultiPrepare<String, Integer, String, Void>> list = new ArrayList<>();

          list.addAll(MultiPrepareBuilder.list(child1, asList(Prepare.put(1, "one"), Prepare.put(1, "one"))));
          list.addAll(MultiPrepareBuilder.list(child2, asList(Prepare.put(2, "two"), Prepare.put(2, "two"))));
          list.addAll(MultiPrepareBuilder.list(child3, asList(Prepare.put(3, "three"), Prepare.put(3, "three"))));

          return list.iterator();
        },

        () ->
          Stream.concat(
            Stream.concat(
              MultiPrepareBuilder.stream(child1, asList(Prepare.put(1, "one"), Prepare.put(1, "one"))),
              MultiPrepareBuilder.stream(child2, asList(Prepare.put(2, "two"), Prepare.put(2, "two")))
            ),
            MultiPrepareBuilder.stream(child3, asList(Prepare.put(3, "three"), Prepare.put(3, "three")))
          ).iterator()
      );

    shouldBeTrue(child1.isEmpty());
    shouldBeTrue(child2.isEmpty());
    shouldBeTrue(child3.isEmpty());

    child1.commitMultiPrepare(multiPrepare);

    shouldBeFalse(child1.isEmpty());
    shouldBeFalse(child2.isEmpty());
    shouldBeFalse(child3.isEmpty());

    shouldContain(child1.get(1), "one");
    shouldContain(child2.get(2), "two");
    shouldContain(child3.get(3), "three");

    child1.clearKeyValues();
    shouldBeEmpty(child1.get(1));
    shouldBeTrue(child1.isEmpty());

    shouldContain(child2.get(2), "two");
    shouldContain(child3.get(3), "three");

    root.delete();
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

    root.delete();
  }
}
