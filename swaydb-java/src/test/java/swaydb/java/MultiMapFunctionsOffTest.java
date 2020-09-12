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

import java.io.IOException;

import static java.util.Arrays.asList;
import static swaydb.data.java.CommonAssertions.*;
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

    MultiMap<String, Integer, String, Void> child1 = root.schema().init("child1");

    shouldContain(root.schema().get("child1"), child1);
    shouldContainOnly(root.schema().stream(), child1);

    shouldContain(root.get(1), "root value");
    shouldBeTrue(child1.isEmpty());
  }

  @Test
  void createNestedChildren() throws IOException {
    MultiMap<String, Integer, String, Void> root = createMap(stringSerializer(), intSerializer(), stringSerializer());
    root.put(1, "root value");

    MultiMap<String, Integer, String, Void> child1 =
      root
        .schema().init("child1")
        .schema().init("child2")
        .schema().init("child3")
        .schema().init("child4")
        .schema().init("child5");

    shouldBe(root.schema().flatten().map(swaydb.MultiMap::mapKey), asList("child1", "child2", "child3", "child4", "child5"));
    shouldHaveSize(root.schema().flatten(), 5);

    shouldContain(root.get(1), "root value");
    shouldBeTrue(child1.isEmpty());
  }

  @Test
  void removeChildren() throws IOException {
    MultiMap<String, Integer, String, Void> root = createMap(stringSerializer(), intSerializer(), stringSerializer());
    root.put(1, "root value");

    MultiMap<String, Integer, String, Void> child1 = root.schema().init("child1");
    MultiMap<String, Integer, String, Void> child2 = root.schema().init("child2");
    MultiMap<String, Integer, String, Void> child3 = root.schema().init("child3");

    shouldBe(root.schema().flatten().map(swaydb.MultiMap::mapKey), asList("child1", "child2", "child3"));

    root.schema().remove(child1.mapKey());
    shouldBe(root.schema().flatten().map(swaydb.MultiMap::mapKey), asList("child2", "child3"));

    root.schema().remove(child2.mapKey());
    shouldBe(root.schema().flatten().map(swaydb.MultiMap::mapKey), asList("child3"));

    root.schema().remove(child3.mapKey());
    shouldBeEmpty(root.schema().flatten());
  }
}
