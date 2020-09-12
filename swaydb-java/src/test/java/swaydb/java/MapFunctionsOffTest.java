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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.java;

import org.junit.jupiter.api.Test;
import swaydb.KeyVal;
import swaydb.Pair;
import swaydb.Prepare;
import swaydb.data.java.TestBase;
import swaydb.java.data.slice.Slice;
import swaydb.java.serializers.Serializer;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static swaydb.data.java.CommonAssertions.*;
import static swaydb.data.java.JavaEventually.sleep;
import static swaydb.java.serializers.Default.intSerializer;
import static swaydb.java.serializers.Default.stringSerializer;

abstract class MapFunctionsOffTest extends TestBase {

  public abstract <K, V> MapT<K, V, Void> createMap(Serializer<K> keySerializer,
                                                    Serializer<V> valueSerializer) throws IOException;

  public abstract <K, V> MapT<K, V, Void> createMap(Serializer<K> keySerializer,
                                                    Serializer<V> valueSerializer,
                                                    KeyComparator<K> keyComparator) throws IOException;

  /********************
   * PUT
   * ******************
   */
  @Test
  void putIndividuals() throws IOException {

    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(1, "one");
    map.put(2, "two");
    map.put(3, "three");
    map.put(4, "four");
    map.put(5, "five");

    shouldContain(map.get(1), "one");
    shouldContain(map.get(2), "two");
    shouldContain(map.get(3), "three");
    shouldContain(map.get(4), "four");
    shouldContain(map.get(5), "five");

    map.delete();
  }

  @Test
  void putAndExpire() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(1, "one", Duration.ofSeconds(1));
    map.put(2, "two");
    map.put(3, "three", Duration.ofSeconds(3));
    map.put(4, "four");
    map.put(5, "five", Duration.ofSeconds(5));

    shouldContain(map.get(2), "two");
    shouldContain(map.get(4), "four");

    shouldBeEmptyEventually(1, () -> map.get(1));
    shouldBeEmptyEventually(3, () -> map.get(3));
    shouldBeEmptyEventually(5, () -> map.get(5));

    shouldContain(map.get(2), "two");
    shouldContain(map.get(4), "four");

    map.delete();
  }

  @Test
  void putAndExpirePrepare() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());

    map.commit(
      asList(
        Prepare.put(1, "one", Duration.ofSeconds(1)),
        Prepare.put(2, "two"),
        Prepare.put(3, "three", Duration.ofSeconds(1)),
        Prepare.put(1, "one overwrite")
      )
    );

    sleep(Duration.ofSeconds(1));
    shouldContain(map.get(1), "one overwrite");
    shouldContain(map.get(2), "two");
    shouldBeEmpty(map.get(3));

    map.delete();
  }


  @Test
  void putIterable() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    List<KeyVal<Integer, String>> keyVals = asList(KeyVal.create(1, "1 value"), KeyVal.create(2, "2 value"), KeyVal.create(3, "3 value"));

    map.put(keyVals);

    keyVals.forEach(keyVal -> shouldContain(map.get(keyVal.key()), keyVal.key() + " value"));

    shouldBe(map.stream(), keyVals);
    shouldHaveSize(map.stream(), 3);

    map.delete();
  }

  @Test
  void putStream() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.create(integer, integer + " value")));

    foreachRange(1, 100, integer -> shouldContain(map.get(integer), integer + " value"));

    map.delete();
  }

  @Test
  void putPrepare() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.commit(
      asList(
        Prepare.put(1, "one"),
        Prepare.put(2, "two"),
        Prepare.put(3, "three"),
        Prepare.put(1, "one overwrite")
      )
    );

    shouldContain(map.get(1), "one overwrite");
    shouldContain(map.get(2), "two");
    shouldContain(map.get(3), "three");

    map.delete();
  }


  /********************
   * REMOVE
   * ******************
   */

  @Test
  void removeIndividual() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.create(integer, integer + " value")));

    map.remove(1);
    map.remove(50);
    map.remove(100);

    shouldBeEmpty(map.get(1));
    shouldBeEmpty(map.get(50));
    shouldBeEmpty(map.get(100));

    shouldContain(map.get(10), "10 value");
    shouldContain(map.get(90), "90 value");

    map.delete();
  }

  @Test
  void removeRange() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.create(integer, integer + " value")));

    map.remove(1, 100);

    shouldHaveSize(map.stream(), 0);
    shouldBeTrue(map.isEmpty());

    shouldBeEmpty(map.head());
    shouldBeEmpty(map.last());

    map.delete();
  }

  @Test
  void removeStream() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.create(integer, integer + " value")));

    map.remove(Stream.range(1, 100));

    shouldHaveSize(map.stream(), 0);
    shouldBeTrue(map.isEmpty());

    shouldBeEmpty(map.head());
    shouldBeEmpty(map.last());

    map.delete();
  }

  @Test
  void removePrepare() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.create(integer, integer + " value")));

    map.commit(
      asList(
        Prepare.removeFromMap(1),
        Prepare.removeFromMap(2),
        Prepare.removeFromMap(3)
      )
    );

    shouldBeEmpty(map.get(1));
    shouldBeEmpty(map.get(2));
    shouldBeEmpty(map.get(3));

    shouldHaveSize(map.stream(), 97);
    shouldBeFalse(map.isEmpty());

    shouldBe(map.stream().map(KeyVal::key).materialize(), Stream.range(4, 100).materialize());

    map.delete();
  }


  /********************
   * EXPIRE
   * ******************
   */

  @Test
  void expireIndividual() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.create(integer, integer + " value")));

    map.expire(5, Duration.ofSeconds(1));
    map.expire(1, Duration.ofSeconds(1));
    map.expire(10, Duration.ofSeconds(1));

    sleep(Duration.ofSeconds(1));

    shouldBeEmptyEventually(1, () -> map.get(1));
    shouldBeEmptyEventually(1, () -> map.get(5));
    shouldBeEmptyEventually(1, () -> map.get(10));

    map.delete();
  }

  @Test
  void expireRange() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.create(integer, integer + " value")));

    map.expire(1, 100, Duration.ofSeconds(1));

    sleep(Duration.ofSeconds(1));

    Stream.range(1, 100).forEach(i -> shouldBeEmpty(map.get(i)));
    shouldBeTrue(map.isEmpty());

    map.delete();
  }

  @Test
  void expireStream() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.create(integer, integer + " value")));

    map.expire(Stream.range(1, 100).map(i -> Pair.create(i, Duration.ofSeconds(1))));

    sleep(Duration.ofSeconds(1));

    Stream.range(1, 100).forEach(i -> shouldBeEmpty(map.get(i)));
    shouldBeTrue(map.isEmpty());

    map.delete();
  }

  @Test
  void expirePrepare() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.create(integer, integer + " value")));

    map.commit(
      asList(
        Prepare.expireFromMap(1, Duration.ofSeconds(1)),
        Prepare.expireFromMap(2, Duration.ofSeconds(1)),
        Prepare.expireFromMap(3, 20, Duration.ofSeconds(1)),
        Prepare.expireFromMap(90, 100, Duration.ofSeconds(1))
      )
    );

    sleep(Duration.ofSeconds(1));

    shouldBe(map.stream().map(KeyVal::key), Stream.range(21, 89));

    map.delete();
  }

  /********************
   * UPDATE
   * ******************
   */

  @Test
  void updateIndividual() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.create(integer, integer + " value")));

    map.update(5, "updated");
    map.update(5, "updated again"); //overwrites
    map.update(10, "updated");
    map.update(101, "updated"); //does not exists
    map.update(0, "updated"); //does not exists

    shouldContain(map.get(5), "updated again");
    shouldContain(map.get(10), "updated");
    shouldBeEmpty(map.get(101));
    shouldBeEmpty(map.get(0));

    map.delete();
  }

  @Test
  void updateStream() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.create(integer, integer + " value")));

    map.update(Stream.range(1, 100).map(integer -> KeyVal.create(integer, "updated")));

    shouldBe(map.stream().map(KeyVal::value), Stream.range(1, 100).map(i -> "updated"));

    map.stream().forEach(
      keyValue ->
        shouldBe(keyValue.value(), "updated")
    );

    map.delete();
  }

  @Test
  void updatePrepare() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.create(integer, integer + " value")));

    map.commit(
      asList(
        Prepare.update(1, "updated"),
        Prepare.update(2, "updated")
      )
    );

    shouldContain(map.get(1), "updated");
    shouldContain(map.get(2), "updated");
    shouldBe(map.stream().take(2).map(KeyVal::value), Stream.range(1, 2).map(i -> "updated"));
    shouldBe(map.stream().drop(2).map(KeyVal::value), Stream.range(3, 100).map(i -> i + " value"));

    map.delete();
  }


  /********************
   * clearKeyValues
   * ******************
   */

  @Test
  void clearKeyValues() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.create(integer, integer + " value")));

    map.clearKeyValues();

    shouldBeEmpty(map.get(1));
    shouldBeEmpty(map.stream());

    map.delete();
  }

  /********************
   * COMMIT
   * ******************
   */

  @Test
  void commitStream() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.commit(Stream.range(1, 100).map(integer -> Prepare.put(integer, integer + " value")));

    foreachRange(1, 100, integer -> shouldContain(map.get(integer), integer + " value"));

    map.delete();
  }

  @Test
  void commitIterable() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());

    map.commit(
      asList(
        Prepare.put(1, "one"),
        Prepare.put(2, "two", Duration.ofSeconds(2)),
        Prepare.put(3, "remove"),
        Prepare.put(4, "range remove"),
        Prepare.put(5, "range remove"),
        Prepare.put(6, "expire"),
        Prepare.put(7, "update"),
        Prepare.put(8, "update"),
        Prepare.put(9, "update"),
        Prepare.removeFromMap(3),
        Prepare.removeFromMap(4, 5),
        Prepare.expireFromMap(6, Duration.ofSeconds(3)),
        Prepare.update(7, "updated value"),
        Prepare.update(8, 9, "updated value")
      )
    );

    shouldContain(map.get(1), "one");

    shouldContain(map.get(2), "two");
    sleep(Duration.ofSeconds(2));
    shouldBeEmpty(map.get(2));

    shouldBeEmpty(map.get(3));
    shouldBeEmpty(map.get(4));
    shouldBeEmpty(map.get(5));

    sleep(Duration.ofSeconds(3));
    shouldBeEmpty(map.get(6));

    shouldContain(map.get(7), "updated value");
    shouldContain(map.get(8), "updated value");
    shouldContain(map.get(9), "updated value");

    map.delete();
  }

  @Test
  void comparatorTest() throws IOException {
    MapT<Integer, Integer, Void> map =
      createMap(intSerializer(), intSerializer(), (left, right) -> left.compareTo(right) * -1);

    assertDoesNotThrow(() -> map.put(1, 1));
    assertDoesNotThrow(() -> map.put(2, 2));

    List<Integer> integers = map
      .stream()
      .map(KeyVal::key)
      .materialize();

    assertEquals(asList(2, 1), integers);

    map.delete();
  }

  @Test
  void createMapWithCustomSerializer() throws IOException {
    class Key {
      Integer key;

      Key setKey(Integer key) {
        this.key = key;
        return this;
      }
    }

    class Value {
      Integer value;

      Value setValue(Integer value) {
        this.value = value;
        return this;
      }
    }

    Key key1 = new Key().setKey(1);
    Key key2 = new Key().setKey(2);

    Value value1 = new Value().setValue(1);
    Value value2 = new Value().setValue(2);

    Serializer<Key> keySerializer = new Serializer<Key>() {
      @Override
      public byte[] write(Key data) {
        byte[] bytes = {data.key.byteValue()};
        return bytes;
      }

      @Override
      public Key read(Slice<Byte> slice) {
        if (slice.get(0) == 1) {
          return key1;
        } else {
          return key2;
        }
      }
    };

    Serializer<Value> valueSerializer = new Serializer<Value>() {
      @Override
      public byte[] write(Value data) {
        byte[] bytes = {data.value.byteValue()};
        return bytes;
      }

      @Override
      public Value read(Slice<Byte> slice) {
        if (slice.get(0) == 1) {
          return value1;
        } else {
          return value2;
        }
      }
    };

    MapT<Key, Value, Void> map =
      createMap(keySerializer, valueSerializer);

    assertDoesNotThrow(() -> map.put(key1, value1));
    assertDoesNotThrow(() -> map.put(key2, value2));

    List<Key> mapKeys =
      map
        .stream()
        .map(KeyVal::key)
        .materialize();

    assertEquals(asList(key1, key2), mapKeys);

    if (map instanceof swaydb.java.Map) {
      List<Integer> setKeys =
        ((swaydb.java.Map<Key, Value, Void>) map)
          .keys()
          .stream()
          .map(key -> key.key)
          .materialize();

      assertEquals(asList(1, 2), setKeys);
    }

    map.delete();
  }
}
