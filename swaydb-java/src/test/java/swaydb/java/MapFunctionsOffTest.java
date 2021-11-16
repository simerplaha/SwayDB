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

package swaydb.java;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Test;
import scala.Int;
import swaydb.Prepare;
import swaydb.java.TestBase;
import swaydb.java.serializers.Serializer;
import swaydb.slice.Slice;
import swaydb.slice.utils.ByteOps;
import swaydb.java.KeyVal;
import swaydb.java.Pair;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static swaydb.java.JavaEventually.sleep;
import static swaydb.java.JavaTest.*;
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
    List<KeyVal<Integer, String>> keyVals = asList(KeyVal.of(1, "1 value"), KeyVal.of(2, "2 value"), KeyVal.of(3, "3 value"));

    map.put(keyVals);

    keyVals.forEach(keyVal -> shouldContain(map.get(keyVal.key()), keyVal.key() + " value"));

    shouldBe(map, keyVals);
    shouldHaveSize(map, 3);

    map.delete();
  }

  @Test
  void putStream() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

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
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

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
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    map.remove(1, 100);

    shouldHaveSize(map, 0);
    shouldBeTrue(map.isEmpty());

    shouldBeEmpty(map.head());
    shouldBeEmpty(map.last());

    map.delete();
  }

  @Test
  void removeStream() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    map.remove(Stream.range(1, 100));

    shouldHaveSize(map, 0);
    shouldBeTrue(map.isEmpty());

    shouldBeEmpty(map.head());
    shouldBeEmpty(map.last());

    map.delete();
  }

  @Test
  void removePrepare() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

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

    shouldHaveSize(map, 97);
    shouldBeFalse(map.isEmpty());

    shouldBe(map.map(KeyVal::key).materialize(), Stream.range(4, 100).materialize());

    map.delete();
  }


  /********************
   * EXPIRE
   * ******************
   */

  @Test
  void expireIndividual() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

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
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    map.expire(1, 100, Duration.ofSeconds(1));

    sleep(Duration.ofSeconds(1));

    Stream.range(1, 100).forEach(i -> shouldBeEmpty(map.get(i)));
    shouldBeTrue(map.isEmpty());

    map.delete();
  }

  @Test
  void expireStream() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    map.expire(Stream.range(1, 100).map(i -> Pair.of(i, Duration.ofSeconds(1))));

    sleep(Duration.ofSeconds(1));

    Stream.range(1, 100).forEach(i -> shouldBeEmpty(map.get(i)));
    shouldBeTrue(map.isEmpty());

    map.delete();
  }

  @Test
  void expirePrepare() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    map.commit(
      asList(
        Prepare.expireFromMap(1, Duration.ofSeconds(1)),
        Prepare.expireFromMap(2, Duration.ofSeconds(1)),
        Prepare.expireFromMap(3, 20, Duration.ofSeconds(1)),
        Prepare.expireFromMap(90, 100, Duration.ofSeconds(1))
      )
    );

    sleep(Duration.ofSeconds(1));

    shouldBe(map.map(KeyVal::key), Stream.range(21, 89));

    map.delete();
  }

  /********************
   * UPDATE
   * ******************
   */

  @Test
  void updateIndividual() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

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
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    map.update(Stream.range(1, 100).map(integer -> KeyVal.of(integer, "updated")));

    shouldBe(map.map(KeyVal::value), Stream.range(1, 100).map(i -> "updated"));

    map.forEach(
      keyValue ->
        shouldBe(keyValue.value(), "updated")
    );

    map.delete();
  }

  @Test
  void updatePrepare() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    map.commit(
      asList(
        Prepare.update(1, "updated"),
        Prepare.update(2, "updated")
      )
    );

    shouldContain(map.get(1), "updated");
    shouldContain(map.get(2), "updated");
    shouldBe(map.take(2).map(KeyVal::value), Stream.range(1, 2).map(i -> "updated"));
    shouldBe(map.drop(2).map(KeyVal::value), Stream.range(3, 100).map(i -> i + " value"));

    map.delete();
  }


  /********************
   * clearKeyValues
   * ******************
   */

  @Test
  void clearKeyValues() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    map.clearKeyValues();

    shouldBeEmpty(map.get(1));
    shouldBeEmpty(map);

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

  /**
   * GET
   */
  @Test
  void get() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    foreachRange(1, 100, integer -> shouldContain(map.get(integer), integer + " value"));

    shouldBeEmpty(map.get(200));
    shouldBeEmpty(map.get(Integer.MAX_VALUE));
    shouldBeEmpty(map.get(Integer.MIN_VALUE));

    map.delete();
  }

  @Test
  void getKey() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    foreachRange(1, 100, integer -> shouldContain(map.getKey(integer), integer));

    shouldBeEmpty(map.getKey(200));
    shouldBeEmpty(map.getKey(Integer.MAX_VALUE));
    shouldBeEmpty(map.getKey(Integer.MIN_VALUE));

    map.delete();
  }

  @Test
  void getKeyValue() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    foreachRange(1, 100, integer -> shouldContain(map.getKeyValue(integer), KeyVal.of(integer, integer + " value")));

    shouldBeEmpty(map.getKeyValue(200));
    shouldBeEmpty(map.getKeyValue(Integer.MAX_VALUE));
    shouldBeEmpty(map.getKeyValue(Integer.MIN_VALUE));

    map.delete();
  }

  @Test
  void getKeyDeadline() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    foreachRange(1, 100, integer -> shouldContain(map.getKeyDeadline(integer), Pair.of(integer, Optional.empty())));

    shouldBeEmpty(map.getKeyDeadline(200));
    shouldBeEmpty(map.getKeyDeadline(Integer.MAX_VALUE));
    shouldBeEmpty(map.getKeyDeadline(Integer.MIN_VALUE));

    map.delete();
  }

  @Test
  void getKeyValueDeadline() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    foreachRange(1, 100, integer -> shouldContain(map.getKeyValueDeadline(integer), Pair.of(KeyVal.of(integer, integer + " value"), Optional.empty())));

    shouldBeEmpty(map.getKeyValueDeadline(200));
    shouldBeEmpty(map.getKeyValueDeadline(Integer.MAX_VALUE));
    shouldBeEmpty(map.getKeyValueDeadline(Integer.MIN_VALUE));

    map.delete();
  }

  /**
   * CONTAINS
   */

  @Test
  void contains() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    foreachRange(1, 100, integer -> shouldBeTrue(map.contains(integer)));

    assertFalse(map.contains(200));

    shouldBeFalse(map.contains(200));
    shouldBeFalse(map.contains(Integer.MAX_VALUE));
    shouldBeFalse(map.contains(Integer.MIN_VALUE));

    map.delete();
  }

  @Test
  void mightContain() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    foreachRange(1, 100, integer -> shouldBeTrue(map.mightContain(integer)));

    assertFalse(map.mightContain(200));

    shouldBeFalse(map.mightContain(200));
    shouldBeFalse(map.mightContain(Integer.MAX_VALUE));
    shouldBeFalse(map.mightContain(Integer.MIN_VALUE));

    map.delete();
  }

  /**
   * KEYS
   */

  @Test
  void keys() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    shouldBe(map.keys(), Stream.range(1, 100));

    map.delete();
  }

  /**
   * VALUES
   */

  @Test
  void values() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    shouldBe(map.values(), Stream.range(1, 100).map(integer -> integer + " value"));

    map.delete();
  }

  /**
   * meters
   */
  @Test
  void levelZeroMeter() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    shouldBe(map.levelZeroMeter().logsCount(), 1);

    map.delete();
  }

  @Test
  void levelMeter() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    shouldBeEmpty(map.levelMeter(8));

    map.delete();
  }

  @Test
  void head() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    shouldContain(map.head(), KeyVal.of(1, "1 value"));

    map.delete();
  }

  @Test
  void stream() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    Stream<KeyVal<Integer, String>> stream =
      map
        .from(10)
        .drop(10)
        .takeWhile(keyValue -> true);

    shouldBe(stream, Stream.range(20, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    map.delete();
  }

  @Test
  void iterator() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    Iterator<KeyVal<Integer, String>> iterator = map.iterator();

    shouldBeSameIterators(iterator, Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")).iterator());

    map.delete();
  }

  @Test
  void clearAppliedFunctions() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    shouldBeEmpty(map.clearAppliedFunctions());

    shouldBeEmpty(map.clearAppliedAndRegisteredFunctions());

    map.delete();
  }

  @Test
  void asJava() throws IOException {
    MapT<Integer, String, Void> map = createMap(intSerializer(), stringSerializer());
    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    Map<Integer, String> javaMap = map.asJava();
    foreachRange(1, 100, integer -> shouldBe(javaMap.get(integer), integer + " value"));

    //insert in java map
    shouldBe(javaMap.put(1, "new value"), "1 value");
    //read from scala map
    shouldContain(map.get(1), "new value");

    HashMap<Integer, String> integerStringHashMap = new HashMap<>();
    integerStringHashMap.put(10, "ten");
    integerStringHashMap.put(11, "eleven");
    integerStringHashMap.put(Int.MaxValue(), "MaxValue");
    //put many in java
    javaMap.putAll(integerStringHashMap);

    //read from scala map
    shouldContain(map.get(10), "ten");
    shouldContain(map.get(11), "eleven");
    shouldContain(map.get(Int.MaxValue()), "MaxValue");

    //old values remain
    shouldContain(map.get(2), "2 value");

    //remove
    shouldBe(javaMap.remove(11), "eleven");
    shouldBeFalse(map.contains(11));

    shouldBe(javaMap.remove(100), "100 value");
    shouldBeFalse(map.contains(100));

    map.delete();
  }

  @Test
  void comparatorTest() throws IOException {
    MapT<Integer, String, Void> map =
      createMap(intSerializer(), stringSerializer(), (left, right) -> left.compareTo(right) * -1);

    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + " value")));

    Iterable<Integer> stream =
      map
        .map(KeyVal::key)
        .materialize();

    List<Integer> expected = Stream.range(1, 100).materializeList();
    Collections.reverse(expected);

    shouldBe(stream, expected);

    //reversing a reversed map results in ordered
    Stream<Integer> reversed =
      map
        .reverse()
        .map(KeyVal::key);

    shouldBe(reversed, Stream.range(1, 100));

    map.delete();
  }

  @RequiredArgsConstructor(staticName = "of")
  @EqualsAndHashCode
  static class Key {
    @Getter
    private final int key;
  }

  @RequiredArgsConstructor(staticName = "of")
  @EqualsAndHashCode
  static class Value {
    @Getter
    private final int value;
  }

  @Test
  void createMapWithCustomSerializer() throws IOException {

    Key key1 = Key.of(1);
    Key key2 = Key.of(2);

    Value value1 = Value.of(1);
    Value value2 = Value.of(2);

    Serializer<Key> keySerializer = new Serializer<Key>() {
      @Override
      public Slice<Byte> write(Key data) {
        return Slice.writeUnsignedInt(data.key, ByteOps.Java());
      }

      @Override
      public Key read(Slice<Byte> slice) {
        return new Key(slice.readUnsignedInt(ByteOps.Java()));
      }
    };

    Serializer<Value> valueSerializer = new Serializer<Value>() {
      @Override
      public Slice<Byte> write(Value data) {
        return Slice.writeUnsignedInt(data.value, ByteOps.Java());
      }

      @Override
      public Value read(Slice<Byte> slice) {
        return new Value(slice.readUnsignedInt(ByteOps.Java()));
      }
    };

    MapT<Key, Value, Void> map =
      createMap(keySerializer, valueSerializer);

    map.put(key1, value1);
    map.put(key2, value2);

    shouldBe(map.materialize(), asList(KeyVal.of(key1, value1), KeyVal.of(key2, value2)));

    map.delete();
  }

  /**
   * Tests <a href="https://github.com/simerplaha/SwayDB/issues/308">Issue 308</a>
   * to create serialisers directly from native byte array without requiring explicit boxing.
   */
  @Test
  void createSerialiserFromPrimitiveByteArray() throws IOException {
    byte[] key1 = {1, 2, 3, 4};
    byte[] key2 = {5, 6, 7, 8};

    byte[] value1 = {9, 10, 11, 12};
    byte[] value2 = {13, 14, 15, 16};

    Serializer<byte[]> keySerializer = new Serializer<byte[]>() {
      @Override
      public Slice<Byte> write(byte[] data) {
        return Slice.ofJava(data);
      }

      @Override
      public byte[] read(Slice<Byte> slice) {
        return slice.toByteArray();
      }
    };

    Serializer<byte[]> valueSerializer = new Serializer<byte[]>() {
      @Override
      public Slice<Byte> write(byte[] data) {
        return Slice.ofJava(data);
      }

      @Override
      public byte[] read(Slice<Byte> slice) {
        return slice.toByteArray();
      }
    };

    MapT<byte[], byte[], Void> map =
      createMap(keySerializer, valueSerializer);

    //put
    map.put(key1, value1);
    map.put(key2, value2);

    //get
    shouldContain(map.get(key1), value1);
    shouldContain(map.get(key2), value2);

    //stream
    Iterator<KeyVal<byte[], byte[]>> materialized = map.materialize().iterator();

    KeyVal<byte[], byte[]> keyValue1 = materialized.next();
    shouldBe(keyValue1.key(), key1);
    shouldBe(keyValue1.value(), value1);

    KeyVal<byte[], byte[]> keyValue2 = materialized.next();
    shouldBe(keyValue2.key(), key2);
    shouldBe(keyValue2.value(), value2);

    map.delete();
  }

}
