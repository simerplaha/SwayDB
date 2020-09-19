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
import swaydb.Exception;
import swaydb.*;
import swaydb.core.Core;
import swaydb.data.java.TestBase;
import swaydb.java.serializers.Serializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static swaydb.PureFunctionJava.*;
import static swaydb.data.java.JavaTest.*;
import static swaydb.java.serializers.Default.intSerializer;
import static swaydb.java.serializers.Default.stringSerializer;

abstract class MapFunctionsOnTest extends TestBase {

  public abstract boolean isPersistent();

  public abstract <K, V> MapT<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                                Serializer<V> valueSerializer,
                                                                                List<PureFunction<K, V, Apply.Map<V>>> functions) throws IOException;

  public abstract <K, V> MapT<K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<K> keySerializer,
                                                                                Serializer<V> valueSerializer,
                                                                                List<PureFunction<K, V, Apply.Map<V>>> functions,
                                                                                KeyComparator<K> keyComparator) throws IOException;


  OnKey<Integer, String> appendUpdated =
    (key) ->
      Apply.update(key + " updated");

  OnKeyValue<Integer, String> incrementBy1 =
    (key, value) ->
      Apply.update(key + 1 + "");

  OnValue<Integer, String> doNothingWithValue = Apply::update;

  OnKeyValue<Integer, String> removeMod0OrIncrementBy1 =
    (key, value) -> {
      if (key % 10 == 0) {
        return Apply.removeFromMap();
      } else {
        return Apply.update(key + 1 + "");
      }
    };

  //add this function will not compile - invalid types!
  OnKeyValue<String, String> invalidType1 =
    (key, value) ->
      Apply.update(value + 1);

  //add this function will not compile - invalid types!
  OnKeyValue<String, Integer> invalidType2 =
    (key, value) ->
      Apply.update(value + 1);

  @Test
  void reportMissingFunction() throws IOException {
    assumeTrue(this::isPersistent, "IGNORED! Test not needed for memory databases");

    MapT<Integer, String, PureFunction<Integer, String, Apply.Map<String>>> map =
      createMap(intSerializer(), stringSerializer(), Collections.emptyList());

    OnKeyValue<Integer, String> missingFunction =
      (key, value) ->
        Apply.update(value + 1);

    Exception.FunctionNotFound functionNotFound = assertThrows(Exception.FunctionNotFound.class, () -> map.applyFunction(1, missingFunction));
    shouldInclude(functionNotFound.getMessage(), missingFunction.id());
    shouldIncludeIgnoreCase(functionNotFound.getMessage(), "not found");

    map.delete();
  }

  @Test
  void reportMissingFunctionOnReboot() throws IOException {
    assumeTrue(this::isPersistent, "IGNORED! Test not needed for memory databases");

    MapT<Integer, String, PureFunction<Integer, String, Apply.Map<String>>> map =
      createMap(intSerializer(), stringSerializer(), Arrays.asList(appendUpdated, incrementBy1, removeMod0OrIncrementBy1, doNothingWithValue));

    map.applyFunction(1, appendUpdated);
    map.applyFunction(2, removeMod0OrIncrementBy1);

    map.close();

    Exception.MissingFunctions exception = assertThrows(Exception.MissingFunctions.class, () -> createMap(intSerializer(), stringSerializer(), Collections.emptyList()));

    shouldContainTheSameElementsAs(exception.asJava(), Arrays.asList(appendUpdated.id(), removeMod0OrIncrementBy1.id()));

    map.delete();
  }

  @Test
  void notReportUnAppliedMissingFunctionOnReboot() throws IOException {
    assumeTrue(this::isPersistent, "IGNORED! Test not needed for memory databases");

    MapT<Integer, String, PureFunction<Integer, String, Apply.Map<String>>> map =
      createMap(intSerializer(), stringSerializer(), Arrays.asList(appendUpdated, incrementBy1, removeMod0OrIncrementBy1));

    map.close();

    MapT<Integer, String, PureFunction<Integer, String, Apply.Map<String>>> reopened =
      assertDoesNotThrow(() -> createMap(intSerializer(), stringSerializer(), Collections.emptyList()));

    reopened.put(1, "one");
    shouldContain(reopened.get(1), "one");

    //closed databases cannot process messages
    IllegalAccessException illegalAccessException = assertThrows(IllegalAccessException.class, () -> map.get(1));
    shouldBe(illegalAccessException.getMessage(), Core.closedMessage());

    reopened.delete();
  }

  @Test
  void registerAndApplyFunction() throws IOException {
    MapT<Integer, String, PureFunction<Integer, String, Apply.Map<String>>> map =
      createMap(intSerializer(), stringSerializer(), Arrays.asList(appendUpdated, incrementBy1, removeMod0OrIncrementBy1, doNothingWithValue));

    map.put(Stream.range(1, 100).map(integer -> KeyVal.of(integer, integer + "")));

    map.applyFunction(1, appendUpdated);
    shouldContain(map.get(1), "1 updated");

    map.applyFunction(10, 20, incrementBy1);
    foreachRange(10, 20, key -> shouldContain(map.get(key), key + 1 + ""));

    map.applyFunction(21, 50, removeMod0OrIncrementBy1);

    map.applyFunction(21, 50, doNothingWithValue);

    foreachRange(
      21,
      50,
      key -> {
        if (key % 10 == 0) {
          shouldBeEmpty(map.get(key));
        } else {
          shouldContain(map.get(key), key + 1 + "");
        }
      }
    );

    //untouched 51 - 100. Overlapping functions executions.
    map.commit(
      Arrays.asList(
        Prepare.applyMapFunction(51, appendUpdated),
        Prepare.applyMapFunction(52, 60, appendUpdated),
        Prepare.applyMapFunction(61, 80, incrementBy1)
      )
    );

    shouldContain(map.get(51), "51 updated");
    foreachRange(51, 60, key -> shouldContain(map.get(key), key + " updated"));
    foreachRange(61, 80, key -> shouldContain(map.get(key), key + 1 + ""));

    map.delete();
  }

  //similar Scala test can be found in SwayDBFunctionsSpec
  @Test
  void applyAllFunctions() throws IOException {
    //test head should have 1
    OnKey<Integer, String> onKey = (key) -> Apply.update(1 + " " + randomString(10));
    //test head should have 2
    OnKeyExpiration<Integer, String> onKeyExpiration = (key, deadline) -> Apply.update(2 + " " + key + " set key");
    //test head should have 3
    OnKeyValue<Integer, String> onKeyValue = (key, value) -> Apply.update(3 + " " + value + " " + randomString(10));
    //test head should have 4
    OnValue<Integer, String> onValue = value -> Apply.update(4 + " " + value + " " + randomString(10));
    //test head should have original value but deadline is set
    OnValueExpiration<Integer, String> onValueExpiration =
      (value, expiration) -> {
        if (expiration.isPresent()) {
          return Apply.nothingOnMap();
        } else {
          return Apply.expireFromMap(Duration.ofSeconds(100));
        }
      };
    //test no change
    OnKeyValueExpiration<Integer, String> onKeyValueExpiration = (key, value, expiration) -> Apply.nothingOnMap();

    MapT<Integer, String, PureFunction<Integer, String, Apply.Map<String>>> map =
      createMap(
        intSerializer(),
        stringSerializer(),
        Arrays.asList(onKey, onKeyExpiration, onKeyValue, onValue, onValueExpiration, onKeyValueExpiration)
      );


    foreachRange(1, 60, (integer) -> map.put(integer, ""));

    //Write using range or individual
    eitherOne(
      () -> foreachRange(1, 10, (integer) -> map.applyFunction(integer, onKey)),
      () -> map.applyFunction(1, 10, onKey)
    );

    eitherOne(
      () -> foreachRange(11, 20, (integer) -> map.applyFunction(integer, onKeyExpiration)),
      () -> map.applyFunction(11, 20, onKeyExpiration)
    );

    eitherOne(
      () -> foreachRange(21, 30, (integer) -> map.applyFunction(integer, onKeyValue)),
      () -> map.applyFunction(21, 30, onKeyValue)
    );

    eitherOne(
      () -> foreachRange(31, 40, (integer) -> map.applyFunction(integer, onValue)),
      () -> map.applyFunction(31, 40, onValue)
    );

    eitherOne(
      () -> foreachRange(41, 50, (integer) -> map.applyFunction(integer, onValueExpiration)),
      () -> map.applyFunction(41, 50, onValueExpiration)
    );

    eitherOne(
      () -> foreachRange(51, 60, (integer) -> map.applyFunction(integer, onKeyValueExpiration)),
      () -> map.applyFunction(51, 60, onKeyValueExpiration)
    );

    //assert
    foreachRange(
      1,
      10,
      integer -> {
        shouldStartWith(map.get(integer), "1");
        shouldBeEmpty(map.expiration(integer));
      }
    );

    foreachRange(
      11,
      20,
      integer -> {
        shouldStartWith(map.get(integer), "2");
        shouldBeEmpty(map.expiration(integer));
      }
    );

    foreachRange(
      21,
      30,
      integer -> {
        shouldStartWith(map.get(integer), "3");
        shouldBeEmpty(map.expiration(integer));
      }
    );

    foreachRange(
      31,
      40,
      integer -> {
        shouldStartWith(map.get(integer), "4");
        shouldBeEmpty(map.expiration(integer));
      }
    );

    foreachRange(
      41,
      50,
      integer -> {
        shouldBeEmptyString(map.get(integer));
        shouldBeDefined(map.expiration(integer));
      }
    );

    foreachRange(
      51,
      60,
      integer -> {
        shouldBeEmptyString(map.get(integer));
        shouldBeEmpty(map.expiration(integer));
      }
    );

    map.delete();
  }
}
