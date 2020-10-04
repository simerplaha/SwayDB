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

import org.junit.jupiter.api.AfterEach;
import swaydb.java.eventually.persistent.EventuallyPersistentMultiMap;
import swaydb.java.memory.MemoryMultiMap;
import swaydb.java.persistent.PersistentMultiMap;
import swaydb.java.serializers.Serializer;

import java.io.IOException;

/**
 * MultiMapFunctionsOff
 */
class MemoryMultiMapFunctions_Off_Test extends MultiMapFunctionsOffTest {

  public <M, K, V> MultiMap<M, K, V, Void> createMap(Serializer<M> mapKeySerializer,
                                                     Serializer<K> keySerializer,
                                                     Serializer<V> valueSerializer) {
    return
      MemoryMultiMap
        .functionsOff(mapKeySerializer, keySerializer, valueSerializer)
        .get();
  }


  @Override
  public <M, K, V> MultiMap<M, K, V, Void> createMap(Serializer<M> mapKeySerializer,
                                                     Serializer<K> keySerializer,
                                                     Serializer<V> valueSerializer,
                                                     KeyComparator<K> keyComparator) {
    return
      MemoryMultiMap
        .functionsOff(mapKeySerializer, keySerializer, valueSerializer)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}

class PersistentMultiMapFunctions_Off_Test extends MultiMapFunctionsOffTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <M, K, V> MultiMap<M, K, V, Void> createMap(Serializer<M> mapKeySerializer,
                                                     Serializer<K> keySerializer,
                                                     Serializer<V> valueSerializer) throws IOException {

    return
      PersistentMultiMap
        .functionsOff(testDir(), mapKeySerializer, keySerializer, valueSerializer)
        .get();
  }

  @Override
  public <M, K, V> MultiMap<M, K, V, Void> createMap(Serializer<M> mapKeySerializer,
                                                     Serializer<K> keySerializer,
                                                     Serializer<V> valueSerializer,
                                                     KeyComparator<K> keyComparator) throws IOException {
    return
      PersistentMultiMap
        .functionsOff(testDir(), mapKeySerializer, keySerializer, valueSerializer)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}

class EventuallyPersistentMultiMapFunctions_Off_Test extends MultiMapFunctionsOffTest {

  @AfterEach
  void deleteDir() throws IOException {
    deleteTestDir();
  }

  public <M, K, V> MultiMap<M, K, V, Void> createMap(Serializer<M> mapKeySerializer,
                                                     Serializer<K> keySerializer,
                                                     Serializer<V> valueSerializer) throws IOException {

    return
      EventuallyPersistentMultiMap
        .functionsOff(testDir(), mapKeySerializer, keySerializer, valueSerializer)
        .get();
  }

  @Override
  public <M, K, V> MultiMap<M, K, V, Void> createMap(Serializer<M> mapKeySerializer,
                                                     Serializer<K> keySerializer,
                                                     Serializer<V> valueSerializer,
                                                     KeyComparator<K> keyComparator) throws IOException {
    return
      EventuallyPersistentMultiMap
        .functionsOff(testDir(), mapKeySerializer, keySerializer, valueSerializer)
        .setTypedKeyComparator(keyComparator)
        .get();
  }
}
