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
