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

import swaydb.Apply;
import swaydb.PureFunction;
import swaydb.java.serializers.Serializer;

import java.io.IOException;
import java.util.List;

abstract class MultiMapFunctionsOnTest extends TestBase {


  public abstract <M, K, V> MultiMap<M, K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<M> mapKeySerializer,
                                                                                          Serializer<K> keySerializer,
                                                                                          Serializer<V> valueSerializer,
                                                                                          List<PureFunction<K, V, Apply.Map<V>>> functions) throws IOException;

  public abstract <M, K, V> MultiMap<M, K, V, PureFunction<K, V, Apply.Map<V>>> createMap(Serializer<M> mapKeySerializer,
                                                                                          Serializer<K> keySerializer,
                                                                                          Serializer<V> valueSerializer,
                                                                                          List<PureFunction<K, V, Apply.Map<V>>> functions,
                                                                                          KeyComparator<K> keyComparator) throws IOException;
}
