/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core

import swaydb.core.data.KeyValue
import swaydb.serializers.Serializer

/**
  * Oh shit! A mutable setting. This is no good! - Temporary solution.
  *
  * Currently core does not understand Serializers. It only deals with raw bytes. But this is required
  * for [[KeyValue.ReadOnly.UpdateFunction]] types. Core will have to be changed a little and value serializers
  * should be passed in implicitly.
  */
object ValueSerializerHolder_OH_SHIT {

  var valueType: String = _
  var valueSerializer: Serializer[_] = _

}
