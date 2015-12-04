/*
 * Copyright (c) 2014 Azavea.
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

package geotrellis.vector

import com.vividsolutions.jts.geom
import com.vividsolutions.jts.geom.PrecisionModel
import com.vividsolutions.jts.precision.GeometryPrecisionReducer

private[vector] object GeomFactory {

  val factory = new geom.GeometryFactory()

  // 12 digits is maximum to avoid [[TopologyException]], see http://tsusiatsoftware.net/jts/jts-faq/jts-faq.html#D9
  lazy val simplifier = new GeometryPrecisionReducer(new PrecisionModel(12))

}