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

package geotrellis

import com.vividsolutions.jts.{geom => jts}

import scala.collection.mutable
import scala.collection.JavaConversions._

package object vector extends SeqMethods {

  type PointFeature[D] = Feature[Point, D]
  type LineFeature[D] = Feature[Line, D]
  type PolygonFeature[D] = Feature[Polygon, D]
  type MultiPointFeature[D] = Feature[MultiPoint, D]
  type MultiLineFeature[D] = Feature[MultiLine, D]
  type MultiPolygonFeature[D] = Feature[MultiPolygon, D]
  type GeometryCollectionFeature[D] = Feature[GeometryCollection, D]


  implicit def tupleOfIntToPoint(t: (Double, Double)): Point =
    Point(t._1,t._2)

  implicit def tupleOfDoubleToPoint(t: (Int, Int)): Point =
    Point(t._1,t._2)

  implicit def coordinateToPoint(c: jts.Coordinate): Point =
    Point(c.x,c.y)

  implicit def tupleSeqToMultiPoint(ts: Set[(Double, Double)]): Set[Point] =
    ts map(t => Point(t._1, t._2))

  implicit def tupleListToPointList(tl: List[(Double, Double)]): List[Point] =
    tl map(t => Point(t._1, t._2))

  implicit def tupleListToPointList(tl: Seq[(Double, Double)]): Seq[Point] =
    tl map(t => Point(t._1, t._2))

  implicit def coordinateArrayToMultiPoint(ca: Array[jts.Coordinate]): MultiPoint = {
    val ps = (for (i <- 0 until ca.length) yield {
      Point(ca(i).x, ca(i).y)
    }).toSeq
    MultiPoint(ps)
  }

  implicit def pointListToCoordinateArray(ps: List[Point]): Array[jts.Coordinate] =
    ps map(p => new jts.Coordinate(p.x, p.y)) toArray

  implicit def multiPointToSeqPoint(mp: jts.MultiPoint): Seq[Point] = {
    val len = mp.getNumGeometries
      (for (i <- 0 until len) yield {
        Point(mp.getGeometryN(i).asInstanceOf[jts.Point])
      }).toSeq
  }

  implicit def multiLineToSeqLine(ml: jts.MultiLineString): Seq[Line] = {
    val len = ml.getNumGeometries
    (for (i <- 0 until len) yield {
      Line(ml.getGeometryN(i).asInstanceOf[jts.LineString])
    }).toSeq
  }

  implicit def multiPolygonToSeqPolygon(mp: jts.MultiPolygon): Seq[Polygon] = {
    val len = mp.getNumGeometries
    (for (i <- 0 until len) yield {
      Polygon(mp.getGeometryN(i).asInstanceOf[jts.Polygon])
    }).toSeq
  }

  implicit def geometryCollectionToSeqGeometry(gc: jts.GeometryCollection): Seq[Geometry] = {
    val len = gc.getNumGeometries
    (for (i <- 0 until len) yield {
      gc.getGeometryN(i) match {
        case p: jts.Point => Seq[Geometry](Point(p))
        case mp: jts.MultiPoint => multiPointToSeqPoint(mp)
        case l: jts.LineString => Seq[Geometry](Line(l))
        case ml: jts.MultiLineString => multiLineToSeqLine(ml)
        case p: jts.Polygon => Seq[Geometry](Polygon(p))
        case mp: jts.MultiPolygon => multiPolygonToSeqPolygon(mp)
        case gc: jts.GeometryCollection => geometryCollectionToSeqGeometry(gc)
      }
    }).toSeq.flatten
  }

  implicit def seqPointToMultiPoint(ps: Seq[Point]): MultiPoint = MultiPoint(ps)
  implicit def arrayPointToMultiPoint(ps: Array[Point]): MultiPoint = MultiPoint(ps)

  implicit def seqLineToMultiLine(ps: Seq[Line]): MultiLine = MultiLine(ps)
  implicit def arrayLineToMultiLine(ps: Array[Line]): MultiLine = MultiLine(ps)

  implicit def seqPolygonToMultiPolygon(ps: Seq[Polygon]): MultiPolygon = MultiPolygon(ps)
  implicit def arrayPolygonToMultiPolygon(ps: Array[Polygon]): MultiPolygon = MultiPolygon(ps)

  implicit def seqGeometryToGeometryCollection(gs: Seq[Geometry]): GeometryCollection = {
    val points = mutable.ListBuffer[Point]()
    val lines = mutable.ListBuffer[Line]()
    val polygons = mutable.ListBuffer[Polygon]()
    val multiPoints = mutable.ListBuffer[MultiPoint]()
    val multiLines = mutable.ListBuffer[MultiLine]()
    val multiPolygons = mutable.ListBuffer[MultiPolygon]()
    val geometryCollections = mutable.ListBuffer[GeometryCollection]()

    for(g <- gs) {
      g match {
        case p: Point => points += p
        case l: Line => lines += l
        case p: Polygon => polygons += p
        case mp: MultiPoint => multiPoints += mp
        case ml: MultiLine => multiLines += ml
        case mp: MultiPolygon => multiPolygons += mp
        case gc: GeometryCollection => geometryCollections += gc
        case _ => sys.error(s"Unknown Geometry type: $g")
      }
    }
    GeometryCollection(points, lines, polygons,
                       multiPoints, multiLines, multiPolygons,
                       geometryCollections)
  }
}
