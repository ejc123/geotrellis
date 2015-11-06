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

package geotrellis.engine

import geotrellis._
import geotrellis.engine.actors._

import akka.actor._
import akka.pattern.ask
import akka.routing.FromConfig

import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory

import scala.collection.mutable

class Engine(id: String, val catalog: Catalog) extends Serializable {
  val debug = false

  private[engine] val layerLoader = new LayerLoader(this)

  private[this] val cache = new HashCache[String]()

  catalog.initCache(cache)

  var actor: akka.actor.ActorRef = Engine.actorSystem.actorOf(Props(classOf[EngineActor], this), id)

  Engine.startActorSystem
  def system = Engine.actorSystem

  def startUp: Unit = ()

  def shutdown(): Unit = { 
    Engine.actorSystem.shutdown()
    Engine.actorSystem.awaitTermination()
  }

  private val routers = mutable.Map[String, ActorRef]()
  def getRouter(): ActorRef = getRouter("clusterRouter")
  def getRouter(routerName: String): ActorRef = {
    if(!routers.contains(routerName)) { 
      routers(routerName) = 
        system.actorOf(
          Props.empty.withRouter(FromConfig),
          name = routerName)
    }
    routers(routerName)
  }

  def log(msg: String) = if(debug) println(msg)

  def get[T](src: OpSource[T]): T =
    run(src) match {
      case Complete(value, _) => value
      case Error(msg, trace) =>
        println(s"Operation Error. Trace: $trace")
        sys.error(msg)
    }
  
  def get[T](op: Op[T]): T = 
    run(op) match {
      case Complete(value, _) => value
      case Error(msg, trace) =>
        println(s"Operation Error. Trace: $trace")
        sys.error(msg)
    }

  def run[T](src: OpSource[T]): OperationResult[T] =
    run(src.convergeOp)

  def run[T](op: Op[T]): OperationResult[T] = 
    _run(op)

  def layerExists(layerId: LayerId): Boolean = catalog.layerExists(layerId)
  def layerExists(layerName: String): Boolean = catalog.layerExists(LayerId(None, layerName))

  private[engine] def _run[T](op: Op[T]): OperationResult[T] = {
    log(s"engine._run called with $op")

    val d = Duration.create(60000, TimeUnit.SECONDS)
    implicit val t = Timeout(d)
    val future = 
        (actor ? Run(op)).mapTo[PositionedResult[T]]

    val result = Await.result(future, d)

    result match {
      case PositionedResult(c: Complete[_], _) => c.asInstanceOf[Complete[T]]
      case PositionedResult(e: Error, _) => e
      case r => sys.error(s"unexpected status: $r")
    }
  }
}

object Engine {
  def apply(id: String, path: String) = new Engine(id, Catalog.fromPath(path))
  def apply(id: String, catalog: Catalog) = new Engine(id, catalog)
  def empty(id: String) = new Engine(id, Catalog.empty(id))

  var actorSystem: akka.actor.ActorSystem = akka.actor.ActorSystem("GeoTrellis", ConfigFactory.load())

  def startActorSystem {
    if (actorSystem.isTerminated) {
      actorSystem = akka.actor.ActorSystem("GeoTrellis", ConfigFactory.load())
    } 
  }
}
