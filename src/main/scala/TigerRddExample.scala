/**
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

import Main.resourceFolder
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialOperator.{JoinQuery, SpatialPredicate}
import org.apache.sedona.core.spatialRDD.{CircleRDD, SpatialRDD}
import org.apache.sedona.sql.utils.Adapter
import org.apache.sedona.viz.core.{ImageGenerator, RasterOverlayOperator}
import org.apache.sedona.viz.extension.visualizationEffect.{HeatMap, ScatterPlot}
import org.apache.sedona.viz.utils.ImageType
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Geometry

import java.awt.Color

object TigerRddExample {

  val arealmFileLocation = resourceFolder+"tiger/arealm"
  val arealWaterFileLocation = resourceFolder+"tiger/areawater"

  def runTigerQuery(sedona: SparkSession): Unit =
  {

    // Prepare NYC area landmarks which includes airports, museums, colleges, hospitals
    var arealmRDD = new SpatialRDD[Geometry]()
    var areaWaterRDD = new SpatialRDD[Geometry]()

    arealmRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, arealmFileLocation)
    areaWaterRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, arealWaterFileLocation)

    val spatialPredicate = SpatialPredicate.TOUCHES // Only return gemeotries fully covered by each query window in queryWindowRDD
    arealmRDD.analyze()
    arealmRDD.spatialPartitioning(GridType.KDBTREE)

    areaWaterRDD.analyze()
    areaWaterRDD.spatialPartitioning(arealmRDD.getPartitioner)

    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    val usingIndex = true
    areaWaterRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
    areaWaterRDD.indexedRDD = areaWaterRDD.indexedRDD.cache()

    val result = JoinQuery.SpatialJoinQuery(arealmRDD, areaWaterRDD, usingIndex, spatialPredicate)
    System.out.println(result.count())
  }
}

