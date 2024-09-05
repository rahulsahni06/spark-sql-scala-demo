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

import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialOperator.{JoinQuery, SpatialPredicate}
import org.apache.sedona.core.spatialRDD.{SpatialRDD}
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Geometry

object TigerRddExample {

  val shapeFileLocation = "/user/rahulsahni/dataset/tiger_dataset/shape_separate/"

  def runTigerQuery(sedona: SparkSession): Unit =
  {

    val startTime = System.currentTimeMillis()

    var arealmRDD = new SpatialRDD[Geometry]()
    var areaWaterRDD = new SpatialRDD[Geometry]()

    arealmRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, shapeFileLocation + "arealm")
    areaWaterRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, shapeFileLocation + "areawater")

    val spatialPredicate = SpatialPredicate.TOUCHES // Only return gemeotries fully covered by each query window in queryWindowRDD
    arealmRDD.analyze()
    arealmRDD.spatialPartitioning(GridType.KDBTREE)

    areaWaterRDD.analyze()
    areaWaterRDD.spatialPartitioning(arealmRDD.getPartitioner)

    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    val usingIndex = true

    val indexStartTime = System.currentTimeMillis()
    areaWaterRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
    areaWaterRDD.indexedRDD = areaWaterRDD.indexedRDD.cache()
    val indexEndTime = System.currentTimeMillis()

    val executionStartTime = System.currentTimeMillis()
    val result = JoinQuery.SpatialJoinQuery(arealmRDD, areaWaterRDD, usingIndex, spatialPredicate)
    val resultCount = result.count()
    val executionEndTime = System.currentTimeMillis()

    System.out.println("Total Time: "+ (executionEndTime - startTime) + "ms")
    System.out.println("Index Time: "+ (indexEndTime - indexStartTime) + "ms")
    System.out.println("Execution Time: "+ (executionEndTime - executionStartTime) + "ms")
    System.out.println("Result : "+ resultCount + " Tuples")

  }
}

