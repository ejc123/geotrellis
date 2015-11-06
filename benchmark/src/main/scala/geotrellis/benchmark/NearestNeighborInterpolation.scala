package geotrellis.benchmark

import geotrellis.raster.interpolation._

import com.google.caliper.Param

object NearestNeighborInterpolation
    extends BenchmarkRunner(classOf[NearestNeighborInterpolation])

class NearestNeighborInterpolation extends InterpolationBenchmark {

  def interp = NearestNeighbor

}
