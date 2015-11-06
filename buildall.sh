#!/bin/bash

./sbt -J-Xmx2G "project proj4" test "project vector-test" test "project raster-test" test "project geotools" compile "project benchmark" compile "project demo" compile "project vector-benchmark" compile "project spark" package "project spark" test "project spark" test "project dev" compile "project services" compile "project jetty" compile "project admin" compile "project slick" test:compile "project gdal" test:compile
