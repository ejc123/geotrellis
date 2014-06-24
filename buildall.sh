#!/bin/bash

./sbt -J-Xmx2G "project feature-test" test "project core-test" test "project geotools" compile "project tasks" compile "project benchmark" compile "project demo" compile "project feature-benchmark" compile "project dev" compile "project services" compile "project jetty" compile "project admin" compile "project slick" test:compile
