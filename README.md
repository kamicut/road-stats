# Simple road stats 🛣

Simple road statistics using [VectorPipe](https://github.com/geotrellis/vectorpipe) and OSM [ORC files](https://github.com/mojodna/osm2orc).

This repo aims to answer "What changed in this area after this date?".

Currently:
- Number of roads added, deleted, modified
- Number of roads with oneway and surface tags

## Running

### Requirements:
- Scala 2.11
- Spark 2.2.0
- sbt
- geotrellis/vectorpipe locally built using `sbt publishLocal`

In the directory after cloning:

```sh
sbt assembly
spark-submit --class "StatsJob" target/scala-2.11/simple-stats-assembly-0.1.jar > result.log
```

## License
vectorpipe licensed using Apache © Azavea
Code in this repo licensed using MIT © Marc Farra
