"""Defines trends calculations for stations"""
import logging
from dataclasses import dataclass
import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)
table = app.Table(
   "stations_count",
   default=int,
   partitions=4,
   changelog_topic=out_topic,
)

@app.agent(topic)
async def transform_input(station_records):
    async for station in station_records:
        line = ""

        if station.red == True:
            line = "red"

        if station.green == True:
            line = "green"

        if station.blue == True:
            line = "blue"

        transformed = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line
        )

        table[station.station_id] = transformed

if __name__ == "__main__":
    app.main()
