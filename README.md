# Kafka and Spark Sensor Alerts

A Structured Streaming example that aggregates Kafka sensor events in sliding windows and evaluates configurable alert rules.

## Data flow

1. `producer_part.py` publishes simulated temperature and humidity readings.
2. `consumer_part.py` parses the Kafka stream, applies a 10-second watermark, and calculates one-minute averages every 30 seconds.
3. Static rules from `alerts_conditions.csv` are cross-joined with each aggregate window.
4. Matching alerts are written to Kafka.
5. `listener_part.py` displays the emitted alert events.

## Setup

1. Install Java and Apache Spark prerequisites.
2. Create a virtual environment and run `pip install -r requirements.txt`.
3. Copy `.env.example` to `.env` and provide Kafka credentials.
4. Export those variables in the shell or IDE.
5. Start the producer, streaming consumer, and listener in separate terminals.

## Configuration

`configs.py` reads broker credentials from environment variables. The checked-in `.env.example` contains placeholders only, while `.gitignore` prevents a real `.env` from being committed.

## Notes

The Spark code contains both a combined output stream and separate temperature/humidity output examples. Checkpoint directories are generated per run under `/tmp/oksana_chk`.
