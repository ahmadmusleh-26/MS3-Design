import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

# The measurements that must all be present in a record
MEASUREMENTS = ['temperature', 'humidity', 'pressure']


def has_all_measurements(record):
    # Keep the record only if none of the measurements is missing (None)
    return all(record.get(m) is not None for m in MEASUREMENTS)


def convert_units(record):
    record = dict(record)
    record['pressure'] = float(record['pressure']) / 6.895            # kPa -> psi
    record['temperature'] = float(record['temperature']) * 1.8 + 32   # Celsius -> Fahrenheit
    return record


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True,
                        help='Input Pub/Sub topic: projects/<project>/topics/<topic>')
    parser.add_argument('--output', dest='output', required=True,
                        help='Output Pub/Sub topic: projects/<project>/topics/<topic>')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    # Reading from Pub/Sub is an unbounded source, so the job must run in streaming mode
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        (p
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic=known_args.input)
            | 'toDict' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
            | 'Filter' >> beam.Filter(has_all_measurements)
            | 'Convert' >> beam.Map(convert_units)
            | 'toBytes' >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
            | 'Write to Pub/Sub' >> beam.io.WriteToPubSub(topic=known_args.output))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
