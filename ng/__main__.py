#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import logging
import os
from ng.generator import generate_noise
from pyspark.sql import SparkSession


def create_parser():
    parser = argparse.ArgumentParser(
        description='Introduce some noise into a given dataset.')
    parser.add_argument('-i', '--input', help='The input dataset',
                        required=True)
    parser.add_argument('-if', '--input-format',
                        help='The format of the input dataset', default='csv')
    parser.add_argument('-o', '--output', help='The output dataset',
                        required=True)
    parser.add_argument('-of', '--output-format',
                        help='The format of the output path', default='csv')
    parser.add_argument('--inferSchema', dest='inferSchema',
                        action='store_true')
    parser.add_argument('--header', dest='header', action='store_true')
    parser.add_argument('-quote', '--quote',
                        help='The default value used for the quote',
                        default='"')
    parser.add_argument('-null', '--null',
                        help='The default value used for representing '
                             'null values',
                        default='null')
    parser.add_argument('-n', '--noise',
                        help='The noise that that will be inserted '
                             'into the data',
                        required=True)
    parser.add_argument('-d', '--distribution',
                        help='The distribution followed by '
                             'the noise generators',
                        required=True)
    parser.add_argument('-extra-noise', '--extra-noise',
                        help='The noise generator extra parameters',
                        default=None)
    parser.add_argument('-extra-distribution', '--extra-distribution',
                        help='The distribution extra parameters', default=None)
    parser.add_argument('-cols', '--columns',
                        help='The list of columns where the noise '
                             'will be generated',
                        type=str)
    parser.add_argument('-ids', '--id-columns',
                        help='The column list where the noise '
                             'will NOT be generated',
                        type=str)
    return parser


def read_dataset(spark_session, input_path, input_format, infer_schema, header,
                 null, quote):
    reader = spark_session.read
    input_format = input_format.lower().replace('.', '').strip()
    if input_format == 'csv':
        return reader.csv(input_path, inferSchema=infer_schema, header=header,
                          nullValue=null, quote=quote)
    elif input_format == 'json':
        return reader.json(input_path, inferSchema=infer_schema, header=header,
                           nullValue=null, quote=quote)
    elif input_format == 'parquet':
        return reader.parquet(input_path, inferSchema=infer_schema,
                              header=header, nullValue=null, quote=quote)
    else:
        logging.WARN(
            'The input format {} is currently not tested.'.format(input_format))
        return spark_session.read.load(input, inferSchema=infer_schema,
                                       header=header, nullValue=null,
                                       quote=quote)


def write_dataset(data, output_path, output_format, header, null, quote):
    output_format = output_format.lower()
    if output_format == 'csv':
        data.write.mode('overwrite').csv(output_path, header=header,
                                         nullValue=null, quote=quote)
    elif output_format == 'json':
        data.write.mode('overwrite').json(output_path, header=header,
                                          nullValue=null, quote=quote)
    elif output_format == 'parquet':
        data.write.mode('overwrite').parquet(output_path, header=header,
                                             nullValue=null, quote=quote)
    else:
        logging.WARN('The output format {} is currently not tested.'.format(
            output_format))


def get_columns(data, given_columns, id_columns):
    if not given_columns or given_columns is None:
        cols = list(range(len(data.columns)))
    else:
        cols = list(map(int, given_columns.split(',')))
    if id_columns:
        return [int(col) for col in cols if col not in id_columns.split(',')]
    return cols


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger('org').setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger('akka').setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger('org.apache.spark').setLevel(logger.Level.ERROR)


def create_session(params):
    app_name = 'NoiseGenerator - {} - {} - {} - {}'.format(
        os.path.basename(params.input),
        params.noise,
        params.distribution,
        params.extra_distribution)
    builder = SparkSession.builder.appName(app_name) \
        .config('spark.hadoop.validateOutputSpecs', 'false') \
        .config('spark.rpc.askTimeout', '1200s') \
        .config('spark.network.timeout', '1200s') \
        .config('spark.sql.broadcastTimeout', '1200') \
        .config('spark.driver.maxResultSize', '0') \
        .config('spark.ui.showConsoleProgress', 'false')

    if os.path.exists('/Users/forons'):
        builder = builder.config('spark.driver.host', 'localhost').master(
            'local[*]')
    spark_session = builder.getOrCreate()
    quiet_logs(spark_session.sparkContext)
    return spark_session


if __name__ == '__main__':
    args = create_parser().parse_args()
    print(args)
    spark = create_session(args)

    df = read_dataset(spark, args.input, args.input_format, args.inferSchema,
                      args.header, args.null, args.quote)

    columns = get_columns(df, args.columns, args.id_columns)
    output = generate_noise(df, args.noise, args.distribution, columns,
                            args.extra_noise, args.extra_distribution)

    write_dataset(output, args.output, args.output_format, args.header,
                  args.null, args.quote)
