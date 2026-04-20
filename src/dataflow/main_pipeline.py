import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import pyarrow as pa
import csv
import io
import logging
from datetime import datetime


T_STR = pa.string()
T_INT = pa.int64()
T_FLOAT = pa.float64()
T_TS = pa.timestamp('s')

# Конфигурация всех таблиц датасета Olist
TABLE_SCHEMAS = {
    'customers': pa.schema([
        ('customer_id', T_STR),
        ('customer_unique_id', T_STR),
        ('customer_zip_code_prefix', T_STR),
        ('customer_city', T_STR),
        ('customer_state', T_STR)
    ]),
    
    'geolocation': pa.schema([
        ('geolocation_zip_code_prefix', T_STR),
        ('geolocation_lat', T_FLOAT),
        ('geolocation_lng', T_FLOAT),
        ('geolocation_city', T_STR),
        ('geolocation_state', T_STR)
    ]),
    
    'order_items': pa.schema([
        ('order_id', T_STR),
        ('order_item_id', T_INT),
        ('product_id', T_STR),
        ('seller_id', T_STR),
        ('shipping_limit_date', T_TS),
        ('price', T_FLOAT),
        ('freight_value', T_FLOAT)
    ]),
    
    'order_payments': pa.schema([
        ('order_id', T_STR),
        ('payment_sequential', T_INT),
        ('payment_type', T_STR),
        ('payment_installments', T_INT),
        ('payment_value', T_FLOAT)
    ]),
    
    'order_reviews': pa.schema([
        ('review_id', T_STR),
        ('order_id', T_STR),
        ('review_score', T_INT),
        ('review_comment_title', T_STR),
        ('review_comment_message', T_STR),
        ('review_creation_date', T_TS),
        ('review_answer_timestamp', T_TS)
    ]),
    
    'orders': pa.schema([
        ('order_id', T_STR),
        ('customer_id', T_STR),
        ('order_status', T_STR),
        ('order_purchase_timestamp', T_TS),
        ('order_approved_at', T_TS),
        ('order_delivered_carrier_date', T_TS),
        ('order_delivered_customer_date', T_TS),
        ('order_estimated_delivery_date', T_TS)
    ]),
    
    'products': pa.schema([
        ('product_id', T_STR),
        ('product_category_name', T_STR),
        ('product_name_lenght', T_FLOAT),
        ('product_description_lenght', T_FLOAT),
        ('product_photos_qty', T_FLOAT),
        ('product_weight_g', T_FLOAT),
        ('product_length_cm', T_FLOAT),
        ('product_height_cm', T_FLOAT),
        ('product_width_cm', T_FLOAT)
    ]),
    
    'sellers': pa.schema([
        ('seller_id', T_STR),
        ('seller_zip_code_prefix', T_STR),
        ('seller_city', T_STR),
        ('seller_state', T_STR)
    ]),
    
    'category_translation': pa.schema([
        ('product_category_name', T_STR),
        ('product_category_name_english', T_STR)
    ])
}

class ParseCSVRow(beam.DoFn):
    def __init__(self, table_name):
        self.table_name = table_name
        self.columns = TABLE_SCHEMAS[table_name].names

    def process(self, element):
        try:
            f = io.StringIO(element)
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                yield dict(zip(self.columns, row))
        except Exception as e:
            logging.error(f"Ошибка в {self.table_name}: {e}")

class TypeCastingDoFn(beam.DoFn):
    def __init__(self, table_name):
        self.table_name = table_name

    def process(self, element):
        schema = TABLE_SCHEMAS[self.table_name]
        typed_row = {}

        for field in schema:
            name = field.name
            target_type = field.type
            val = element.get(name)

            if val is None or val == '':
                typed_row[name] = None
                continue

            try:
                if target_type == pa.int64():
                    typed_row[name] = int(float(val))
                elif target_type == pa.float64():
                    typed_row[name] = float(val)
                elif target_type == pa.timestamp('s'):
                    typed_row[name] = datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
                else:
                    typed_row[name] = str(val)
            except Exception:
                typed_row[name] = None

        yield typed_row

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', required=True)
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    args, beam_args = parser.parse_known_args(argv)

    if args.table not in TABLE_SCHEMAS:
        raise ValueError(f"Таблица {args.table} не найдена!")

    pipeline_options = PipelineOptions(beam_args, save_main_session=False)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p 
            | 'Read CSV' >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            | 'Parse CSV' >> beam.ParDo(ParseCSVRow(args.table))
            | 'Cast Types' >> beam.ParDo(TypeCastingDoFn(args.table))
            | 'Write to Parquet' >> beam.io.WriteToParquet(
                file_path_prefix=args.output,
                schema=TABLE_SCHEMAS[args.table],
                file_name_suffix='.parquet'
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()