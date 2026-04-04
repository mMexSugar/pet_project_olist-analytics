import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import pyarrow as pa
import csv
import io
import logging

# Конфигурация всех таблиц датасета Olist
TABLE_CONFIGS = {
    'customers': {
        'columns': ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state'],
    },
    'geolocation': {
        'columns': ['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng', 'geolocation_city', 'geolocation_state'],
    },
    'order_items': {
        'columns': ['order_id', 'order_item_id', 'product_id', 'seller_id', 'shipping_limit_date', 'price', 'freight_value'],
    },
    'order_payments': {
        'columns': ['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value'],
    },
    'order_reviews': {
        'columns': ['review_id', 'order_id', 'review_score', 'review_comment_title', 'review_comment_message', 'review_creation_date', 'review_answer_timestamp'],
    },
    'orders': {
        'columns': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date'],
    },
    'products': {
        'columns': ['product_id', 'product_category_name', 'product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'],
    },
    'sellers': {
        'columns': ['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state'],
    },
    'category_translation': {
        'columns': ['product_category_name', 'product_category_name_english'],
    }
}

class ParseCSVRow(beam.DoFn):
    """Парсит строку CSV с учетом кавычек и сложных текстовых полей."""
    def __init__(self, table_name):
        self.table_name = table_name

    def process(self, element):
        try:
            f = io.StringIO(element)
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                columns = TABLE_CONFIGS[self.table_name]['columns']
                yield dict(zip(columns, row))
        except Exception as e:
            logging.error(f"Ошибка при парсинге строки в таблице {self.table_name}: {e}")

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', required=True)
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    args, beam_args = parser.parse_known_args(argv)

    if args.table not in TABLE_CONFIGS:
        raise ValueError(f"Таблица {args.table} не найдена!")

    pipeline_options = PipelineOptions(beam_args, save_main_session=True)
    
    columns = TABLE_CONFIGS[args.table]['columns']
    parquet_schema = pa.schema([(col, pa.string()) for col in columns])
    # ---------------------------

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p 
            | 'Read CSV' >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            | f'Parse {args.table}' >> beam.ParDo(ParseCSVRow(args.table))
            | f'Write {args.table} to Parquet' >> beam.io.WriteToParquet(
                file_path_prefix=args.output,
                schema=parquet_schema,
                file_name_suffix='.parquet'
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()