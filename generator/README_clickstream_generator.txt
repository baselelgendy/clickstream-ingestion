CLICKSTREAM & ABANDONED CART DATA GENERATOR

Files
- generate_clickstream_kaggle.py: generator for all 12 CSV tables.

1) Install Python dependencies
   pip install pandas numpy

2) Download the Kaggle dataset (requires Kaggle API credentials configured)
   pip install kaggle
   kaggle datasets download -d mkechinov/ecommerce-events-history-in-cosmetics-shop --unzip -p kaggle_data

   Or download the ZIP manually from:
   https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop
   and pass its path to --input.

3) Run at target scale (10 million events)
   python generate_clickstream_kaggle.py --input kaggle_data --output-dir generated_csvs

   You can also pass a downloaded ZIP:
   python generate_clickstream_kaggle.py --input ecommerce-events-history-in-cosmetics-shop.zip --output-dir generated_csvs

4) Test with a smaller dataset first
   python generate_clickstream_kaggle.py --input kaggle_data --output-dir test_csvs --events 10000 --sessions 1000 --devices 600 --customers 150 --cart-items 700 --checkouts 300 --payment-attempts 200 --orders 100 --products 100 --categories 10 --calendar-days 30

Default targets
- events: 10,000,000
- sessions: 1,000,000
- devices: 600,000
- customers: 150,000
- cart_items: 700,000
- checkouts: 300,000
- payment_attempts: 200,000
- orders: 100,000
- products: 10,000
- categories: 30
- event_types: 8
- calendar_days: 730

Important data notes
- The Kaggle source contains view, cart, remove_from_cart, and purchase events. It does not directly contain checkout-start or payment-attempt events, so those are simulated.
- Event type ID 2 is called cart_remove in this script to preserve the source event meaning. If your ERD requires coupon_applied instead, change EVENT_TYPES and decide how to represent remove_from_cart.
- Source event rows are sampled with replacement to reach the target scale; the resulting 10 million rows are not 10 million distinct observed source events.
- Product, customer, device, session, checkout, payment, and order records are generated or transformed for the target schema. Device types and marketing channels are synthetic.
- The generator writes event data in batches, but the final 10-million-row CSV requires substantial disk space and runtime.

5) Validate generated row counts and key relationships
   python validate_clickstream_csvs.py --input-dir generated_csvs

The validator checks table presence, row counts, and major foreign-key relationships.
