#! /bin/bash

python3.11 -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt

python $(pwd)/src/datagenerator.py --seed 123 --num-events 10000 --base-path data/raw_events/ --write-data

spark-submit $(pwd)/src/eventloader.py --source-file data/raw_events/events.json --domain account --event-type account-open --date 2024-10-10 --target-path data/target/ --backfill
spark-submit $(pwd)/src/eventloader.py --source-file data/raw_events/events.json --domain account --event-type account-close --date 2024-10-10 --target-path data/target/ --backfill
spark-submit $(pwd)/src/eventloader.py --source-file data/raw_events/events.json --domain account --event-type status-change --date 2024-10-10 --target-path data/target/ --backfill

spark-submit $(pwd)/src/eventloader.py --source-file data/raw_events/events.json --domain transaction --event-type payment-to --date 2024-10-10 --target-path data/target/ --backfill
spark-submit $(pwd)/src/eventloader.py --source-file data/raw_events/events.json --domain transaction --event-type payment-from --date 2024-10-10 --target-path data/target/ --backfill
