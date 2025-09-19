-- Main OHLCV table
   CREATE TABLE ohlcv_stocks (
       datetime TIMESTAMP,
       symbol SYMBOL CAPACITY 512 CACHE,
       open DOUBLE,
       high DOUBLE,
       low DOUBLE,
       close DOUBLE,
       volume LONG,
       insert_count INT,
       first_inserted TIMESTAMP,
       last_updated TIMESTAMP,
       load_batch_id STRING,
       source_file STRING
   ) TIMESTAMP(datetime) PARTITION BY DAY WAL 
   DEDUP UPSERT KEYS(datetime, symbol);

   -- Load summary table
   CREATE TABLE load_summary (
       load_id STRING,
       symbol STRING,
       load_timestamp TIMESTAMP,
       records_processed INT,
       records_new INT,
       records_updated INT,
       source_file STRING,
       job_status STRING
   ) TIMESTAMP(load_timestamp) PARTITION BY DAY;