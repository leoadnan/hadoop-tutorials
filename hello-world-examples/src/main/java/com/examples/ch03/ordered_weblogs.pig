nobots = LOAD '/user/aahmed/apache_nobots_tsv.txt' AS (ip: chararray, timestamp:long, page:chararray, http_status:int,payload_size:int, useragent:chararray);

ordered_weblogs = ORDER nobots BY timestamp;

STORE ordered_weblogs INTO '/user/aahmed/ordered_weblogs';