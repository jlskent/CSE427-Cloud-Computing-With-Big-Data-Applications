data = LOAD '/dualcore/ad_data[1-2]/part*' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray, 
             placement:chararray, was_clicked:int, cpc:int);

data_1 = GROUP data BY keyword;

data_2 = FOREACH data_1 GENERATE group, SUM(data.cpc) AS cost;

data_3 = ORDER data_2 BY cost DESC;

data_4 = LIMIT data_3 3;

dump data_4;
