
--load hdfs
data = LOAD '/dualcore/ad_data[1-2]/part*' USING PigStorage(',') AS (campaign_id:chararray,date:chararray, time:chararray,keyword:chararray, display_site:chararray,placement:chararray, was_clicked:int, cpc:int);

--load local
--data = LOAD 'test_ad_data.txt' USING PigStorage(',') AS (campaign_id:chararray,date:chararray, time:chararray,keyword:chararray, display_site:chararray,placement:chararray, was_clicked:int, cpc:int);


--f = LIMIT data 10;
--DUMP f;

--Include only records where was_clicked has a value of 1
c = FILTER data BY was_clicked == 1;

--Group the data by the appropriate field
g = GROUP c BY display_site;

--DESCRIBE g;

/*  Create a new relation which includes only the 
 *  display site and the total cost of all clicks 
 *  on that site
 */
n = FOREACH g GENERATE group, SUM(c.cpc) AS sum;

--DUMP n;

--Sort that new relation by cost (ascending)
s = ORDER n BY sum;

--Display just the first three records to the screen
l = LIMIT s 4;
DUMP l;
