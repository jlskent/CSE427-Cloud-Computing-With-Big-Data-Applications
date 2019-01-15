-- Load only the ad_data1 and ad_data2 directories

--load data from hdfs
data = LOAD '/dualcore/ad_data[1-2]/part*' USING PigStorage(',') AS (campaign_id:chararray,date:chararray, time:chararray,keyword:chararray, display_site:chararray,placement:chararray, was_clicked:int, cpc:int);

--load data from local
--data = LOAD '../test_ad_data.txt' USING PigStorage(',') AS (campaign_id:chararray,date:chararray, time:chararray,keyword:chararray, display_site:chararray,placement:chararray, was_clicked:int, cpc:int);



grouped = GROUP data BY display_site;

  	--DUMP grouped;

by_site = FOREACH grouped {
 	-- Include only records where the ad was clicked
	c = FILTER data BY was_clicked == 1;
  	-- count the number of records in this group
  	n = SUM(c.was_clicked);
	/* Calculate the click-through rate by dividing the 
	* clicked ads in this group by the total number of ads
	* in this group.
	*/
 	GENERATE group, (FLOAT)n/COUNT(data.was_clicked) AS ctr;

}

--DUMP by_site;

--Get rid of null records and
--sort the records in ascending order of clickthrough rate
nn = FILTER by_site BY ctr is not null;
sort = ORDER nn BY ctr ASC;
--set output number
r = LIMIT nn 4;
--DUMP r
DUMP sort;

-- show just the first three
--DUMP r
