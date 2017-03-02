--optimizations(reference: https://hortonworks.com/blog/5-ways-make-hive-queries-run-faster/ )
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.execution.engine=tez;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;


-- cost-based optimization start
set hive.cbo.enable=true; 
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;

analyze table TweetsAlpha compute statistics for columns id, created_at;
analyze table tweets_sentiment compute statistics for columns id, sentiment;
-- cost-based optimization end. 



--inspired by Microsoft(https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-analyze-twitter-data-linux)
DROP TABLE IF EXISTS TweetsRaw;

--create the raw Tweets table on json formatted twitter data
CREATE EXTERNAL TABLE TweetsRaw(json_response STRING)
STORED AS TEXTFILE LOCATION 'wasb://finallyhello@projectksustorage.blob.core.windows.net/HdiSamples/TwitterProject/Data';

DROP TABLE IF EXISTS TweetsAlpha;
--creates the table to store tweetts
CREATE TABLE TweetsAlpha(
    id BIGINT,
    created_at STRING,
    created_at_date STRING,
    created_at_year STRING,
    created_at_month STRING,
    created_at_day STRING,
    created_at_time STRING,
    in_reply_to_user_id_str STRING,
    text STRING) STORED AS ORC;
	
	
--inserts the data with parsing
FROM TweetsRaw
INSERT OVERWRITE TABLE TweetsAlpha
SELECT
    CAST(get_json_object(json_response, '$.id_str') as BIGINT),
    get_json_object(json_response, '$.created_at'),
    CONCAT(SUBSTR (get_json_object(json_response, '$.created_at'),1,10),' ',
    SUBSTR (get_json_object(json_response, '$.created_at'),27,4)),
    SUBSTR (get_json_object(json_response, '$.created_at'),27,4),
    CASE SUBSTR (get_json_object(json_response, '$.created_at'),5,3)
        WHEN 'Jan' then '01'
        WHEN 'Feb' then '02'
        WHEN 'Mar' then '03'
        WHEN 'Apr' then '04'
        WHEN 'May' then '05'
        WHEN 'Jun' then '06'
        WHEN 'Jul' then '07'
        WHEN 'Aug' then '08'
        WHEN 'Sep' then '09'
        WHEN 'Oct' then '10'
        WHEN 'Nov' then '11'
        WHEN 'Dec' then '12' end,
    SUBSTR (get_json_object(json_response, '$.created_at'),9,2),
    SUBSTR (get_json_object(json_response, '$.created_at'),12,8),
    get_json_object(json_response, '$.in_reply_to_user_id_str'),
    get_json_object(json_response, '$.text')
WHERE (LENGTH(json_response) > 500 AND (json_response like '%Obama%' or json_response like '%Romney%'));
analyze table TweetsAlpha compute statistics for columns id;
SELECT count(id) as cnt FROM TweetsAlpha;


--dictionary creation
drop table IF EXISTS dictionary;
create table dictionary(word string,rating int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
LOAD DATA INPATH 'wasb://finallyhello@projectksustorage.blob.core.windows.net/HdiSamples/TwitterProject/Dictionary' into TABLE dictionary;

--inspired by http://hortonworks.com/hadoop-tutorial/how-to-refine-and-visualize-sentiment-data/
--sentiment analysis using affin dictionary
analyze table TweetsAlpha compute statistics for columns id, text, created_at;
analyze table dictionary compute statistics for columns word, rating;
drop view l1;
create view IF NOT EXISTS l1 as select id, words from TweetsAlpha lateral view explode(sentences(lower(text))) dummy as words;
drop view l2;
create view IF NOT EXISTS l2 as select id, word from l1 lateral view explode( words ) dummy as word;
drop view l3;
create view IF NOT EXISTS l3 as select
    id,
    l2.word,
    d.rating as polarity
 from l2 left outer join dictionary d on l2.word = d.word;
create table IF NOT EXISTS tweets_sentiment stored as orc as select
  id,
  case
    when avg( polarity ) > 0 then 'positive'
    when avg( polarity ) < 0 then 'negative'  
    else 'neutral' end as sentiment
from l3 group by id;

analyze table tweets_sentiment compute statistics for columns id, sentiment;
Drop table tweetsbi;
CREATE TABLE IF NOT EXISTS tweetsbi
STORED AS ORC
AS SELECT
  SUBSTR(t.created_at,4, 8) as datetime1, --Mon Mar 30 18:56:55 +0000 2009
  case s.sentiment
    when 'positive' then 1
    when 'neutral' then 0
    when 'negative' then -1
  end as sentiment  
FROM TweetsAlpha t LEFT OUTER JOIN tweets_sentiment s on t.id = s.id and t.created_at is not null;

analyze table TweetsAlpha compute statistics for columns id, created_at, created_at_month, text;
analyze table tweets_sentiment compute statistics for columns id, sentiment;
drop table tweets;
CREATE TABLE IF NOT EXISTS tweets
STORED AS ORC
AS SELECT
  t.id,
  created_at,
  t.created_at_month,
  SUBSTR(t.created_at,4, 8) as tweetdate, --Mon Mar 30 18:56:55 +0000 2009
  case 
    when t.text like '%Obama%' AND t.text not like '%Romney%' then 1
    when t.text like '%Romney%' AND t.text not like '%Obama%' then 2
  end as candidate  
  ,
  case s.sentiment
    when 'positive' then 1
    when 'neutral' then 0
    when 'negative' then -1
  end as sentiment  
FROM TweetsAlpha t LEFT OUTER JOIN tweets_sentiment s on t.id = s.id
and t.created_at is not null and s.sentiment is not null;


analyze table tweets compute statistics for columns id, sentiment, tweetdate, candidate,created_at, created_at_month, coordinates;
select count(id) from tweets where candidate is not null;

