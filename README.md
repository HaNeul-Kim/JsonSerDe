# JsonSerDe

## 사용법

먼저 json 파일을 hdfs 에 올리고 json 파일을 이용해서 hive table 을 생성한다

CREATE TABLE `serdetest`(
  `number` int,
  `text` string)
ROW FORMAT SERDE
  'JsonSerDe'
WITH SERDEPROPERTIES (
  'characterSet'='euc-kr')
LOCATION
  'hdfs://hskimsky:9000/user/hskimsky/json';

hive> CREATE TABLE `serdetest`(
    >   `number` int,
    >   `text` string)
    > ROW FORMAT SERDE
    >   'JsonSerDe'
    > WITH SERDEPROPERTIES (
    >   'characterSet'='euc-kr')
    > LOCATION
    >   'hdfs://hskimsky:9000/user/hskimsky/json';

hive> select * from serdetest;
OK
1	one
2	two
3	three
Time taken: 0.285 seconds, Fetched: 3 row(s)
hive>
