ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE s201719;

drop table if exists tmp;
drop table if exists Users_small;

create table Users_small as
select u.sex, u.ip, count(*)
		from Users u
		group by u.sex, u.ip;
		
create table tmp as
select u.sex, r.region, r.ip
		from (
			SELECT *
			FROM Users_small TABLESAMPLE(20 PERCENT)) as u JOIN (
			SELECT *
			FROM IPRegions TABLESAMPLE(20 PERCENT)) as r
		ON u.ip = r.ip;		


select t3.cnt, t3.region, t4.cnt
 FROM (
	select count(t1.ip) as cnt,  t1.region
		from tmp as t1
		where t1.sex = 'male'
		group by t1.region
	) as t3 JOIN  
	(
	select count(t1.ip) as cnt, t1.region
		from tmp as t1
		where t1.sex = 'female'
		group by t1.region
	) as t4
	ON t3.region = t4.region;


