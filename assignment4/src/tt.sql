ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE s201760;

drop table if exists tmp_task;

create table tmp_task as
select count(f.request) as cnt, f.client,u.gender
from Logs as f
inner join Users as u
on f.ip = u.ip
group by f.client,u.gender;

drop table if exists tmp_task_20;

create table tmp_task_20 as
select ((a.cnt -s.cnt)*(a.cnt-s.cnt)) as err,a.client,a.gender as error
from tmp_task as a
inner join tmp_task TABLESAMPLE(20 PERCENT) s
on  a.client = s.client and a.gender = s.gender;



drop table if exists tmp_task_40;

create table tmp_task_40 as
select ((a.cnt -s.cnt)*(a.cnt-s.cnt)) as err,a.client,a.gender as error
from tmp_task as a
inner join tmp_task TABLESAMPLE(40 PERCENT) s
on  a.client = s.client and a.gender = s.gender;


drop table if exists tmp_task_60;

create table tmp_task_60 as
select ((a.cnt -s.cnt)*(a.cnt-s.cnt)) as err,a.client,a.gender as error
from tmp_task as a
inner join tmp_task TABLESAMPLE(60 PERCENT) s
on  a.client = s.client and a.gender = s.gender;




drop table if exists tmp_task_80;

create table tmp_task_80 as
select ((a.cnt -s.cnt)*(a.cnt-s.cnt)) as err,a.client,a.gender as error
from tmp_task as a
inner join tmp_task TABLESAMPLE(80 PERCENT) s
on  a.client = s.client and a.gender = s.gender;

drop table if exists tmpAll;

create table tmpAll as
select avg(err) as mse from tmp_task_20
union all
select avg(err) as mse from tmp_task_40
union all
select avg(err) as mse from tmp_task_60
union all
select avg(err) as mse from tmp_task_80;

select * from tmpAll;