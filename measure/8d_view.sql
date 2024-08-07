drop view if exists subquery1;
drop view if exists subquery2;
drop view if exists subquery3;
drop view if exists subquery4;
drop view if exists subquery5;

create view subquery1 as
  select mc.movie_id
  from movie_companies AS mc,
       company_name AS cn
  where cn.country_code ='[us]'
    and mc.company_id = cn.id;

create view subquery2 as
  select t.id,
         t.title,
	 subquery1.movie_id
  from title AS t,
       subquery1
  where t.id = subquery1.movie_id;

create view subquery3 as
  select ci.role_id,
         ci.person_id,
	 subquery2.title
  from subquery2,
       cast_info AS ci
  where ci.movie_id = subquery2.movie_id
    and ci.movie_id = subquery2.id;

create view subquery4 as
  select subquery3.title,
         subquery3.person_id
  from subquery3,
       role_type AS rt
  where rt.role ='costume designer'
    and subquery3.role_id = rt.id;

create view subquery5 as
  select subquery4.person_id as ci_person_id,
         an1.person_id as an1_person_id,
	 an1.name,
	 subquery4.title
  from subquery4,
       aka_name AS an1
  where an1.person_id = subquery4.person_id;

explain analyze select MIN(subquery5.name) AS costume_designer_pseudo,
       min(subquery5.title) as movie_with_costumes
from subquery5,
     name AS n1
where subquery5.an1_person_id = n1.id
  and n1.id = subquery5.ci_person_id;

drop view subquery1;
drop view subquery2;
drop view subquery3;
drop view subquery4;
drop view subquery5;
