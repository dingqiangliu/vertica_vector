create table if not exists test_vector(
  id int
  , value array[float, 5000]
  )
order by id
segmented by hash(id) all nodes
;


/*
 * vector_init(val, size) : init elements with val
*/

DO $$
DECLARE
  is_empty boolean;
BEGIN
  FOR is_empty IN QUERY select count(*)=0 from test_vector minus select false LOOP
    perform insert into test_vector
            select num, string_to_array((num%100)::varchar||repeat(','||(num%100), 5000-1))::array[float, 5000]
            from (
              select extract(epoch from ts)::int as num from (
                select '1970-01-01 00:00:01 +0'::timestamp as tm
                  union
                select '1970-01-03 00:00:00 +0'::timestamp as tm 
               ) t0 
               timeseries ts as '1 second' over (order by tm)
            ) t1
    ;
  END LOOP;
END;
$$;


select id, array_count(value), value[0:3] val_3cols_begin, value[4997:5000] val_3cols_end
from test_vector
order by id
limit 3;

select id, count(distinct position), min(position), max(position), min(value), max(value)
from (
    select id, explode(value) over(partition by id)
    from test_vector
  ) a
where id <= 3
group by id
order by id
;



/*
 * vector_add(vec1, vec2) : add by element
*/

select id, implode(value) within group(order by position) 
from (
  select a1.id, a1.position, a1.value + a2.value as value 
  from (select id, explode(value) over(partition by id) from test_vector order by id, position) a1
  join (select id, explode(value) over(partition by id) from test_vector order by id, position) a2 on a1.id = a2.id and a1.position = a2.position
) a 
where id <= 3
group by id
order by id
;


select max(id), max(array_count(value))
from (
  select id, implode(value) within group(order by position) as value 
  from (
    select a1.id, a1.position, a1.value + a2.value as value 
    from (select id, explode(value) over(partition by id) from test_vector order by id, position) a1
    join (select id, explode(value) over(partition by id) from test_vector order by id, position) a2 on a1.id = a2.id and a1.position = a2.position
  ) a 
  group by id
) a
;


/*
 * vector_mul(vec, scalar) : element multiplied by a scalar
*/

select id, implode(value) within group(order by position) 
from (
  select a1.id, a1.position, a1.value * 3 as value 
  from (select id, explode(value) over(partition by id) from test_vector) a1
) a 
where id <= 3
group by id
order by id
;

select count(distinct id), max(array_count(value))
from (
  select id, implode(value) within group(order by position) as value
  from (
    select a1.id, a1.position, a1.value * 3 as value 
    from (select id, explode(value) over(partition by id) from test_vector) a1
  ) a 
  group by id
) a
;



/*
 * vector_sum(vec) : sum by element
*/

select id, implode(value) within group(order by position) 
from (
  select a1.id, a1.position, sum(a1.value) as value 
  from (select id, explode(value) over(partition by id) from test_vector order by id, position) a1
  group by a1.id, a1.position
) a 
where id <= 3
group by id
order by id
;

select count(distinct id), max(array_count(value))
from (
  select id, implode(value) within group(order by position) as value
  from (
    select a1.id, a1.position, sum(a1.value) as value 
    from (select id, explode(value) over(partition by id) from test_vector order by id, position) a1
    group by a1.id, a1.position
  ) a 
  group by id
) a
;


select implode(value) within group(order by position) 
from (
  select a1.position, sum(a1.value) as value 
  from (select id, explode(value) over(partition by id) from test_vector order by position) a1
  group by a1.position
) a 
;

