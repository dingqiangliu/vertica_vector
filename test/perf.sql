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
            select num, vector_init((num%100)::float, 5000)
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

select id, vector_add(value, value) as value
from test_vector
where id <= 3
order by id
;


select max(id), max(array_count(value))
from (
  select id, vector_add(value, value) as value
  from test_vector
) a
;


/*
 * vector_mul(vec, scalar) : element multiplied by a scalar
*/

select id, vector_mul(value, 3)
from test_vector
where id <= 3
order by id
;

select count(distinct id), max(array_count(value))
from (
  select id, vector_mul(value, 3) as value
  from test_vector
) a
;



/*
 * vector_sum(vec) : sum by element
*/

select id, vector_sum(value) as value
from test_vector
where id <= 3
group by id
order by id
;

select count(distinct id), max(array_count(value))
from (
  select id, vector_sum(value) as value
  from test_vector
  group by id
) a
;


select vector_sum(value) as value
from test_vector
;

