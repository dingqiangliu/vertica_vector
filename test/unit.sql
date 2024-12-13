/*****************************
 *
 * User Defined Functions for vector
 *
 * Copyright DingQiang Liu(dingqiangliu@gmail.com), 2012 - 2024
 */


-- unit test case of vector_init: create an empty vector
select error_on_check_false(
         vector_init(1, 0) = array[]
         , 'empty vector', 'case vector_init: create an empty vector', ''
      ) as "case vector_init: create an empty vector" ;

-- unit test case of vector_init: create an non-empty vector of nulls
select error_on_check_false(
         apply_count(value) = 0 and apply_count_elements(value) = 3
         , 'non-empty vector of nulls', 'case vector_init: create an non-empty vector of nulls', ''
      ) as "case vector_init: create an non-empty vector of nulls" 
from (
  select vector_init(null::int, 3) value
  ) t;

-- unit test case of vector_init: create a non-empty vector of integers
select error_on_check_false(
         vector_init(1, 3) = array[1, 1, 1]::array[int, 3]
         , 'non-empty vector of integers', 'case vector_init: create a non-empty vector of integers', ''
      ) as "case vector_init: create a non-empty vector of integers" ;

-- unit test case of vector_init: create a non-empty vector of floats
select error_on_check_false(
         vector_init(1.0::float, 3) = array[1, 1, 1]::array[float, 3]
         , 'non-empty vector of floats', 'case vector_init: create a non-empty vector of floats', ''
      ) as "case vector_init: create a non-empty vector of floats" ;

-- unit test case of vector_init: create a non-empty vector of numerics
select error_on_check_false(
         vector_init(1.234::numeric(17, 1), 3) = array[1.2, 1.2, 1.2]::array[numeric(17, 1), 3]
         , 'non-empty vector of numerics', 'case vector_init: create a non-empty vector of numerics', ''
      ) as "case vector_init: create a non-empty vector of numerics" ;


-- unit test case of vector_add: null input
select error_on_check_false(
         vector_add(null::array[float, 3], array[1.0, 2.0, 3.0]) is null
         and vector_add(array[1.0, 2.0, 3.0], null) is null
         , 'result of null input is null', 'unit test case of vector_add: null input', ''
      ) as "unit test case of vector_add: null input" ;

-- unit test case of vector_add: vector of floats
select error_on_check_false(
         vector_add(array[1.0, 2.0, 3.0], array[1.0, 2.0, 3.0]) = array[2, 4, 6]::array[float, 3]
         , 'add two vectors of floats', 'unit test case of vector_add: vector of floats', ''
      ) as "unit test case of vector_add: vector of floats" ;


-- unit test case of vector_mul: null input
select error_on_check_false(
         vector_mul(null::array[float, 3], 2) is null
         and vector_mul(array[1.0, 2.0, 3.0], null) is null
         , 'result of null input is null', 'unit test case of vector_mul: null input', ''
      ) as "unit test case of vector_mul: null input" ;

-- unit test case of vector_mul: vector of floats
select error_on_check_false(
         vector_mul(array[1.0, 2.0, 3.0], 2) = array[2, 4, 6]::array[float, 3]
         , 'a vector of floats multiplied by a float', 'unit test case of vector_mul: vector of floats', ''
      ) as "unit test case of vector_mul: vector of floats" ;


-- unit test case of vector_sum: null input
select error_on_check_false(
         value = array[2, 2, 3]::array[float, 3]
         , 'sum of vectors of floats', 'unit test case of vector_sum: vector of floats', ''
      ) as "unit test case of vector_sum: vector of floats"
from (
  select vector_sum(arry) value
  from (
  select array[1.0, null, 3.0] as arry
  union all
  select array[1.0, 2.0, null] as arry
  union all
  select null::array[float, 3] as arry
  ) a
) t;

-- unit test case of vector_sum: vector of floats
select error_on_check_false(
         value = array[2, 4, 6]::array[float, 3]
         , 'sum of vectors of floats', 'unit test case of vector_sum: vector of floats', ''
      ) as "unit test case of vector_sum: vector of floats"
from (
  select vector_sum(arry) value
  from (
  select array[1.0, 2.0, 3.0] as arry
  union all
  select array[1.0, 2.0, 3.0] as arry
  ) a
) t;

