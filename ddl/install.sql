/*****************************
 *
 * User Defined Functions for vector
 *
 * Copyright DingQiang Liu(dingqiangliu@gmail.com), 2012 - 2024
 */

-- Step 1: Create LIBRARY 
\set libfile '\''`pwd`'/.libs/vector.so\'';
CREATE LIBRARY vector AS :libfile;

-- Step 2: Create cube/rollup Factory
\set tmpfile '/tmp/vectorinstall.sql'
\! cat /dev/null > :tmpfile

\t
\o :tmpfile

select 'CREATE FUNCTION '||lower(regexp_replace(replace(obj_name, 'Factory', ''), '([a-z])([A-Z])', '\1_\2')::varchar(255))
    ||' AS LANGUAGE ''C++'' NAME '''||obj_name||''' LIBRARY vector'
    ||' not fenced;'
    from user_library_manifest where lib_name='vector' and obj_type='Scalar Function';
select 'GRANT EXECUTE ON FUNCTION '||lower(regexp_replace(replace(obj_name, 'Factory', ''), '([a-z])([A-Z])', '\1_\2')::varchar(255))
    ||' ('||regexp_replace(arg_types, '\w+\[(\w+)\]', '\1[]')||')'
    ||' to PUBLIC;'
    from user_library_manifest where lib_name='vector' and obj_type='Scalar Function';


select 'CREATE AGGREGATE FUNCTION '||lower(regexp_replace(replace(obj_name, 'Factory', ''), '([a-z])([A-Z])', '\1_\2')::varchar(255))
    ||' AS LANGUAGE ''C++'' NAME '''||obj_name||''' LIBRARY vector;'
from user_library_manifest where lib_name='vector' and obj_type='Aggregate Function';
select 'GRANT EXECUTE ON AGGREGATE FUNCTION '||lower(regexp_replace(replace(obj_name, 'Factory', ''), '([a-z])([A-Z])', '\1_\2')::varchar(255))
    ||' ('||regexp_replace(arg_types, '\w+\[(\w+)\]', '\1[]')||')'
    ||' to PUBLIC;'
from user_library_manifest where lib_name='vector' and obj_type='Aggregate Function';


select 'CREATE ANALYTIC FUNCTION '||lower(regexp_replace(replace(obj_name, 'Factory', ''), '([a-z])([A-Z])', '\1_\2')::varchar(255))
    ||' AS LANGUAGE ''C++'' NAME '''||obj_name||''' LIBRARY vector'
    ||' not fenced;'
from user_library_manifest where lib_name='vector' and obj_type='Analytic Function';
select 'GRANT EXECUTE ON ANALYTIC FUNCTION '||lower(regexp_replace(replace(obj_name, 'Factory', ''), '([a-z])([A-Z])', '\1_\2')::varchar(255))
    ||' () to PUBLIC;'
from user_library_manifest where lib_name='vector' and obj_type='Analytic Function';


select 'CREATE TRANSFORM FUNCTION '||lower(regexp_replace(replace(obj_name, 'Factory', ''), '([a-z])([A-Z])', '\1_\2')::varchar(255))
    ||' AS LANGUAGE ''C++'' NAME '''||obj_name||''' LIBRARY vector'
    ||' not fenced;'
from user_library_manifest where lib_name='vector' and obj_type='Transform Function';
select 'GRANT EXECUTE ON TRANSFORM FUNCTION '||lower(regexp_replace(replace(obj_name, 'Factory', ''), '([a-z])([A-Z])', '\1_\2')::varchar(255))
    ||' () to PUBLIC;'
from user_library_manifest where lib_name='vector' and obj_type='Transform Function';

\o
\t

\i :tmpfile
\! rm -f :tmpfile
