# Vector algebra for Vertica

This is a set of Vertica User Defined Functions (UDF) for basic vector algebra, but much faster than pure SQL solution with the built-in explode and implode functions.

## Syntax

1. **vector_init** (elem, size)
   Create an array with given size and element value.

   ***Parameters:***

   * elem: value of element, int/float/double/numeric.
   * size: size of the array, non-negative int
   * (return): an array initialized.

2. **vector_mul** (arry, lamda)
   each element of a array multiplied by a scalar.

   ***Parameters:***

   * arry: a array of float
   * lamda: float
   * (return): the result array.

3. **vector_add** (arry1, arry2)
   add arrays by element

   ***Parameters:***

   * arry1: a array of float
   * arry2: a array of float
   * (return): the result array.

4. **vector_sum** (arry)
   aggregate arrays by element

   ***Parameters:***

   * arry: a array of float
   * (return): the result array.


## Examples

```SQL
select vector_init(1, 3) as value;
 value
--------
 [1,1,1]
(1 row)

select vector_init(1.0, 3) as value;
 value
--------
 [1.0,1.0,1.0]
(1 row)

select vector_add(array[1.0, 2.0, 3.0], array[1.0, 2.0, 3.0]) as value;
     value
---------------
 [2.0,4.0,6.0]

select vector_mul(array[1.0, 2.0, 3.0], 2) as value;
     value
---------------
 [2.0,4.0,6.0]

select vector_sum(arry) as value
from (
  select array[1.0, 2.0, 3.0] as arry
  union all
  select array[1.0, 2.0, 3.0] as arry
  ) t;
     value
---------------
 [2.0,4.0,6.0]
```

## Install, test and uninstall

Before build and install, g++ should be available(**yum -y groupinstall "Development tools" && yum -y groupinstall "Additional Development"** or **apt install -y build-essential** can help on this).

 * Build: 

   ```bash
   autoreconf -f -i && ./configure && make
   ```

 * Install: 

   ```bash
   make install
   ```

 * Tests: 

   ```bash
   # unit tests
   make test

   # perf tests
   make perf

   # perf tests with pure sql implement as a baseline
   make perf_sql
   ```

 * Uninstall: 

   ```bash
   make uninstall
   ```

 * Clean: 

   ```bash
   make distclean
   ```

 * Clean all: 

   ```bash
   git clean -i .
   ```
