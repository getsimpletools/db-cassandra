### 0.2.15 (2019-06-23)
1. **Simpletools\Db\Cassandra\Query**
    1. Fixed getQuery() auto casting
    2. Added type auto casting support on any where methods (->where(), ->also(), alternatively()) etc.
    3. Fixed NULL auto casting
1. **Simpletools\Db\Cassandra\Result**
    1. Fixed NULL auto casting
2. **Simpletools\Db\Cassandra\Type\Data**
    1. Added ->toDateTime()
    2. Changed default from 0 to now
3. **Simpletools\Db\Cassandra\Type\Timestamp**
    1. Changed default from 0 to now
4. **Simpletools\Db\Cassandra\Type\Time**
    1. Introduced new type


### 0.2.14 (2019-06-22)
1. **Simpletools\Db\Cassandra\Batch**
    1. Replaced [] with array() for older PHP versions support

### 0.2.13 (2019-06-22)
1. **Simpletools\Db\Cassandra\Batch**
    1. Improved batch auto runs
    
### 0.2.12 (2019-06-22)
1. **Simpletools\Db\Cassandra\Batch**
    1. Added ->runEvery() for batch auto runs
    2. Added ->runIfNotEmpty()
    3. Added ->size() to see current size of the batch
    4. Fixed ->reset()
    5. Removed ->rewind()
    6. Added ->reset() after ->run()
2. **Simpletools\Db\Cassandra\Query**
    1. Added Exception for ->getSchema() if non existence namespace has been provided 

### 0.2.1 (2019-06-07)
1. **Simpletools\Db\Cassandra\Doc**
2. **Simpletools\Db\Cassandra\Type\Blob**
3. **Simpletools\Db\Cassandra\Type\Decimal**
4. **Simpletools\Db\Cassandra\Type\SimpleFloat**
5. **Simpletools\Db\Cassandra\Type\Tunyint**

### 0.1.3 (2019-06-03)
1. **Simpletools\Db\Cassandra\Doc**
2. **Simpletools\Db\Cassandra\Doc\Body**
3. **Simpletools\Db\Cassandra\Type\Map**

### 0.0.5 (2019-05-23)
1. **Simpletools\Db\Cassandra\{all}**

### 0.0.4 (2019-05-12)
1. **Simpletools\Db\Cassandra\Client**
    1. Cleaning duplicated licence entry
2. **Simpletools\Db\Cassandra\Connection**
    1. Cleaning duplicated licence entry

### 0.0.3 (2019-05-12)
1. **Simpletools\Db\Cassandra\Client**
     1. Improved connect code seq
     
### 0.0.2 (2019-05-12)
1. **Simpletools\Db\Cassandra\Client**
     1. Added ::settings()
     2. Added ->connect()

### 0.0.1 (2019-05-12)
1. **Simpletools\Db\Cassandra**
     1. Structure setup
