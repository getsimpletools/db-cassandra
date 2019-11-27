### 0.3.18 (2019-11-27)
1. **Simpletools\Db\Cassandra\SessionHandler**
    1. Clear cookies on ->destroy()

### 0.3.14 (2019-10-14)
1. **Simpletools\Db\Cassandra\Client**
    1. Added `ioThreads` settings

### 0.3.13 (2019-11-14)
4. **Simpletools\Db\Cassandra\Async**
    1. Added `Async` for the bulk write operations

### 0.3.12 (2019-11-06)
4. **Simpletools\Db\Cassandra\Type\Set**
    1. Introduced new type

### 0.3.10 (2019-10-11)
1. **Simpletools\Db\Cassandra\Client**
    1. Added `connectTimeout` and `requestTimeout` settings

### 0.3.8 (2019-10-07)
1. **Simpletools\Db\Cassandra\Query**
    1. Added ->ifNotExists() and ->ifExists()
    2. Added partial Map update.
1. **Simpletools\Db\Cassandra\Doc**
    1. Added partial Map update.

### 0.3.7 (2019-09-13)
1. **Simpletools\Db\Cassandra\Query**
    1. ->keyspace() - made this function private

### 0.3.6 (2019-08-06)
1. **Simpletools\Db\Cassandra\Schema**
    1. Added ->getPartitionKey() and ->getClusteringKey() 
2. **Simpletools\Db\Cassandra\Query**
    1. Added ->getPartitionKey() and ->getClusteringKey() 

### 0.3.5 (2019-09-06)
1. **Simpletools\Db\Cassandra\Query**
    1. Added ->getSchema()

### 0.3.4 (2019-08-20)
1. **Simpletools\Db\Cassandra\Result**
    1. Added automatic pagination on ->fetch()

### 0.3.3 (2019-08-08)
1. **Simpletools\Db\Cassandra\Lucene**
    1. Added `Lucene` class

### 0.3.1 (2019-08-06)
1. **Simpletools\Db\Cassandra\Schema**
    1. Added `Schema` class
2. **Simpletools\Db\Cassandra\Query**
    1. Moved schema to `Schema` class and cached per connection
3. **Simpletools\Db\Cassandra\Docs**
    1. Added `Docs` class

### 0.3.0 (2019-07-27)
1. **Simpletools\Db\Cassandra\SessionHandler**
    1. Added `SessionHandler` class implementing PHP `SessionHandlerInterface` and `SessionIdInterface`
    
### 0.2.22 (2019-07-11)
1. **Simpletools\Db\Cassandra\Query**
    1. Added double quotes to all column names

### 0.2.20 (2019-07-10)
1. **Simpletools\Db\Cassandra\Doc**
    1. Added Exception on not existing documents and when try get more then one

### 0.2.19 (2019-07-10)
1. **Simpletools\Db\Cassandra\Query**
    1. Added the schema of the materialized view

### 0.2.18 (2019-07-09)
1. **Simpletools\Db\Cassandra\Client**
    1. Added `pluginSettings` defines global plugin settings
    2. Added `convertMapToJson` a plugin setting which defines whether all map columns should be always converted to JSON in the database
2. **Simpletools\Db\Cassandra\Query**
    1. Added `convertMapToJson`
    2. ->expires($time) - $time can be seconds or timestamp
3. **Simpletools\Db\Cassandra\Doc**
    1. Added `convertMapToJson`
    2. ->expires($time) - $time can be seconds or timestamp
    3. unset($doc->body->property) - sets null in the database
4. **Simpletools\Db\Cassandra\Result**
    1. Added `convertMapToJson`

### 0.2.17 (2019-07-03)
1. **Simpletools\Db\Cassandra\Connection**
    1. Added routing and consistency constants (`CONSISTENCY_ANY`, `CONSISTENCY_ONE`, `CONSISTENCY_TWO`, `CONSISTENCY_THREE`, `CONSISTENCY_QUORUM`, `CONSISTENCY_ALL`, `CONSISTENCY_LOCAL_QUORUM`, `CONSISTENCY_EACH_QUORUM`, `CONSISTENCY_SERIAL`, `CONSISTENCY_LOCAL_SERIAL`, `CONSISTENCY_LOCAL_ONE`, `ROUTING_TOKEN_AWARE`, `ROUTING_LATENCY_AWARE`)
2. **Simpletools\Db\Cassandra\Client**
    1. Added `consistency` cluster settings option (`Connection::CONSISTENCY_*`)
    2. Added `pageSize` cluster settings option
    3. Added `timeout` cluster settings option
    4. Added `routing` cluster settings option (`Connection::ROUTING_TOKEN_AWARE`, `Connection::ROUTING_LATENCY_AWARE`), passed as string or an array of options
    5. Added `persistentSessions` cluster settings option (`true` or `false`)

    
### 0.2.16 (2019-06-23)
1. **Simpletools\Db\Cassandra\Query**
    1. Added more readable exception on non existing table
2. **Simpletools\Db\Cassandra\Type\AutoIncrement**
    1. Fixed type auto casting
    
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
