### 1.0.8 (2024-06-19)
1. **Simpletools\Db\Cassandra\Client**
   1. Added `datacenter` setting to support `->withDatacenterAwareRoundRobinLoadBalancingPolicy()`
   
### 1.0.4 (2023-02-08)
1. **Simpletools\Db\Cassandra\Async**
   1. Added `->get()` and `->collect()` for async reading
2. **Simpletools\Db\Cassandra\Query**
   1. Added `getResultFromRawResponse()` to get formatted Result object

### 1.0.3 (2023-01-19)
1. **Simpletools\Db\Cassandra\Type\BigInt**
  1. jsonSerialize() return value set to `mixed` to clear final deprication message.
2. **Simpletools\Db\Cassandra\Type\Blob**
  1. jsonSerialize() return value set to `mixed` to clear final deprication message.
3. **Simpletools\Db\Cassandra\Type\Date**
  1. jsonSerialize() return value set to `mixed` to clear final deprication message.
4. **Simpletools\Db\Cassandra\Type\Decimal**
  1. jsonSerialize() return value set to `mixed` to clear final deprication message.
5. **Simpletools\Db\Cassandra\Type\Inet**
  1. jsonSerialize() return value set to `mixed` to clear final deprication message.
8. **Simpletools\Db\Cassandra\Type\Set**
  1. jsonSerialize() return value set to `mixed` to clear final deprication message.
9. **Simpletools\Db\Cassandra\Type\SimpleFloat**
  1. jsonSerialize() return value set to `mixed` to clear final deprication message.
10. **Simpletools\Db\Cassandra\Type\Time**
  1. jsonSerialize() return value set to `mixed` to clear final deprication message.
11. **Simpletools\Db\Cassandra\Type\Timestamp**
  1. jsonSerialize() return value set to `mixed` to clear final deprication message.
12. **Simpletools\Db\Cassandra\Type\Timeuuid**
  1. jsonSerialize() return value set to `mixed` to clear final deprication message.
13. **Simpletools\Db\Cassandra\Type\Tinyint**
  1. jsonSerialize() return value set to `mixed` to clear final deprication message.
14. **Simpletools\Db\Cassandra\Type\Uuid**
  1. jsonSerialize() return value set to `mixed` to clear final deprication message.
15: **Simpletools\Db\Cassandra\Result**
  1. Updated Iterator functions with return types
    - current() : mixed
    - next() : void
    - key() : mixed
    - rewind() : void
    - valid() : bool

### 1.0.2 (2023-01-06)
1. **Simpletools\Db\Cassandra\Type\Map**
    1. `Map::jsonSerialize()` return value set to `mixed` to clear final deprication message.

### 1.0.1 (2023-01-06)
1. **composer.json**
   1. Composer Invalid fix

2. **Simpletools\Db\Cassandra\Query**
    1. next returning value when set to void

### 1.0.0 (2023-01-06)
1. **Simpletools\Db\Cassandra\Doc\Body**
   1. Added `mixed` return type to `jsonSerialize()`
2. **Simpletools\Db\Cassandra\Query**
   1. Added return types to iterator functions:
    * rewind() : void
    * current() : mixed
    * key() : mixed
    * next() : void
    * valid() : bool

### 0.8.8 (2022-10-13)
1. **Simpletools\Db\Cassandra\Batch**
   1. Fixed replicator bug on bubble

### 0.8.7 (2021-08-17)
1. **Simpletools\Db\Cassandra\Query**
    1. Added `->if()` for a lightweight transaction

### 0.8.6 (2021-07-20)
1. **Simpletools\Db\Cassandra\Query**
    1. Fixed map property object bug

### 0.8.5 (2021-07-20)
1. **Simpletools\Db\Cassandra\Query**
    1. Fixed map property object bug

### 0.8.4 (2021-05-10)
1. **Simpletools\Db\Cassandra\Async**
    1. Fixed reconnect issue

### 0.8.3 (2021-04-26)
1. **Simpletools\Db\Cassandra\Client**
    1. Added `->nextPageWithReconnect(result)` for pagination reconnect
2. **Simpletools\Db\Cassandra\Result**
    1. Added Client to construct for pagination reconnect

### 0.8.2 (2021-04-20)
1. **Simpletools\Db\Cassandra\Client**
    1. Added more error codes for auto-reconnect
2. **Simpletools\Db\Cassandra\Async**
    1. Added more error codes for auto-reconnect

### 0.8.0 (2021-02-21)
1. **Simpletools\Db\Cassandra\Client**
    1. Added `->executeWithReconnect()` to auto-reconnect all timeout requests
2. **Simpletools\Db\Cassandra\Async**
    1. Added `->setRetryPolicy(reconnect|fallthrough|silence)` by default auto-reconnect all timeout requests

### 0.7.20 (2021-02-16)
1. **Simpletools\Db\Cassandra\Type\Timestamp**
   1. Added support for DateTime value
   
### 0.7.19 (2021-02-15)
1. **Simpletools\Db\Cassandra\Client**
    1. Added `retryPolicy` and `retryPolicyLogging` settings

### 0.7.18 (2020-12-21)
1. **Simpletools\Db\Cassandra\SessionHandler**
    1. Updated all error loggers to catch `Throwable` instead of just `Exception`
    
### 0.7.17 (2020-12-21)
1. **Simpletools\Db\Cassandra\SessionHandler**
    1. Updated `::logFile()` to be less strict with path
    
### 0.7.16 (2020-12-21)
1. **Simpletools\Db\Cassandra\SessionHandler**
    1. Added `::logFile(false)` so log file can be switched off
    
### 0.7.15 (2020-12-21)
1. **Simpletools\Db\Cassandra\SessionHandler**
    1. Added `::logFile($path)` allowing to set logFile path on runtime
    
### 0.7.14 (2020-12-20)
1. **Simpletools\Db\Cassandra\SessionHandler**
    1. Added `::onConnectException($e)` allowing to handle connect exceptions
    
### 0.7.13 (2020-12-20)
1. **Simpletools\Db\Cassandra\SessionHandler**
    1. Added `::onWriteException($e)` allowing to handle write exceptions
    
### 0.7.12 (2020-11-19)
1. **Simpletools\Db\Cassandra\Query**
    1. Fixed `->bubble()` exception on `Doc`
    
### 0.7.11 (2020-11-19)
1. **Simpletools\Db\Cassandra\Query**
    1. Added `->bubble()` for single replication within Batch
2. **Simpletools\Db\Cassandra\Batch**
    1. Added `->bubble()` for bulk replication for all Queries inside Batch
4. **Simpletools\Db\Cassandra\Doc**
    1. Added `->bubble()` for single replication within Batch

### 0.7.9 (2020-09-11)
1. **Simpletools\Db\Cassandra\Query**
    1. Added `Client` to `__construct`

### 0.7.7 (2020-07-24)
1. **Simpletools\Db\Cassandra\Query**
    1. Fixed the bug with incorrect map<int, text> update.
2. **Simpletools\Db\Cassandra\Type\Map**
    1. Added `->getKeyType()` and `->getValueType()` methods.

### 0.7.6 (2020-07-08)
1. **Simpletools\Db\Cassandra\Query**
    1. Fixed the bug with incorrect schema when changing client.

### 0.7.5 (2020-07-06)
1. **Simpletools\Db\Cassandra\Client**
    1. Added `->getClusterSettings()` method.
1. **Simpletools\Db\Cassandra\Schema**
    1. Added `->getTables()` method.

### 0.7.4 (2020-06-23)
1. **Simpletools\Db\Cassandra\Doc**
    1. Added `->ifExists()` and `->ifNotExists()` methods.

### 0.7.3 (2020-06-16)
1. **Simpletools\Db\Cassandra\Query**
    1. Added `->cql(string $rawQuery, array $params=[])` for raw CQL query.

### 0.7.2 (2020-06-14)
1. **Simpletools\Db\Cassandra\SessionHandler**
    1. Updated `->destroy()` to remove set cookie reset
    
### 0.7.1 (2020-06-14)
1. **Simpletools\Db\Cassandra\SessionHandler**
    1. Added `::regenerateSessionId()` allowing to regenerate sessionId - used natively by `Simpletools\Store\Session`  
2. **Simpletools\Db\Cassandra\Doc**
    1. Added `->client($client)` allowing to set custom client

### 0.7.0 (2020-05-25)
1. **Simpletools\Db\Cassandra\Async**
    1. Added `->consistency($level=null)` allowing to specify per query consistency
2. **Simpletools\Db\Cassandra\Batch**
    1. Added `->consistency($level=null)` allowing to specify per query consistency
3. **Simpletools\Db\Cassandra\Client**
    1. Added query specific options `->execute($input=null,$thisQueryOptions=null)`
4. **Simpletools\Db\Cassandra\Doc**
    1. Added `->consistency($level=null)` allowing to specify per query consistency
5. **Simpletools\Db\Cassandra\Query**
    1. Added `->consistency($level=null)` allowing to specify per query consistency
    2. Fixed ability to specify `foo as bar` and get bar on return
    3. Fixed ability to add columns return mappers `->columns(['column'=>function($column){return $column;}])`
6. **Simpletools\Db\Cassandra\SessionHandler**
    1. Added `readConsistency` configuration setting
    2. Added `writeConsistency` configuration setting
    3. Changed `::setup($compactionWindowSize=12,$compactionWindowUnit="HOURS")` to set up compaction for session table as `TimeWindowCompactionStrategy` as well as set up `default_time_to_live` to the `$compactionWindowSize` and `$compactionWindowUnit`
    4. Forced `maxLifeTime` not to be smaller than `1 sec` to avoid never removed SSTables caused by `TimeWindowCompactionStrategy`
    5. Removed `date_modified` and `date_expires` since they are not needed

### 0.6.3 (2020-04-02)
1. **Simpletools\Db\Cassandra\Async**
    1. Fixed the bug with empty Batch

### 0.6.2 (2020-03-24)
1. **Simpletools\Db\Cassandra\Schema**
    1. Check schema consistency after create a TempTable

### 0.6.1 (2020-03-22)
1. **Simpletools\Db\Cassandra\TempTable**
    1. Fixed unnecessary case sensitivity for argument of `::expires($intervalIso8601)`, `->expire($intervalIso8601)` methods, normalised to uppercase
   
### 0.6.0 (2020-03-22)
1. **Simpletools\Db\Cassandra\TempTable**
    1. Added `::expires($intervalIso8601)` to setup global expiration ISO 8601 interval utilised by `::listActiveTempTables($keyspace)` method helping to remove leaky tables, defaults to PT0S - no expiration set
    2. Added `->expire($intervalIso8601)` to setup per-table expiration ISO 8601 interval utilised by `::listActiveTempTables($keyspace)` method helping to remove leaky tables, defaults to PT0S - no expiration set
    
### 0.5.0 (2020-03-22)
1. **Simpletools\Db\Cassandra\TempTable**
    1. Added register functions to improve cleanup and minimise tables leaking 
    2. Added `::registerAutoShutdown()` to enable on terminate process signal and shutdown auto cleanup 
    3. Added `::registerMaxSize()` to specify max size of opened TempTables in the current process
    4. Added `::cleanup($signal=0)` to start register cleanup and temp tables deletion, otherwise automatically triggered by `::registerAutoShutdown()`
    5. Added `->drop()` to enable individual object table cleanup
    6. Added `::listActiveTempTables($keyspace)` to get list of all active temp tables in a given keyspace

### 0.4.0 (2020-02-18)
1. **Simpletools\Db\Cassandra**
    1. Added integration with `Simpletools\Db\Replicator` to replicate data between databases
2. **Simpletools\Db\Cassandra\Query**
    1. By default foreach on Query or Result will return only first page.
    2. Added `->autoScroll()` to automatically get next page on foreach
    3. Added `->getScrollId()` and `->setScrollId()` to manually control pagination
    4. Added `->size()` to control page size
3. **Simpletools\Db\Cassandra\Result**
    1. Added `->getScrollId()` and `->autoScroll()` to control pagination
4. **Simpletools\Db\Cassandra\Async**
    1. `->add($batch)` method can handle Batch object
    2. Added `->disableReplication()` (enabled by default)
5. **Simpletools\Db\Cassandra\Batch**
    1. Added `->constraint()` to force constraint index

### 0.3.28 (2020-02-11)
1. **Simpletools\Db\Cassandra\TempTable**
    1. Added auto create
    
### 0.3.27 (2020-02-11)
1. **Simpletools\Db\Cassandra\Schema**
    1. Added support for keyspace on definition, defaults to client default keyspace
    
### 0.3.26 (2020-02-11)
1. **Simpletools\Db\Cassandra\TempTable**
    1. Introduced new TempTable() functionality
    
### 0.3.24 (2020-02-04)
1. **Simpletools\Db\Cassandra\Query**
    1. Added ->increase() and ->decrease() for counter table
2. **Simpletools\Db\Cassandra\Doc**
    1. Added ->increase() and ->decrease() for counter table
    
### 0.3.20 (2020-01-03)
1. **Simpletools\Db\Cassandra\Type\Inet**
    1. Introduced new type

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
