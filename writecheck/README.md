# WriteCheck

WriteCheck is a tool to automatically detect write-specific serializability violations in DBMSs.

## Implementation

* Transaction test case generation: WriteCheck extends SQLancer to generate deterministic transaction test cases.
  * WriteCheck first utilizes SQLancer to generate initial databases and individual SQL statements.
  * Then, WriteCheck generates a group of transactions along with a submitted order for all the statements within those transactions.
* Actual concurrent schedule obtainment: WriteCheck develops a transaction execution protocol to obtain an actual concurrent schedule of a transaction test case.
* Write-specific serializability violation identification: WriteCheck infers the serial schedule of a transaction test case under write-specific serializability,
  and compares the final database states in actual schedule with those in transaction-level and statement-level serial schedules.

## Overview

* src/sqlancer/: Contains the source code for SQLancer.
* src/sqlancer/common/transaction/: Contains the source code for transaction generation and scheduling, as implemented by WriteCheck.
* src/sqlancer/`dbms`/transaction/: Contains the source code for transaction generation and scheduling, customized for target DBMS, as implemented by WriteCheck.
* src/sqlancer/`dbms`/oracle/transaction/: Contains the source code for detect write-specific serializability violations, customized for target DBMS, as implemented by WriteCheck.

## Requirements

* [Java 11](https://www.oracle.com/java/technologies/downloads/#java11) or above (`sudo apt install openjdk-11-jdk` on Ubuntu)
* [Maven](https://maven.apache.org/) (`sudo apt install maven` on Ubuntu)
* [Docker](https://www.docker.com/) for deploying tested DBMSs if necessary
* Supported DBMSs:
  * [MySQL](https://hub.docker.com/_/mysql) (tested version: 8.0.25, using Docker container)
  * [PostgreSQL](https://hub.docker.com/_/postgres) (tested version: 15.2, using Docker container)
  * SQLite (tested version: 3.36.0, embedded in WriteCheck)
  * [MariaDB](https://hub.docker.com/_/mariadb) (tested version: 10.5.12, using Docker container)
  * [CockroachDB](https://www.cockroachlabs.com/docs/v22.2/install-cockroachdb-linux) (tested version: 22.2.5)
  * [TiDB](https://docs.pingcap.com/tidb/stable/quick-start-with-tidb) (tested version: 5.2.0, with 1 TiDB instance, 1 TiKV instances and 1 PD instances)

# Running WriteCheck

## Compile

The following commands put all dependencies into the subdirectory target/lib/ and create a Jar for WriteCheck:
```
cd writecheck
mvn package -DskipTests
```

## Usage
Important Parameters:
* `--host`: The host used to log into the target DBMS. Default: `127.0.0.1`
* `--port`: The port used to log into the target DBMS. Default: `3306`
* `--username`: The username used to log into the target DBMS. Default: `"root"`
* `--password`: The password used to log into the target DBMS. Default: `""`

## Testing

The following commands automatically generate transaction test cases for testing DBMSs.

```bash
cd target
java -jar sqlancer*.jar --host 127.0.0.1 --port 3306 --username root --password root mysql --oracle WRITE_CHECK
```