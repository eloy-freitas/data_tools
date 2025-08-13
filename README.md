# Data Tools - Multithreaded ETL Framework

A high-performance, multithreaded ETL (Extract, Transform, Load) framework designed for PostgreSQL databases using Python and SQLAlchemy. This framework implements a producer-consumer pattern with thread synchronization to efficiently process large datasets.

## 🏗️ Architecture Overview

The framework is built around a producer-consumer architecture with the following key components:

### Core Components

- **Monitor**: Central coordinator that manages thread synchronization and data flow
- **Workers**: Producer and consumer threads that handle data processing
- **Connection Management**: Factory pattern for database connections
- **Templates**: High-level ETL orchestration classes
- **Utilities**: Database operations and logging support

## 📁 Project Structure

```
src/
├── connection/
│   ├── __init__.py
│   └── postgres_connection_factory.py    # Database connection factory
├── monitors/
│   ├── __init__.py
│   └── monitor.py                        # Thread coordination and synchronization
├── workers/
│   ├── __init__.py
│   ├── base_worker.py                    # Abstract worker base class
│   ├── sqlalchemy_producer.py           # Data producer worker
│   └── sqlalchemy_consumer.py           # Data consumer worker
├── templates/
│   ├── __init__.py
│   ├── template_stage_ad_hoc_query_multithread.py    # Ad-hoc query ETL template
│   └── template_stage_copy_table_multithread.py     # Table copy ETL template
├── utils/
│   ├── log/
│   │   ├── __init__.py
│   │   └── log_utils.py                  # Logging utilities
│   └── table/
│       ├── __init__.py
│       └── table_manager.py              # Database table operations
├── resources/
│   └── postgres_connections.json         # Database connection configurations
└── __init__.py
```

## 🔧 Core Classes

### Monitor
Central coordinator that manages the producer-consumer pattern:
- **Buffer Management**: Thread-safe data buffer with configurable size
- **Worker Coordination**: Manages producer and consumer thread lifecycle
- **Synchronization**: Uses threading primitives for safe concurrent operations

### BaseWorker (Abstract)
Base class for all worker threads:
- Extends Python's `Thread` class
- Provides common functionality for producers and consumers
- Abstract `run()` method for specific implementations

### SQLAlchemyProducer
Data producer that reads from source databases:
- Streams data using SQLAlchemy with configurable chunk sizes
- Optimized for large datasets with `stream_results` and `yield_per`
- Generates insert queries dynamically based on source schema

### SQLAlchemyConsumer
Data consumer that writes to target databases:
- Processes data from the monitor's buffer
- Executes batch inserts with transaction management
- Handles errors with automatic rollback

### PostgresConnectionFactory
Database connection management:
- Reads connection configurations from JSON files
- Creates SQLAlchemy engines with PostgreSQL-specific optimizations
- Supports multiple database connections

### TableManager
Database utility operations:
- Table schema inspection and column extraction
- Dynamic query generation for SELECT and INSERT operations
- Table truncation and data insertion with transaction support

## 📋 ETL Templates

### StageAdHocMultiThread
For executing custom SQL queries:
```python
template = StageAdHocMultiThread(
    query="SELECT * FROM source_table WHERE condition",
    table_name_target="target_table",
    conn_input=input_engine,
    conn_output=output_engine,
    table_manager=table_manager,
    log_utils=log_utils,
    consumers=4,  # Number of consumer threads
    chunksize=20000  # Rows per batch
)
template.run()
```

## Mermaid Class Diagram

```mermaid
classDiagram
    %% Abstract Base Classes
    class BaseWorker {
        <<abstract>>
        -_stop: Event
        -_monitor: Monitor
        -_is_producer: bool
        +__init__(monitor, is_producer)
        +run()* 
        +stop()
        +stop_all_workers()
        +is_producer: bool
    }

    %% Connection Management
    class PostgresConnectionFactory {
        +__init__()
        +read_file(conn_id: str, file_path: str) dict[str, str]
        +create_connection_url(connection_dict: dict) str
        +create_engine_by_file(conn_id: str, file_path: str) Engine
    }

    %% Monitor/Coordinator
    class Monitor {
        -_buffer: list[tuple]
        -_buffer_size: int
        -_workers: list
        -_producers_online: int
        -_mutex: Semaphore
        -_full: Condition
        -_empty: Condition
        -_insert_query_available: Condition
        -_end_process: Event
        -_timeout: int
        -_insert_query: str
        +__init__(buffer_size: int, timeout: int)
        +write(data: object)
        +read() object
        +notify_all()
        +stop_all_workers()
        +producer_end_process()
        +subscribe(worker)
        +start()
        +set_insert_query(query: str)
        +get_insert_query() str
        +wait_for_completion()
        +signal_end_process()
    }

    %% Worker Implementations
    class SQLAlchemyProducer {
        -_engine: Engine
        -_query: str
        -_max_rows_buffer: int
        -_chunksize: int
        -_table_manager: TableManager
        -_table_target: str
        +__init__(monitor, engine, query, max_rows_buffer, chunksize, table_manager, table_target)
        +run()
    }

    class SQLAlchemyConsumer {
        -_engine: Engine
        -_table_manager: TableManager
        -_insert_query_template: str
        +__init__(monitor, engine, table_manager)
        +run()
    }

    %% Utility Classes
    class TableManager {
        +__init__()
        +truncate_table(conn: Engine, table_name: str, schema: str)
        +get_table_columns(conn: Engine, table_name: str, schema: str) list[str]
        +create_select_query(table_name: str, columns: list[str], schema: str, ignore_columns: list[str]) str
        +insert(data: object, conn: Connection, cursor: object, insert_query_template: str)
        +build_insert_query(table_name: str, columns: list[str]) str
    }

    class LogUtils {
        +__init__()
        +get_logger(name: str) Logger
    }

    %% Template Classes
    class StageAdHocMultiThread {
        -_query: str
        -_table_name_target: str
        -_conn_input: Engine
        -_conn_output: Engine
        -_consumers: int
        -_monitor_timeout: int
        -_monitor_buffer_size: int
        -_max_rows_buffer: int
        -_chunksize: int
        -_table_manager: TableManager
        -_log_utils: LogUtils
        -_logger: Logger
        -_monitor: Monitor
        +__init__(query, table_name_target, conn_input, conn_output, table_manager, log_utils, ...)
        +init_services()
        +run()
    }

    class StageCopyTableMultiThread {
        -_table_name_source: str
        -_table_name_target: str
        -_conn_input: Engine
        -_conn_output: Engine
        -_consumers: int
        -_monitor_timeout: int
        -_monitor_buffer_size: int
        -_max_rows_buffer: int
        -_chunksize: int
        -_log_utils: LogUtils
        -_table_manager: TableManager
        -_logger: Logger
        -_monitor: Monitor
        +__init__(table_name_source, table_name_target, conn_input, conn_output, log_utils, table_manager, ...)
        +init_services()
        +run()
    }

    %% External Dependencies
    class Thread {
        <<external>>
    }
    
    class Engine {
        <<external>>
    }

    %% Relationships
    BaseWorker --|> Thread
    SQLAlchemyProducer --|> BaseWorker
    SQLAlchemyConsumer --|> BaseWorker
    
    Monitor --> BaseWorker : manages
    SQLAlchemyProducer --> Monitor : writes to
    SQLAlchemyConsumer --> Monitor : reads from
    SQLAlchemyProducer --> TableManager : uses
    SQLAlchemyConsumer --> TableManager : uses
    
    StageAdHocMultiThread --> Monitor : orchestrates
    StageAdHocMultiThread --> SQLAlchemyProducer : creates
    StageAdHocMultiThread --> SQLAlchemyConsumer : creates
    StageAdHocMultiThread --> TableManager : uses
    StageAdHocMultiThread --> LogUtils : uses
    
    StageCopyTableMultiThread --> Monitor : orchestrates
    StageCopyTableMultiThread --> SQLAlchemyProducer : creates
    StageCopyTableMultiThread --> SQLAlchemyConsumer : creates
    StageCopyTableMultiThread --> TableManager : uses
    StageCopyTableMultiThread --> LogUtils : uses
    
    PostgresConnectionFactory --> Engine : creates
```

## How to use:

1. Install Python 3.10.11;
2. Install Python requirements;
```
pip install -r requirements.txt --no-cache-dir
```
3. Run database docker compose:
```
docker compose -f postgres_compose.yml up -d
```
4. Run main.py or main2.py
```
python main.py
```