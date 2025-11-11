# Rust IoT & Information Hub

## 1. Project Overview

This project is a high-performance, microservice-based IoT and data aggregation platform built entirely in **Rust**. It is designed to run in containers, orchestrated by **K3s** on a low-power device like a Raspberry Pi.

The architecture is **event-driven**, using an MQTT broker as the central message bus. This design allows for high decoupling, scalability, and resilience. Data is ingested from various sources, processed, stored in a polyglot persistence layer (Time-series, Document, and Relational), indexed for search, and served via a REST API and a simple HTML5 dashboard.

### Core Principles

* **Language:** Rust, focusing on safety, performance, and idiomatic "OOP" patterns (Traits, Structs, Enums, and Design Patterns).
* **Architecture:** Decoupled microservices communicating via an MQTT event bus.
* **Deployment:** All Rust services are individual Docker containers, managed by a K3s cluster.
* **Data Storage:** Polyglot persistence (using the right database for the right job).

---

## 2. System Architecture & Components

The system is composed of several independent services, each running in its own container.



### 2.1. Core Broker
* **Service:** **Mosquitto (MQTT Server)**
* **Role:** The central nervous system of the application. It follows a **Pub/Sub pattern**, decoupling data producers (Ingestion) from data consumers (Processing).

### 2.2. Data Ingestion Layer (Rust)
This layer is responsible for getting data *into* the system.

* **`Input Servers` (Multiple)**
    * **Purpose:** Listens for incoming data (e.g., from IoT devices, webhooks).
    * **Config:** Reads configuration (e.g., ports, topics) from a JSON string in environment variables.
    * **Logic:**
        1.  Receives data.
        2.  Uses an internal buffer to avoid blocking the input.
        3.  Validates data against **RegEx patterns** (loaded from config).
        4.  Valid data is published as a JSON message to a specific MQTT topic.
        5.  Invalid/Valid data is logged to **stdout** (for K3s `kubectl logs`).
* **`Downloader Servers` (Multiple)**
    * **Purpose:** Periodically fetches data from public-facing JSON APIs.
    * **Pattern:** Acts as an **Adapter** between external APIs and our internal MQTT bus.
    * **Logic:** Fetches data, formats it as standard JSON, and publishes to an MQTT topic.
* **`Scraper Servers` (Multiple)**
    * **Purpose:** Periodically scrapes data from configured websites.
    * **Pattern:** Also an **Adapter**.
    * **Logic:** Scrapes HTML, extracts data, formats as JSON, and publishes to an MQTT topic.

### 2.3. Data Processing Layer (Rust)

* **`Parsers` (Multiple)**
    * **Purpose:** Subscribes to MQTT topics, processes the raw JSON, and stores it in the correct database.
    * **Pattern:** Uses a **Strategy Pattern**. Each parser is configured with a "storage strategy".
    * **Config:** Reads a JSON mapping from environment variables (e.g., `{"mqtt-topic-A": "postgres_strategy", "mqtt-topic-B": "valkey_strategy"}`).
    * **Logic:**
        1.  Subscribes to one or more MQTT topics.
        2.  Receives a JSON message.
        3.  Transforms/parses the data.
        4.  Writes data to the configured database (Postgres, Valkey, etc.).
        5.  Logs success or failure to stdout.

### 2.4. Data Storage Layer (External to K3s)
These services run outside the K3s cluster (or as `StatefulSet`s) to persist data.

* **`PostgreSQL + TimescaleDB`:** Primary store for time-series data (e.g., sensor readings, metrics).
* **`ValkeyDB`:** A high-performance Key/Value store (Redis fork) used for storing simple JSON documents, session data, or caches.
* **`SQLite`:** Used for smaller, simpler relational data (e.g., configuration, user metadata) that doesn't warrant a full Postgres instance.

### 2.5. Search & Integration (External + Rust)

* **`ManticoreSearch` (External):** A high-performance search engine. It will be configured to index data from both PostgreSQL and ValkeyDB to provide fast full-text search capabilities.
* **`Integration Servers` (Rust, Multiple):**
    * **Purpose:** Provides two-way integration with other third-party systems.
    * **Logic:** Can both subscribe to MQTT topics to trigger external actions (e.g., send an email) and publish to MQTT topics based on external events.

### 2.6. Presentation Layer (Rust)

* **`API Server`**
    * **Purpose:** Provides a centralized, secure **REST API** for all data in the hub.
    * **Pattern:** Acts as a **Facade**, hiding the complexity of the multiple data stores.
    * **Features:**
        * Asynchronous and multithreaded (built with `axum` or `actix-web`).
        * Follows the **OpenAPI standard** for API documentation.
        * Queries Valkey, Postgres, and ManticoreSearch to serve data.
* **`Web UI Server (Dashboard)`**
    * **Purpose:** A simple, lightweight web dashboard for monitoring the system.
    * **Framework:** Built in Rust (e.g., using `axum` for routing and `askama` for templating).
    * **Style:** Pure HTML5 and minimal CSS (no heavy frameworks).
    * **Pattern:** Follows an **MVC** (Model-View-Controller) pattern, where "Models" are data structures fetched from the `API Server`.
    * **Features (Pages):**
        1.  **ValkeyDB Viewer:** A page to browse Key/Value data from Valkey.
        2.  **MQTT Live:** A view of active MQTT channels and recent messages.
        3.  **MQTT History:** A queryable history of messages (likely pulled from a database where parsers log them).
        4.  **Postgres Data:** A view/table of data from the PostgreSQL database.
        5.  **Search:** A search interface powered by ManticoreSearch.

---

## 3. Deployment

* All Rust services (`InputServer`, `Parser`, `APIServer`, etc.) will be packaged as separate, minimal Docker images (e.g., using `alpine` or `distroless`).
* `Dockerfile`s and Kubernetes `.yaml` definitions (Deployments, Services, ConfigMaps) will be created for each service.
* The entire stack will be deployed to a **K3s** cluster.
* Service configuration (JSON configs) will be injected into containers via K3s `ConfigMaps` or `Secrets` (mounted as environment variables).

---

## 4. Learning Objectives (Rust)

This project serves as a practical exercise for learning advanced Rust concepts in a "real-world" scenario:

* **Idiomatic Rust "OOP":** Implementing design patterns (Facade, Strategy, Adapter, Builder) using Traits and Structs.
* **Asynchronous Programming:** Extensive use of `tokio` and `async/await` for all I/O bound services (servers, database clients, MQTT clients).
* **Concurrency & Multithreading:** Building high-performance, thread-safe servers.
* **Error Handling:** Robust error handling using Rust's `Result` and `Option` enums.
* **Configuration Management:** Deserializing JSON configuration (`serde_json`) from environment variables.
* **Database Interaction:** Using Rust clients (e.g., `sqlx`, `redis-rs`) in an async context.
* **Web Services:** Building both REST APIs (`axum`, `serde`) and server-side rendered UIs (`askama`).