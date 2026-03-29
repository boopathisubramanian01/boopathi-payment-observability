# Payment Observability Stack

End-to-end observability for an instant payment processing pipeline. Includes log collection, storage, visualization, and an AI-powered query interface.

## Architecture

```
Java Services → log files → Promtail → Loki ← Grafana
                                            ← AI Observability API (FastAPI + Ollama)
```

### Services

| Service | Port | Description |
|---|---|---|
| Grafana | 3000 | Dashboards and log exploration |
| Loki | 3100 | Log storage backend |
| Promtail | — | Log shipper (tails `/tmp/*.log`) |
| Ollama | 11434 | Local LLM (`llama3.2:1b`) |
| AI Observability API | 8000 | FastAPI app + embedded UI |

### Payment Pipeline (separate repo)

| Service | Port | Description |
|---|---|---|
| payment-initiation-service | 8080 | Accepts Pain001 payment requests |
| payment-router | — | Routes payments to appropriate handler |
| instant-payment-handler | — | Processes instant payments |
| MongoDB | 27017 | Payment state storage |
| Kafka | 9092 | Event bus between services |

---

## Prerequisites

- Docker Desktop
- Java 21 (`openjdk@21` via Homebrew)
- Python 3.11+ (for load tests)
- `payment-initiation-service` repo (MongoDB + Kafka dependencies)

---

## Startup

### 1. Start infrastructure dependencies

```bash
cd /Users/boopathi.subramania/payment-initiation-service
docker compose up -d mongodb kafka kafka-ui
```

### 2. Start observability stack

```bash
cd /Users/boopathi.subramania/payment-observability
docker compose up -d
```

Wait ~15 seconds for Ollama to pull `llama3.2:1b` on first run.

### 3. Start Java payment services

Run each in a separate terminal:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

# Terminal 1
cd /Users/boopathi.subramania/payment-initiation-service
java -jar target/payment-initiation-service-1.0.0.jar

# Terminal 2
cd /Users/boopathi.subramania/payment-router
java -jar target/payment-router-1.0.0.jar

# Terminal 3
cd /Users/boopathi.subramania/instant-payment-handler
java -jar target/instant-payment-handler-1.0.0.jar
```

---

## Usage

### AI Observability UI

Open `http://localhost:8000` in a browser.

- **Payment Lookup**: Enter a payment ID to get a summary of its status, current service, and event history
- **LogQL Query**: Run a custom Loki query and get an AI-generated summary
- **Use Ollama model** checkbox: Unchecked by default (returns instant rule-based summary). Check to invoke the local LLM (adds ~15–20s)

### Grafana

Open `http://localhost:3000` (admin / admin).

- Loki datasource is pre-provisioned
- Use **Explore** → select Loki → query by service label, e.g. `{service="payment-initiation"}`

### REST API

**Payment lookup:**
```bash
curl -X POST http://localhost:8000/ai/ask \
  -H 'Content-Type: application/json' \
  -d '{"question":"What is the status?","paymentId":"PAY-xxx","hours":24}'
```

**LogQL query:**
```bash
curl -X POST http://localhost:8000/ai/logql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{service=\"payment-router\"}","question":"Summarize activity","hours":24,"limit":200}'
```

**Health check:**
```bash
curl http://localhost:8000/health
```

---

## Load Testing

```bash
python3 /Users/boopathi.subramania/load_test.py \
  --requests 5 \
  --concurrency 2 \
  --instant-pct 100 \
  --invalid-pct 0 \
  --verbose
```

---

## Log Files

Promtail tails these files and ships logs to Loki:

| File | Service Label |
|---|---|
| `/tmp/initiation.log` | `payment-initiation` |
| `/tmp/router.log` | `payment-router` |
| `/tmp/handler.log` | `payment-handler` |

> **Tip:** If Grafana or the AI API returns `eventCount: 0`, check that Promtail is running:
> ```bash
> docker compose ps promtail
> # If stopped:
> docker compose up -d promtail
> ```

---

## Project Structure

```
payment-observability/
├── docker-compose.yml          # All observability services
├── ollama-entrypoint.sh        # Auto-pulls llama3.2:1b on start
├── ai-observability/
│   ├── app.py                  # FastAPI app + embedded HTML UI
│   ├── Dockerfile
│   └── requirements.txt
├── grafana/
│   └── provisioning/
│       └── datasources/        # Loki datasource auto-provisioned
├── loki/
│   └── loki-config.yml
└── promtail/
    └── promtail-config.yml
```

---

## Ollama Model

Default model: `llama3.2:1b` (1.3 GB, CPU-friendly).

To switch models, update `OLLAMA_MODEL` in `docker-compose.yml` and rebuild:

```bash
docker compose up -d --build ai-observability
```
