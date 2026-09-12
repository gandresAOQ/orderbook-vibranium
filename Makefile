.PHONY: build run test vet fmt tidy loadtest clean \
        up down logs ps reset psql topic groups replay-test \
        up-memory down-memory diagram

# ---------- local (no infrastructure) ----------

build:
	go build -o bin/orderbook ./cmd/orderbook

run: ## run in-process, zero infrastructure
	go run ./cmd/orderbook

test:
	go test ./... -race -count=1

vet:
	go vet ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

clean:
	rm -rf bin *.out

# ---------- full stack (Redpanda + Postgres + Redis) ----------

up: ## start the full architecture
	docker compose up --build -d
	@echo "waiting for the API to become healthy..."
	@until curl -sf localhost:3000/health >/dev/null 2>&1; do sleep 1; done
	@echo "ready -> http://localhost:3000  (console: http://localhost:8080)"

down: ## stop the stack, keep data volumes
	docker compose down

reset: ## stop the stack AND delete all data
	docker compose down -v

logs:
	docker compose logs -f orderbook

ps:
	docker compose ps

# ---------- in-memory stack (single container) ----------

up-memory: ## start the zero-infrastructure variant
	docker compose -f docker-compose.memory.yml up --build -d

down-memory:
	docker compose -f docker-compose.memory.yml down

# ---------- load & verification ----------

loadtest: ## 5000 pairs, then reconcile balances
	go run ./scripts/loadtest -pairs 5000 -concurrency 200

# ---------- architecture diagram ----------

# Regenerates docs/architecture/aws-architecture.png from the Python source,
# using the official AWS Architecture Icons bundled with the `diagrams` package.
# Requires graphviz on the PATH: brew install graphviz
DIAGRAM_VENV := .venv-diagrams

diagram: ## regenerate the AWS architecture diagram
	@command -v dot >/dev/null 2>&1 || { echo "graphviz missing: brew install graphviz"; exit 1; }
	@test -d $(DIAGRAM_VENV) || python3 -m venv $(DIAGRAM_VENV)
	@$(DIAGRAM_VENV)/bin/pip install --quiet diagrams
	@$(DIAGRAM_VENV)/bin/python docs/architecture/aws_architecture.py

# ---------- inspection helpers (for the demo) ----------

psql: ## open a psql shell
	docker compose exec postgres psql -U orderbook -d orderbook

topic: ## show the event log topic
	docker compose exec redpanda rpk topic list
	docker compose exec redpanda rpk topic describe orderbook.events

groups: ## show settlement consumer lag
	docker compose exec redpanda rpk group describe settlement

# Force a full redelivery of the event log and prove money is not duplicated.
replay-test: ## rewind settlement to offset 0 and verify balances are unchanged
	@echo "totals BEFORE replay:"
	@curl -s localhost:3000/wallets | \
	  python3 -c "import sys,json;w=json.load(sys.stdin);print(' COP=',sum(x['copAvailable']+x['copLocked'] for x in w),' VIB=',sum(x['vibraniumAvailable']+x['vibraniumLocked'] for x in w))"
	docker compose stop orderbook
	docker compose exec redpanda rpk group seek settlement --to start
	docker compose start orderbook
	@echo "re-consuming the whole log..."
	@sleep 20
	@echo "totals AFTER replay (must be identical):"
	@curl -s localhost:3000/wallets | \
	  python3 -c "import sys,json;w=json.load(sys.stdin);print(' COP=',sum(x['copAvailable']+x['copLocked'] for x in w),' VIB=',sum(x['vibraniumAvailable']+x['vibraniumLocked'] for x in w))"
