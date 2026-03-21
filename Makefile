# Energy Big Data Platform - Orchestration Makefile

.PHONY: up down restart build logs clean ps

# Start all services with wait conditions
up:
	docker-compose up -d

# Stop all services
down:
	docker-compose down

# Stop all services and remove volumes (Full Reset)
reset:
	docker-compose down -v

# Rebuild and restart
rebuild:
	docker-compose up -d --build

# View logs for specific service (usage: make logs s=api)
logs:
	docker-compose logs -f $(s)

# Show status of all containers
ps:
	docker-compose ps

# Clean up unused Docker resources
prune:
	docker system prune -f

# Check system health via API
health:
	curl http://localhost:8000/
