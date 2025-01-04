.PHONY: up build

# Dosya izinlerini güncelleme
update-permissions:
	sudo chmod -R 777 /home/humeyra/Desktop/redpanda_learning/data

# Docker Compose ile build işlemi başlatma
build: update-permissions
	docker compose up -d --build

up: 
	docker compose up -d --remove-orphans

down:
	docker compose down

bash:
	docker exec -it tools_backend sh

logs:
	docker logs tools_backend

clear:
	$(MAKE) down
	rm -rf vendor || true
	rm .env || true
	rm -rf ./docker/mysql/ || true

restart:
	$(MAKE) down
	$(MAKE) up

migrate:
	docker exec -it tools_backend alembic upgrade head

migrate_fresh:
	$(MAKE) migrate
	docker exec -it tools_backend alembic revision --autogenerate
	$(MAKE) migrate

reload:
	docker exec -it tools_backend curl -X GET --unix-socket /var/run/control.unit.sock  \
      http://localhost/control/applications/fastapi/restart




