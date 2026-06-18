up: ## Start Redis
	docker compose up -d

down: ## Stop Redis
	docker compose down

down-volumes: ## Stop Redis and remove volumes
	docker compose down -v

test: ## Run tests
	npx vitest run

types: ## Generate .d.ts from JSDoc
	npx tsc --declaration --allowJs --emitDeclarationOnly --skipLibCheck \
		--target es2020 --module nodenext --moduleResolution nodenext \
		--strict false --esModuleInterop true --outDir ./types \
		src/index.js src/cache.js

logs: ## Show Redis logs
	docker compose logs -f

.PHONY: up down down-volumes test types logs help
help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(firstword $(MAKEFILE_LIST)) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[32m%-20s\033[0m %s\n", $$1, $$2}'
