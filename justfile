build:
	docker compose --progress plain build

test-scrape:
	curl -X POST http://localhost:3002/v2/scrape \
		-H 'Content-Type: application/json' \
		-d '{ \
			"url": "http://example.com" \
		}'

test-crawl:
	curl -X POST http://localhost:3002/v2/crawl \
		-H 'Content-Type: application/json' \
		-d '{ \
			"url": "http://example.com" \
		}'