test-scrape:
	curl -X POST http://localhost:3002/v2/scrape \
		-H 'Content-Type: application/json' \
		-d '{ \
			"url": "http://example.com" \
		}'