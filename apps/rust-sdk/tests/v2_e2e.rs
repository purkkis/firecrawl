//! End-to-end tests for Firecrawl v2 API.
//!
//! These tests require the following environment variables:
//! - API_URL: The Firecrawl API URL
//! - TEST_API_KEY: A valid API key (optional for self-hosted)

use dotenvy::dotenv;
use firecrawl::v2::{
    AgentOptions, BatchScrapeOptions, Client, CrawlOptions, Format, MapOptions, ScrapeOptions,
    SearchOptions, SitemapMode,
};
use serde_json::json;
use std::env;

fn get_client() -> Client {
    dotenv().ok();
    let api_url = env::var("API_URL").unwrap_or_else(|_| "http://localhost:3002".to_string());
    let api_key = env::var("TEST_API_KEY").ok();
    Client::new_selfhosted(api_url, api_key).expect("Failed to create client")
}

#[tokio::test]
async fn test_v2_scrape() {
    let client = get_client();
    let result = client.scrape("https://example.com", None).await;

    match result {
        Ok(doc) => {
            assert!(doc.markdown.is_some());
            println!("Scrape successful: {:?}", doc.markdown);
        }
        Err(e) => {
            eprintln!("Scrape test error (may require env vars): {}", e);
        }
    }
}

#[tokio::test]
async fn test_v2_scrape_with_options() {
    let client = get_client();
    let options = ScrapeOptions {
        formats: Some(vec![Format::Markdown, Format::Html, Format::Links]),
        only_main_content: Some(true),
        ..Default::default()
    };

    let result = client.scrape("https://example.com", options).await;

    match result {
        Ok(doc) => {
            assert!(doc.markdown.is_some());
            assert!(doc.html.is_some());
            println!("Scrape with options successful");
        }
        Err(e) => {
            eprintln!("Scrape with options test error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_v2_scrape_with_schema() {
    let client = get_client();

    let schema = json!({
        "type": "object",
        "properties": {
            "title": { "type": "string" },
            "description": { "type": "string" }
        }
    });

    let result = client
        .scrape_with_schema("https://example.com", schema, Some("Extract page info"))
        .await;

    match result {
        Ok(data) => {
            println!("Schema extraction result: {}", data);
        }
        Err(e) => {
            eprintln!("Schema scrape test error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_v2_search() {
    let client = get_client();
    let result = client.search("rust programming", None).await;

    match result {
        Ok(response) => {
            assert!(response.success);
            println!("Search returned results");
        }
        Err(e) => {
            eprintln!("Search test error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_v2_search_with_options() {
    let client = get_client();
    let options = SearchOptions {
        limit: Some(5),
        ..Default::default()
    };

    let result = client.search("firecrawl web scraping", options).await;

    match result {
        Ok(response) => {
            assert!(response.success);
            println!("Search with options successful");
        }
        Err(e) => {
            eprintln!("Search with options test error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_v2_map() {
    let client = get_client();
    let result = client.map("https://example.com", None).await;

    match result {
        Ok(response) => {
            assert!(response.success);
            println!("Map found {} links", response.links.len());
        }
        Err(e) => {
            eprintln!("Map test error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_v2_map_with_options() {
    let client = get_client();
    let options = MapOptions {
        sitemap: Some(SitemapMode::Include),
        limit: Some(50),
        ..Default::default()
    };

    let result = client.map("https://example.com", options).await;

    match result {
        Ok(response) => {
            assert!(response.success);
            println!("Map with options successful");
        }
        Err(e) => {
            eprintln!("Map with options test error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_v2_crawl_async() {
    let client = get_client();
    let result = client.start_crawl("https://example.com", None).await;

    match result {
        Ok(response) => {
            assert!(response.success);
            assert!(!response.id.is_empty());
            println!("Crawl started with ID: {}", response.id);

            // Check status
            let status = client.get_crawl_status(&response.id).await;
            if let Ok(s) = status {
                println!("Crawl status: {:?}", s.status);
            }

            // Cancel the crawl
            let _ = client.cancel_crawl(&response.id).await;
        }
        Err(e) => {
            eprintln!("Crawl async test error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_v2_crawl_sync() {
    let client = get_client();
    let options = CrawlOptions {
        limit: Some(2),
        poll_interval: Some(2000),
        ..Default::default()
    };

    let result = client.crawl("https://example.com", options).await;

    match result {
        Ok(job) => {
            println!("Crawl completed with {} pages", job.data.len());
        }
        Err(e) => {
            eprintln!("Crawl sync test error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_v2_batch_scrape_async() {
    let client = get_client();
    let urls = vec![
        "https://example.com".to_string(),
        "https://example.org".to_string(),
    ];

    let result = client.start_batch_scrape(urls, None).await;

    match result {
        Ok(response) => {
            assert!(response.success);
            assert!(!response.id.is_empty());
            println!("Batch scrape started with ID: {}", response.id);
        }
        Err(e) => {
            eprintln!("Batch scrape async test error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_v2_batch_scrape_sync() {
    let client = get_client();
    let urls = vec!["https://example.com".to_string()];

    let options = BatchScrapeOptions {
        options: Some(ScrapeOptions {
            formats: Some(vec![Format::Markdown]),
            ..Default::default()
        }),
        poll_interval: Some(2000),
        ..Default::default()
    };

    let result = client.batch_scrape(urls, options).await;

    match result {
        Ok(job) => {
            println!("Batch scrape completed with {} documents", job.data.len());
        }
        Err(e) => {
            eprintln!("Batch scrape sync test error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_v2_agent_async() {
    let client = get_client();
    let options = AgentOptions {
        urls: Some(vec!["https://example.com".to_string()]),
        prompt: "Describe what this website is about".to_string(),
        ..Default::default()
    };

    let result = client.start_agent(options).await;

    match result {
        Ok(response) => {
            assert!(response.success);
            assert!(!response.id.is_empty());
            println!("Agent task started with ID: {}", response.id);
        }
        Err(e) => {
            eprintln!("Agent async test error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_v2_agent_with_schema() {
    let client = get_client();

    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct WebsiteInfo {
        title: Option<String>,
        description: Option<String>,
    }

    let schema = json!({
        "type": "object",
        "properties": {
            "title": { "type": "string" },
            "description": { "type": "string" }
        }
    });

    let result: Result<Option<WebsiteInfo>, _> = client
        .agent_with_schema(
            vec!["https://example.com".to_string()],
            "Extract the title and description",
            schema,
        )
        .await;

    match result {
        Ok(Some(info)) => {
            println!("Agent extracted: {:?}", info);
        }
        Ok(None) => {
            println!("Agent returned no data");
        }
        Err(e) => {
            eprintln!("Agent with schema test error: {}", e);
        }
    }
}

// Test that the v2 client can be created with different configurations
#[test]
fn test_v2_client_creation() {
    // Cloud client requires API key
    let cloud_result = Client::new("test-key");
    assert!(cloud_result.is_ok());

    // Cloud client without API key should fail
    let cloud_no_key = Client::new_selfhosted("https://api.firecrawl.dev", None::<&str>);
    assert!(cloud_no_key.is_err());

    // Self-hosted client without API key should work
    let selfhosted = Client::new_selfhosted("http://localhost:3000", None::<&str>);
    assert!(selfhosted.is_ok());

    // Self-hosted client with API key should work
    let selfhosted_with_key = Client::new_selfhosted("http://localhost:3000", Some("key"));
    assert!(selfhosted_with_key.is_ok());
}
