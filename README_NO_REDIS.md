# OLX Scraper - Optimized Version (No Redis)

## Overview

This is an optimized version of the OLX scraper that removes Redis database communication for better performance. Instead of storing URLs to a database, it simply logs them to the console.

## Key Features

### 1. No Database Overhead
- **No Redis connection** - eliminates network latency
- **No database writes** - removes I/O bottleneck
- **Faster execution** - pure scraping without storage delays

### 2. Graceful Mid-Task Stopping
- Press `Ctrl+C` to stop gracefully at any time
- Browser windows are closed properly
- No orphaned processes
- Clean shutdown with summary statistics

### 3. URL Logging
- All found URLs are logged to console with prices
- Easy to redirect output to a file if needed
- Organized by page and task

## Files

### Main Files (Optimized Version)
- `scraper_main_no_redis.py` - Main orchestrator without Redis
- `scraper_class_no_redis.py` - Scraper class that logs URLs instead of storing

### Original Files (With Redis)
- `scraper_main.py` - Main orchestrator with Redis integration
- `scraper_class.py` - Scraper class with full Redis storage
- `redis_client.py` - Redis client implementations

## Usage

### Basic Usage

```bash
python scraper_main_no_redis.py
```

### Save Output to File

```bash
python scraper_main_no_redis.py > scraper_output.log 2>&1
```

### Stop Gracefully

Press `Ctrl+C` at any time. The scraper will:
1. Stop accepting new tasks
2. Complete current page extraction
3. Close all browser windows
4. Display final statistics
5. Exit cleanly

## Output Format

The scraper logs URLs in this format:

```
================================================================================
📄 PAGE 1 - 50 URLs Found - [/distrito-federal-e-regiao/brasilia_casas_venda]
================================================================================
  1.      R$ 450,000 | https://www.olx.com.br/imoveis/venda/casas/1234567890
  2.      R$ 320,000 | https://www.olx.com.br/imoveis/venda/casas/1234567891
  3.        No price | https://www.olx.com.br/imoveis/venda/casas/1234567892
  ...
================================================================================
```

## Configuration

Edit `scraper_main_no_redis.py` to adjust:

```python
MAX_WORKERS = 3              # Number of parallel tasks
MAX_PAGES_PER_TASK = 100     # Maximum pages per location/type
LOCATIONS = [...]            # List of locations to scrape
PROPERTY_TYPES = [...]       # Types: apartamentos, casas
TRANSACTION_TYPES = [...]    # Types: venda, aluguel
```

## Performance Comparison

### With Redis (Original)
- Database connection overhead
- Network latency for each write
- Deduplication checks
- Stream publishing
- Slower overall performance

### Without Redis (Optimized)
- **No database overhead**
- **No network latency**
- **No I/O bottleneck**
- **Faster execution**
- **Simpler debugging** (all output visible in console)

## When to Use Each Version

### Use NO REDIS version (`scraper_main_no_redis.py`) when:
- Testing and development
- Performance benchmarking
- Quick URL collection without storage
- You want to process URLs in a different way
- Debugging scraper logic

### Use REDIS version (`scraper_main.py`) when:
- Production environment
- Need persistent storage
- Multiple workers processing URLs
- Deduplication is critical
- Integration with Airtable sync

## Example Session

```bash
$ python scraper_main_no_redis.py

============================================================
 OLX SCRAPER - OPTIMIZED (NO REDIS)
============================================================
 Workers: 3
 Locations: 33
 Property Types: 2
 Transaction Types: 2
 Max Pages per Task: 100
 Mode: LOGGING ONLY (No Database)
============================================================
 Press Ctrl+C to stop gracefully
============================================================

... [scraping in progress] ...

[Press Ctrl+C]

============================================================
🛑 SHUTDOWN SIGNAL RECEIVED
============================================================
Stopping current tasks...
Browsers will be closed gracefully...

============================================================
 SCRAPING COMPLETE
============================================================
 Time: 245.3s (4.1 min)
 Tasks: 15 succeeded, 0 failed, 2 no results
 Pages: 1,247
 URLs Found: 62,350
============================================================
```

## Troubleshooting

### Issue: Browser windows not closing
- The optimized version properly closes browsers on shutdown
- If browsers remain open, check for exceptions in the log
- Force kill: `pkill -f chrome`

### Issue: Output too verbose
- Reduce logging level in code: `logging.basicConfig(level=logging.WARNING)`
- Filter output: `python scraper_main_no_redis.py 2>/dev/null`

### Issue: Want to save only URLs without other logs
- Use grep to filter: `python scraper_main_no_redis.py 2>&1 | grep "https://"`

## Technical Details

### Browser Management
- Uses SeleniumBase with UC mode for anti-bot protection
- Automatic browser restart on failures
- Proper cleanup in all scenarios (success, failure, interrupt)

### State-Based Navigation
- Detects page states: SUCCESS, NO_RESULTS, CAPTCHA, TIMEOUT, UNKNOWN
- Retry logic for transient failures
- Page mismatch recovery

### Graceful Shutdown
- Signal handlers for SIGINT (Ctrl+C) and SIGTERM
- Shared shutdown event across all threads
- Browser cleanup in `finally` blocks
- Clean exit with statistics

## Development

To modify the scraper behavior:

1. Edit extraction logic in `scraper_class_no_redis.py::get_page_data()`
2. Adjust logging in `log_urls_batch()`
3. Modify task generation in `scraper_main_no_redis.py::generate_tasks()`

## Support

For issues or questions:
1. Check console output for error messages
2. Review debug screenshots in `debug_screenshots/` directory
3. Enable debug logging: `logging.basicConfig(level=logging.DEBUG)`
