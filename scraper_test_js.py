#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
OLX Scraper - Minimal JS Testing
Understanding JS execution from scratch
"""
import time
import logging
from seleniumbase import sb_cdp

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Suppress noisy libraries
for lib in ["seleniumbase", "selenium", "urllib3"]:
    logging.getLogger(lib).setLevel(logging.WARNING)


class OlxScraper:
    """Minimal scraper to test JS execution"""

    BROWSER_LOCALE = "pt-br"

    def __init__(self):
        """Initialize scraper instance."""
        self.sb = None
        logger.info("Scraper instance created")

    def init_browser(self):
        """Initialize browser."""
        try:
            self.sb = sb_cdp.Chrome(
                uc=True,
                headless=False,
                locale=self.BROWSER_LOCALE,
                window_size="1920,1080",
                ad_block=True,
            )
            logger.info("✓ Browser initialized")
            return True
        except Exception as e:
            logger.error(f"Browser init failed: {e}")
            raise

    def close_browser(self):
        """Close browser."""
        try:
            if self.sb:
                self.sb.driver.stop()
                logger.info("✓ Browser closed")
        except Exception as e:
            logger.error(f"Error closing browser: {e}")

    def wait_for_base_load(self, timeout: int = 10) -> bool:
        """Wait for body element to exist."""
        try:
            self.sb.wait_for_element("body", timeout=timeout)
            logger.info("✓ Body element loaded")
            return True
        except Exception as e:
            logger.error(f"Base load failed: {e}")
            return False

    def goto_url(self, url: str) -> bool:
        """Navigate to URL and wait for base load."""
        try:
            logger.info(f"Navigating to: {url}")
            self.sb.get(url)

            # Wait for base load
            if not self.wait_for_base_load(timeout=10):
                return False

            logger.info("✓ Page loaded")
            return True

        except Exception as e:
            logger.error(f"Navigation failed: {e}")
            return False

    def get_title_via_js(self) -> str:
        """Get page title using JS execution."""
        try:
            # Execute JS to get title
            title = self.sb.evaluate("return document.title")
            logger.info(f"✓ Title retrieved via JS: '{title}'")
            return title
        except Exception as e:
            logger.error(f"JS execution failed: {e}")
            return ""


def main():
    """Minimal test: navigate, get title via JS, print, close."""

    print("\n" + "="*60)
    print("MINIMAL JS TEST")
    print("="*60 + "\n")

    scraper = None

    try:
        # Initialize
        scraper = OlxScraper()
        scraper.init_browser()

        # Navigate
        test_url = "https://www.olx.com.br/imoveis/venda/casas/estado-df/distrito-federal-e-regiao"

        if scraper.goto_url(test_url):
            # Get title via JS
            title = scraper.get_title_via_js()

            # Print result
            print("\n" + "="*60)
            print("RESULT:")
            print(f"Title: {title}")
            print("="*60 + "\n")

            # Keep browser open to verify
            logger.info("Keeping browser open for 5 seconds to verify...")
            time.sleep(5)
        else:
            logger.error("Failed to load page")

    except Exception as e:
        logger.error(f"Test failed: {e}")

    finally:
        if scraper:
            scraper.close_browser()

    print("\n" + "="*60)
    print("TEST COMPLETE")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
