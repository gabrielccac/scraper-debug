import time
import re
import logging
import os
from bs4 import BeautifulSoup
from seleniumbase import sb_cdp

logger = logging.getLogger(__name__)


class OlxScraper:
    """
    Scraper for olx.com.br property listings.
    """
    
    # ========================================================================
    # SITE-SPECIFIC CONFIGURATION
    # ========================================================================
    
    BASE_URL = "https://www.olx.com.br/"
    SITE_NAME = "olx"
    
    # Selectors (unchanged)
    PAGE_LOADED_SELECTOR = 'div.AdListing_adListContainer__ALQla'
    LISTING_CARD_SELECTOR = 'section.olx-adcard'
    NEXT_PAGE_BUTTON = '//a[text()="Próxima página"]'
    NO_RESULTS_MESSAGE = 'Ops! Nenhum anúncio foi encontrado.'
    
    # Captcha detection patterns (unchanged)
    CAPTCHA_TITLE_KEYWORDS = ["Um momento", "Just a moment"]
    CAPTCHA_TEXT_KEYWORDS = ["Confirme que você é humano", "cf-challenge"]
    
    # Settings (unchanged)
    LOAD_TIMEOUT = 15
    BROWSER_LOCALE = "pt-br"

    # ========================================================================
    # INITIALIZATION - MODIFIED
    # ========================================================================

    def __init__(self):
        """
        Initialize scraper with new Redis clients.
        """
        self.sb = None

        # Screenshot directory (environment variable for Docker compatibility)
        self.FAILED_DIR = os.getenv('FAILED_SCREENSHOTS_DIR', 'failed_page_loads')

        # Ensure failed directory exists
        os.makedirs(self.FAILED_DIR, exist_ok=True)

        logger.debug("Scraper initialized")

    # ========================================================================
    # BROWSER MANAGEMENT
    # ========================================================================

    def init_browser(self, headless: bool = False, max_retries: int = 3):
        """
        Initialize SeleniumBase browser instance with retry logic.

        Args:
            headless: Run in headless mode
            max_retries: Maximum number of retry attempts (default 3)
        """
        for attempt in range(max_retries):
            try:
                self.sb = sb_cdp.Chrome(
                    uc=True,
                    headless=headless,
                    locale=self.BROWSER_LOCALE,
                    window_size="1920,1080",
                    ad_block=True,
                )
                logger.debug(f"Browser initialized successfully (attempt {attempt + 1}/{max_retries})")
                return True

            except Exception as e:
                logger.warning(f"Browser init attempt {attempt + 1}/{max_retries} failed: {str(e)[:100]}")

                # Cleanup failed browser instance before retry
                try:
                    if self.sb:
                        self.sb.driver.quit()
                        self.sb = None
                except:
                    pass

                if attempt < max_retries - 1:
                    # Exponential backoff: 1s, 2s, 4s
                    wait_time = 1.5 ** attempt
                    logger.info(f"Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to initialize browser after {max_retries} attempts")
                    raise

        return False
    
    def navigate(self, url: str) -> bool:
        """
        Navigate to a URL with captcha detection.

        Workflow:
        1. Navigate to URL
        2. Check for captcha FIRST
        3. Handle captcha if present
        4. Then verify page loaded

        Args:
            url: URL to navigate to

        Returns:
            True if navigation successful (page loaded without captcha or captcha solved)
        """
        try:
            logger.debug(f"Navigating to: {url}")
            self.sb.open(url)

            # Not load or is captcha page or is no results page

            # Check for captcha BEFORE verifying page load
            if self.is_captcha_page():
                logger.debug("Captcha detected after navigation")
                if not self.handle_captcha():
                    logger.error("Failed to handle captcha during navigation")
                    return False

            # Now verify the actual page loaded
            return self.verify_page_loaded()

        except Exception as e:
            logger.error(f"Navigation failed: {str(e)[:100]}")
            return False
    
    def close_browser(self):
        """Safely close browser instance."""
        try:
            if self.sb:
                self.sb.driver.stop()
                logger.debug("Browser closed")
        except Exception as e:
            logger.debug(f"Error closing browser: {str(e)[:100]}")

    def restart_browser(self) -> bool:
        """
        Close and reinitialize browser (full restart).

        Returns:
            True if restart successful
        """
        try:
            logger.info("Restarting browser...")

            # Close existing browser
            self.close_browser()
            time.sleep(1)

            # Reinitialize browser
            self.init_browser()

            logger.info("Browser restarted successfully")
            return True

        except Exception as e:
            logger.error(f"Browser restart failed: {str(e)[:100]}")
            return False
    
    def verify_page_loaded(self) -> bool:
        """
        Verify that the listings page has loaded successfully.

        Returns:
            True if page loaded (listings container visible)
        """
        try:
            self.sb.wait_for_element(
                self.PAGE_LOADED_SELECTOR,
                timeout=self.LOAD_TIMEOUT
            )
            logger.debug("Page loaded successfully")
            return True

        except Exception as e:
            logger.warning(f"Page load verification failed: {str(e)[:100]}")

            # Debug: Log current page state for troubleshooting
            try:
                current_title = self.sb.get_title()
                current_url = self.sb.get_current_url()
                logger.info(f"Debug - Page title: '{current_title}' | URL: {current_url[:80]}")
            except Exception as debug_e:
                logger.debug(f"Could not get page info for debugging: {str(debug_e)[:100]}")

            # Check if it's a captcha page before giving up
            is_captcha = self.is_captcha_page()
            logger.info(f"Captcha detection result: {is_captcha}")

            if is_captcha:
                logger.warning("⚠️  Captcha detected during page load verification")
                if self.handle_captcha():
                    # Captcha solved, try verifying page load again
                    try:
                        self.sb.wait_for_element_visible(
                            self.PAGE_LOADED_SELECTOR,
                            timeout=self.LOAD_TIMEOUT
                        )
                        logger.info("✅ Page loaded after captcha resolution")
                        return True
                    except:
                        logger.error("Page still not loaded after captcha resolution")
                        return False
                else:
                    logger.error("Failed to handle captcha during page load verification")
                    return False

            # Not a captcha, genuine page load failure - save screenshot
            try:
                timestamp = time.time_ns()
                current_url = self.sb.get_current_url()
                url_hash = current_url.split('/')[-1][:50] if current_url else "unknown"
                screenshot_path = os.path.join(
                    self.FAILED_DIR,
                    f"page_load_failed_{url_hash}_{timestamp}.png"
                )
                self.sb.save_screenshot(screenshot_path)
                logger.info(f"📸 Screenshot saved: {screenshot_path}")
            except Exception as screenshot_error:
                logger.debug(f"Could not save screenshot: {str(screenshot_error)[:100]}")

            return False

    
    def is_captcha_page(self) -> bool:
        """
        Detect if current page is a captcha screen.

        Returns:
            True if captcha detected
        """
        try:
            # Check page title
            title = self.sb.get_title()
            for keyword in self.CAPTCHA_TITLE_KEYWORDS:
                if keyword in title:
                    logger.debug(f"Captcha detected in title: '{title}'")
                    return True

            # Check page source
            page_source = self.sb.get_page_source()
            for keyword in self.CAPTCHA_TEXT_KEYWORDS:
                if keyword in page_source:
                    logger.debug(f"Captcha detected in page source: '{keyword}'")
                    return True

        except Exception as e:
            logger.debug(f"Error checking for captcha: {str(e)[:100]}")

        return False

    def is_no_results_page(self) -> bool:
        """
        Detect if current page shows "no results" message.

        This happens when:
        - Legitimately no listings match the filters
        - Server issues prevent reaching calculated max page

        Returns:
            True if no results message detected
        """
        try:
            # Check if no results container is present
            no_results_elements = self.sb.assert_text(self.NO_RESULTS_MESSAGE, timeout=self.LOAD_TIMEOUT)

            if no_results_elements:
                logger.debug("No results message detected on page")
                return True

        except Exception as e:
            logger.debug(f"Error checking for no results: {str(e)[:100]}")

        return False
    
    def handle_captcha(self, max_wait: int = 15) -> bool:
        """
        Handle captcha with multiple fallback methods.

        Strategy:
        1. Wait for UC mode auto-solve (60s)
        2. If fails, try gui_click_captcha()
        3. If fails, try solve_captcha()

        Args:
            max_wait: Maximum time to wait for UC mode (seconds, default 60)

        Returns:
            True if captcha cleared
        """
        logger.warning("⚠️  Captcha detected! Trying UC mode auto-solve...")

        # Method 1: UC mode auto-solve (passive waiting)
        start_time = time.time()
        while time.time() - start_time < max_wait:
            # Check if title changed (captcha cleared)
            try:
                title = self.sb.get_title()
                # Check if any captcha keyword is still in title
                captcha_present = any(keyword in title for keyword in self.CAPTCHA_TITLE_KEYWORDS)
                if not captcha_present:
                    logger.info("✅ Captcha cleared by UC mode!")
                    return True
            except:
                pass

            time.sleep(1)

        logger.warning("UC mode timeout - trying fallback methods...")

        # Method 2: gui_click_captcha()
        try:
            logger.info("Trying gui_click_captcha()...")
            self.sb.gui_click_captcha()
            time.sleep(3)  # Wait for captcha to process

            # Check if cleared
            title = self.sb.get_title()
            captcha_present = any(keyword in title for keyword in self.CAPTCHA_TITLE_KEYWORDS)
            if not captcha_present:
                logger.info("✅ Captcha cleared by gui_click_captcha()!")
                return True
        except Exception as e:
            logger.debug(f"gui_click_captcha() failed: {str(e)[:100]}")

        # Method 3: solve_captcha()
        try:
            logger.info("Trying solve_captcha()...")
            self.sb.solve_captcha()
            time.sleep(3)  # Wait for captcha to process

            # Check if cleared
            title = self.sb.get_title()
            captcha_present = any(keyword in title for keyword in self.CAPTCHA_TITLE_KEYWORDS)
            if not captcha_present:
                logger.info("✅ Captcha cleared by solve_captcha()!")
                return True
        except Exception as e:
            logger.debug(f"solve_captcha() failed: {str(e)[:100]}")

        logger.error("🚫 All captcha solving methods failed")
        return False
    
    def click_next_page(self) -> bool:
        """
        Click the "Next" pagination button.
        
        Returns:
            True if next page clicked successfully
        """
        try:
            if self.sb.assert_element(self.NEXT_PAGE_BUTTON, timeout=15):
                logger.debug("Clicking next page button")
                self.sb.click(self.NEXT_PAGE_BUTTON)
                return True
            else:
                logger.debug("Next button not visible")
                return False
                
        except Exception as e:
            logger.error(f"Error clicking next button: {str(e)[:100]}")
            return False
        
    def get_current_page_number(self) -> int:
        """
        Get current page number from OLX URL pattern.
        
        Examples:
            - https://www.olx.com.br/imoveis/venda/estado-df?lis=home_body_search_bar_1002&o=5
            - https://www.olx.com.br/imoveis/venda/estado-df?o=10
            - https://www.olx.com.br/imoveis/venda/estado-df (first page, no 'o' parameter)
        
        Returns:
            Page number (1-indexed), or 1 if not found
        """
        try:
            current_url = self.sb.get_current_url()
            
            # Pattern for OLX: ?o={number} or &o={number} in query parameters
            match = re.search(r'[?&]o=(\d+)', current_url)
            if match:
                return int(match.group(1))
            
            # If no page number found in URL, we're on first page
            # First page URLs don't have the 'o' parameter
            if '?o=' not in current_url and '&o=' not in current_url:
                return 1
                
        except Exception as e:
            logger.debug(f"Error extracting page number from URL: {str(e)[:100]}")
        
        return 0
    
    
    def get_page_url(self, task: dict, page_num: int) -> str:
        """
        Construct URL for a specific page number for OLX.

        Pattern: https://www.olx.com.br/imoveis/{transaction}/{prop_type}/estado-df{location}?o={page}

        Examples:
            - https://www.olx.com.br/imoveis/venda/apartamentos/estado-df?o=2
            - https://www.olx.com.br/imoveis/aluguel/casas/estado-df/distrito-federal-e-regiao/brasilia?o=5
            - https://www.olx.com.br/imoveis/venda/casas/estado-df/distrito-federal-e-regiao/outras-cidades/ra-x---guara?o=3
        """
        prop_type = task['prop_type']
        transaction_type = task['transaction_type']
        location = task.get('location', '')  # Location can be empty string for state-level

        # Build base URL following legacy pattern
        if location:
            base_url = f"https://www.olx.com.br/imoveis/{transaction_type}/{prop_type}/estado-df{location}"
        else:
            base_url = f"https://www.olx.com.br/imoveis/{transaction_type}/{prop_type}/estado-df"

        # Add page parameter if not first page
        if page_num > 1:
            base_url += f"?o={page_num}"

        return base_url
    
    def extract_page_data(self) -> list:
        """
        Extract listing URLs with prices from current OLX page.

        Returns:
            List of tuples: [(url, price), ...]
        """
        try:
            page_html = self.sb.get_page_source()
            soup = BeautifulSoup(page_html, 'lxml')

            urls_with_prices = []
            listing_elements = soup.select(self.LISTING_CARD_SELECTOR)

            for listing in listing_elements:
                # Extract URL from the link with data-testid="adcard-link"
                link_element = listing.select_one('a[data-testid="adcard-link"]')
                if not link_element:
                    continue

                href = link_element.get('href')
                if not href:
                    continue

                # Build full URL
                if href.startswith('http'):
                    full_url = href
                else:
                    full_url = self.BASE_URL.rstrip('/') + href

                # Clean URL: remove query parameters
                if '?' in full_url:
                    clean_url = full_url.split('?')[0]
                else:
                    clean_url = full_url

                # Extract price
                price = None

                # OLX price selector: h3 with class olx-adcard__price
                price_h3 = listing.select_one('h3.olx-adcard__price')
                if price_h3:
                    price_text = price_h3.get_text(strip=True)
                    # Parse: "R$ 900" -> 900
                    # Remove "R$", dots (thousands), spaces and convert to int
                    price_clean = price_text.replace('R$', '').replace('.', '').replace(' ', '').strip()
                    try:
                        price = int(price_clean)
                    except (ValueError, AttributeError):
                        logger.debug(f"Could not parse price: {price_text}")
                        pass

                # Add tuple
                urls_with_prices.append((clean_url, price))

            logger.debug(f"Extracted {len(urls_with_prices)} URLs from page")
            return urls_with_prices

        except Exception as e:
            logger.error(f"Error extracting URLs: {str(e)[:100]}")
            return []
    
    # ========================================================================
    # TOTAL PAGES DETECTION
    # ========================================================================
    
    def detect_total_pages(self) -> int:
        """
        Detect total number of pages from current page (reuses existing browser).

        Reads total results count from OLX's results paragraph.

        Returns:
            Total pages (int), or 0 if detection fails
        """
        MAX_PAGES = 100  # Website blocks beyond this
        LISTINGS_PER_PAGE = 50

        logger.info(f"🔍 Detecting total pages from current page...")

        try:
            # Get page source and parse with BeautifulSoup for more reliable extraction
            page_html = self.sb.get_page_source()
            soup = BeautifulSoup(page_html, 'lxml')

            # Try to find results text element using multiple strategies
            results_text_element = None

            # Strategy 1: Look for data-testid attribute
            results_text_element = soup.select_one('span[data-testid="results-count-text"]')

            # Strategy 2: Find span/p containing "resultados" pattern
            if not results_text_element:
                results_text_element = soup.find(['span', 'p'], string=re.compile(r'de\s+[\d.]+\s+resultados?'))

            if results_text_element:
                results_text = results_text_element.get_text()
                logger.debug(f"Found results text: {results_text}")

                # Extract the total number using regex
                # Pattern matches: "1 - 50 de 4.086 resultados" -> 4.086
                match = re.search(r'de\s+([\d\.]+)\s+resultados?', results_text)

                if match:
                    total_listings_str = match.group(1).replace('.', '').replace(',', '')
                    total_listings = int(total_listings_str)

                    # Calculate pages (ceiling division)
                    expected_pages = (total_listings + LISTINGS_PER_PAGE - 1) // LISTINGS_PER_PAGE

                    # Apply max pages limit
                    total_pages = min(expected_pages, MAX_PAGES)

                    logger.info(f"✅ Detected {total_pages} pages ({total_listings} listings)")
                    return total_pages
                else:
                    logger.warning(f"Could not extract number from results text: {results_text}")
                    return 0
            else:
                logger.warning("Could not find results text element")
                return 0

        except Exception as e:
            logger.error(f"Error detecting pages: {str(e)[:200]}")
            return 0
        
        