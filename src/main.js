import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';

// ----------------- Constants -----------------

const JOBS_PER_PAGE = 20;
const BASE_URL = 'https://www.rozee.pk';
const SEARCH_API = 'https://www.rozee.pk/services/job/jobSearch';

// User-Agent rotation pool for stealth
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
];

const getRandomUserAgent = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
const randomDelay = (min = 300, max = 800) => Math.floor(Math.random() * (max - min)) + min;

// ----------------- Helpers -----------------

const toAbs = (href, base = BASE_URL) => {
    try {
        return new URL(href, base).href;
    } catch {
        return null;
    }
};

const cleanText = (text) => {
    if (!text) return '';
    return text
        .replace(/\s+/g, ' ')
        .replace(/\u00A0/g, ' ')
        .trim();
};

const parseNumber = (value, fallback) => {
    const num = Number(value);
    return Number.isFinite(num) && num > 0 ? num : fallback;
};

const extractJobIdFromUrl = (url) => {
    try {
        const match = url.match(/-jobs-(\d+)/i) || url.match(/(\d+)(?:\/)?$/);
        return match ? match[1] : null;
    } catch {
        return null;
    }
};

const validateJobItem = (item) => {
    if (!item) return false;
    if (!item.title || !item.url) return false;
    return true;
};

// Build search URL with correct pagination pattern: /fpn/{offset}
const buildSearchUrl = (keyword, page = 1) => {
    const kw = keyword?.trim() || 'all';
    const encodedKw = encodeURIComponent(kw);
    if (page === 1) {
        return `${BASE_URL}/job/jsearch/q/${encodedKw}/fc/1`;
    }
    const offset = (page - 1) * JOBS_PER_PAGE;
    return `${BASE_URL}/job/jsearch/q/${encodedKw}/fpn/${offset}`;
};

// Convert description HTML to pretty text
const htmlToText = (html) => {
    if (!html) return '';
    let text = html;
    text = text.replace(/<\s*br\s*\/?>/gi, '\n');
    text = text.replace(/<\/\s*(p|div|li|h[1-6])\s*>/gi, '\n');
    text = text.replace(/<[^>]+>/g, ' ');
    text = text.replace(/\r/g, '');
    text = text.replace(/\n\s*\n+/g, '\n\n');
    return cleanText(text);
};

// Normalize location
const normalizeLocation = (loc) => {
    if (!loc) return '';
    const parts = loc
        .split(',')
        .map((p) => cleanText(p))
        .filter(Boolean)
        .filter((p) => !p.includes('[object') && !/^\d+$/.test(p));

    const unique = [];
    for (const p of parts) {
        const lower = p.toLowerCase();
        if (!unique.some((u) => u.toLowerCase() === lower)) {
            unique.push(p);
        }
    }
    return unique.slice(0, 3).join(', ');
};

// Parse JSON-LD JobPosting from HTML
const parseJsonLd = (html) => {
    const result = {};
    try {
        const regex = /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
        let match;
        while ((match = regex.exec(html)) !== null) {
            try {
                const json = JSON.parse(match[1].trim());
                const items = Array.isArray(json) ? json : [json];
                for (const item of items) {
                    if (item['@type'] === 'JobPosting') {
                        result.title = result.title || item.title || null;

                        if (item.hiringOrganization) {
                            const org = item.hiringOrganization;
                            result.company = result.company || (typeof org === 'string' ? org : org.name) || null;
                        }

                        if (item.jobLocation && !result.location) {
                            const jobLoc = Array.isArray(item.jobLocation) ? item.jobLocation[0] : item.jobLocation;
                            const addr = jobLoc?.address;
                            if (addr) {
                                const parts = [
                                    addr.addressLocality,
                                    addr.addressRegion,
                                    addr.addressCountry,
                                ].filter(Boolean);
                                result.location = parts.join(', ');
                            }
                        }

                        if (item.baseSalary && !result.salary) {
                            const val = item.baseSalary.value;
                            if (val && typeof val === 'object') {
                                const { minValue, maxValue, value, currency } = val;
                                const range = minValue && maxValue ? `${minValue}–${maxValue}` : value ?? minValue ?? maxValue;
                                result.salary = `${currency || ''} ${range || ''}`.trim();
                            } else if (val) {
                                result.salary = String(val);
                            }
                        }

                        result.contract_type = result.contract_type || item.employmentType || null;
                        result.description_html = result.description_html || item.description || null;
                        result.date_posted = result.date_posted || item.datePosted || null;
                        result.valid_through = result.valid_through || item.validThrough || null;
                    }
                }
            } catch {
                // Skip invalid JSON
            }
        }
    } catch {
        // Ignore JSON-LD parse errors
    }
    return result;
};

// ----------------- MAIN -----------------

Actor.main(async () => {
    log.setLevel(log.LEVELS.INFO);

    const input = (await Actor.getInput()) || {};

    const {
        keyword = '',
        location: locationFilter = '',
        results_wanted: RESULTS_WANTED_RAW = 100,
        max_pages: MAX_PAGES_RAW = 50,
        startUrl,
        startUrls,
        url,
        proxyConfiguration,
    } = input;

    const RESULTS_WANTED = parseNumber(RESULTS_WANTED_RAW, 100);
    const MAX_PAGES = parseNumber(MAX_PAGES_RAW, 50);

    const requestQueue = await Actor.openRequestQueue();

    // Build start requests
    const startRequests = [];

    if (Array.isArray(startUrls) && startUrls.length > 0) {
        for (const req of startUrls) {
            if (!req?.url) continue;
            startRequests.push({
                url: req.url,
                userData: { label: 'LIST', pageNo: 1 },
            });
        }
    } else if (typeof startUrl === 'string' && startUrl.trim()) {
        startRequests.push({
            url: startUrl.trim(),
            userData: { label: 'LIST', pageNo: 1 },
        });
    } else if (typeof url === 'string' && url.trim()) {
        startRequests.push({
            url: url.trim(),
            userData: { label: 'LIST', pageNo: 1 },
        });
    } else {
        const searchUrl = buildSearchUrl(keyword, 1);
        startRequests.push({
            url: searchUrl,
            userData: { label: 'LIST', pageNo: 1, keyword },
        });
    }

    for (const req of startRequests) {
        await requestQueue.addRequest(req);
    }

    // Proxy configuration
    const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

    // Tracking
    let saved = 0;
    let detailEnqueued = 0;
    const seenJobIds = new Set();
    const savedBatch = [];
    const BATCH_SIZE = 10;

    log.info(`🚀 RozeePk Scraper starting | target=${RESULTS_WANTED}, maxPages=${MAX_PAGES}`);

    const crawler = new PlaywrightCrawler({
        requestQueue,
        proxyConfiguration: proxyConfig,

        launchContext: {
            launchOptions: {
                headless: true,
                args: [
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-infobars',
                    '--disable-background-networking',
                    '--disable-default-apps',
                    '--disable-extensions',
                    '--disable-sync',
                    '--disable-translate',
                    '--metrics-recording-only',
                    '--no-first-run',
                ],
            },
        },

        useSessionPool: true,
        persistCookiesPerSession: true,
        sessionPoolOptions: {
            maxPoolSize: 10,
            sessionOptions: {
                maxUsageCount: 50,
                maxAgeSecs: 3600,
            },
        },

        maxConcurrency: 6,
        minConcurrency: 2,
        maxRequestRetries: 3,
        navigationTimeoutSecs: 30,
        requestHandlerTimeoutSecs: 60,

        preNavigationHooks: [
            async ({ page }, gotoOptions) => {
                // Set random User-Agent for stealth
                const ua = getRandomUserAgent();
                await page.setExtraHTTPHeaders({
                    'User-Agent': ua,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                });

                // Random viewport
                await page.setViewportSize({
                    width: 1280 + Math.floor(Math.random() * 200),
                    height: 720 + Math.floor(Math.random() * 200),
                });

                // Block unnecessary resources for speed
                await page.route('**/*', (route) => {
                    const type = route.request().resourceType();
                    if (['image', 'media', 'font', 'stylesheet'].includes(type)) {
                        return route.abort();
                    }
                    return route.continue();
                });

                gotoOptions.waitUntil = 'domcontentloaded';
            },
        ],

        requestHandler: async ({ page, request, log: crawlerLog }) => {
            const label = request.userData.label || 'LIST';

            // Early exit if target reached
            if (saved >= RESULTS_WANTED) {
                crawlerLog.debug('Target reached, skipping request.');
                return;
            }

            // Random delay for stealth
            await page.waitForTimeout(randomDelay(300, 700));

            // ----- LIST PAGES -----
            if (label === 'LIST') {
                const pageNo = request.userData.pageNo || 1;
                const kw = request.userData.keyword || keyword || '';

                try {
                    await page.waitForLoadState('domcontentloaded');
                    await page.waitForTimeout(randomDelay(400, 800));
                } catch {
                    crawlerLog.warning(`LIST page load timeout: ${request.url}`);
                }

                // Check for blocking
                const isBlocked = await page.evaluate(() => {
                    const bodyText = (document.body?.innerText || '').toLowerCase();
                    return (
                        document.title.toLowerCase().includes('forbidden') ||
                        bodyText.startsWith('403') ||
                        bodyText.includes('access denied') ||
                        bodyText.includes('blocked')
                    );
                });

                if (isBlocked) {
                    crawlerLog.warning(`🚫 Page blocked: ${request.url}`);
                    throw new Error('Page blocked - will retry');
                }

                // Extract job URLs from listing page
                let jobUrls = [];
                try {
                    jobUrls = await page.$$eval('a[href*="-jobs-"]', (anchors) => {
                        const urls = new Set();
                        for (const a of anchors) {
                            const href = a.getAttribute('href') || '';
                            if (/-jobs-\d+/i.test(href)) {
                                urls.add(href);
                            }
                        }
                        return Array.from(urls);
                    });
                } catch (err) {
                    crawlerLog.warning(`Failed to extract job links: ${err.message}`);
                }

                // Enqueue detail pages
                let newDetails = 0;
                for (const href of jobUrls) {
                    if (saved + detailEnqueued >= RESULTS_WANTED) break;

                    const absUrl = toAbs(href, request.url);
                    if (!absUrl) continue;

                    const jobId = extractJobIdFromUrl(absUrl);
                    if (!jobId || seenJobIds.has(jobId)) continue;

                    seenJobIds.add(jobId);

                    await requestQueue.addRequest({
                        url: absUrl,
                        userData: { label: 'DETAIL', jobId },
                        uniqueKey: `detail-${jobId}`,
                    });

                    detailEnqueued++;
                    newDetails++;
                }

                crawlerLog.info(`📄 LIST #${pageNo} | found=${jobUrls.length}, enqueued=${newDetails}, saved=${saved}`);

                // Pagination - use correct /fpn/{offset} pattern
                if (pageNo < MAX_PAGES && saved + detailEnqueued < RESULTS_WANTED && jobUrls.length > 0) {
                    const nextPageNo = pageNo + 1;
                    const nextUrl = buildSearchUrl(kw, nextPageNo);

                    await requestQueue.addRequest({
                        url: nextUrl,
                        userData: { label: 'LIST', pageNo: nextPageNo, keyword: kw },
                        uniqueKey: `list-${nextPageNo}`,
                    });
                }

                return;
            }

            // ----- DETAIL PAGES -----
            if (label === 'DETAIL') {
                if (saved >= RESULTS_WANTED) return;

                try {
                    await page.waitForLoadState('domcontentloaded');
                    await page.waitForTimeout(randomDelay(200, 500));
                } catch {
                    crawlerLog.warning(`DETAIL page load timeout: ${request.url}`);
                }

                // Get page HTML for JSON-LD parsing
                const html = await page.content();

                // Try JSON-LD first (most reliable)
                const jsonLdData = parseJsonLd(html);

                // Fallback to HTML parsing
                let htmlData = {};
                try {
                    htmlData = await page.evaluate(() => {
                        const getText = (sel) => {
                            const el = document.querySelector(sel);
                            return el ? el.textContent.trim() : null;
                        };

                        const getHtml = (sel) => {
                            const el = document.querySelector(sel);
                            return el ? el.innerHTML.trim() : null;
                        };

                        return {
                            title: getText('h1') || getText('h2') || getText('.job-title'),
                            company: getText('.company-name') || getText('.cp-name') || getText('[itemprop="hiringOrganization"]'),
                            location: getText('.location') || getText('.job-location') || getText('[itemprop="jobLocation"]'),
                            description_html: getHtml('.job-description') || getHtml('#job-description') || getHtml('[itemprop="description"]'),
                            salary: getText('.salary') || getText('.job-salary'),
                            contract_type: getText('.job-type') || getText('.employment-type'),
                        };
                    });
                } catch (err) {
                    crawlerLog.warning(`HTML parse error: ${err.message}`);
                }

                // Merge data (JSON-LD takes priority)
                const merged = {
                    title: jsonLdData.title || htmlData.title,
                    company: jsonLdData.company || htmlData.company,
                    location: jsonLdData.location || htmlData.location,
                    salary: jsonLdData.salary || htmlData.salary,
                    contract_type: jsonLdData.contract_type || htmlData.contract_type,
                    description_html: jsonLdData.description_html || htmlData.description_html,
                    date_posted: jsonLdData.date_posted,
                    valid_through: jsonLdData.valid_through,
                };

                const job = {
                    source: 'rozee.pk',
                    job_id: request.userData.jobId || extractJobIdFromUrl(request.url),
                    url: request.url,
                    title: cleanText(merged.title),
                    company: cleanText(merged.company),
                    location: normalizeLocation(merged.location),
                    salary: cleanText(merged.salary) || null,
                    contract_type: cleanText(merged.contract_type) || null,
                    description_html: merged.description_html || null,
                    description_text: htmlToText(merged.description_html || ''),
                    date_posted: merged.date_posted || null,
                    valid_through: merged.valid_through || null,
                    scraped_at: new Date().toISOString(),
                };

                if (!validateJobItem(job)) {
                    crawlerLog.debug(`Skipping invalid job: ${request.url}`);
                    return;
                }

                // Batch save for efficiency
                savedBatch.push(job);
                saved++;

                if (savedBatch.length >= BATCH_SIZE || saved >= RESULTS_WANTED) {
                    await Dataset.pushData(savedBatch);
                    crawlerLog.info(`💾 Saved ${saved} jobs so far`);
                    savedBatch.length = 0;
                }

                return;
            }

            crawlerLog.warning(`Unknown label: ${label}`);
        },

        failedRequestHandler: async ({ request, error, log: crawlerLog }) => {
            crawlerLog.error(`❌ Request failed: ${request.url} - ${error.message}`);
        },
    });

    await crawler.run();

    // Save any remaining batch
    if (savedBatch.length > 0) {
        await Dataset.pushData(savedBatch);
    }

    log.info(`✅ Scraping complete. Total jobs saved: ${saved} (target: ${RESULTS_WANTED})`);

    if (saved === 0) {
        log.warning('⚠️ No jobs were scraped. The site may be blocking requests or has changed structure.');
    } else {
        log.info(`🎉 Successfully scraped ${saved} jobs from Rozee.pk`);
    }
});
