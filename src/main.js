import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';

// ----------------- Constants -----------------

const BASE_URL = 'https://www.rozee.pk';
const SITEMAP_URL = 'https://www.rozee.pk/sitemap/jobs.xml';
const JOBS_PER_PAGE = 20;

// User-Agent rotation pool
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
];

const getRandomUserAgent = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
const randomDelay = (min = 200, max = 600) => Math.floor(Math.random() * (max - min)) + min;

// ----------------- Helpers -----------------

const cleanText = (text) => {
    if (!text) return '';
    return text.replace(/\s+/g, ' ').replace(/\u00A0/g, ' ').trim();
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

// Build search URL with correct pagination pattern
const buildSearchUrl = (keyword, page = 1) => {
    const kw = keyword?.trim() || 'all';
    const encodedKw = encodeURIComponent(kw);
    if (page === 1) {
        return `${BASE_URL}/job/jsearch/q/${encodedKw}/fc/1`;
    }
    const offset = (page - 1) * JOBS_PER_PAGE;
    return `${BASE_URL}/job/jsearch/q/${encodedKw}/fpn/${offset}`;
};

const htmlToText = (html) => {
    if (!html) return '';
    let text = html;
    text = text.replace(/<\s*br\s*\/?>/gi, '\n');
    text = text.replace(/<\/\s*(p|div|li|h[1-6])\s*>/gi, '\n');
    text = text.replace(/<[^>]+>/g, ' ');
    text = text.replace(/\n\s*\n+/g, '\n\n');
    return cleanText(text);
};

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

// Filter job URL by keyword
const matchesKeyword = (url, keyword) => {
    if (!keyword || !keyword.trim()) return true;
    const urlLower = url.toLowerCase();
    const keywords = keyword.toLowerCase().split(/\s+/);
    return keywords.some((kw) => urlLower.includes(kw));
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
    const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

    // Tracking
    let saved = 0;
    const seenJobIds = new Set();

    log.info(`🚀 RozeePk Scraper starting | target=${RESULTS_WANTED}`);

    // Strategy: Use sitemap for broad searches, direct search for specific keywords
    const useStartUrl = startUrl || url || (Array.isArray(startUrls) && startUrls[0]?.url);

    if (useStartUrl) {
        // Direct URL provided - use LIST mode
        await requestQueue.addRequest({
            url: typeof useStartUrl === 'string' ? useStartUrl : useStartUrl,
            userData: { label: 'LIST', pageNo: 1 },
        });
    } else if (keyword && keyword.trim()) {
        // Keyword search - start with search page
        const searchUrl = buildSearchUrl(keyword, 1);
        await requestQueue.addRequest({
            url: searchUrl,
            userData: { label: 'LIST', pageNo: 1, keyword },
        });
    } else {
        // No keyword - use sitemap for all jobs
        await requestQueue.addRequest({
            url: SITEMAP_URL,
            userData: { label: 'SITEMAP' },
        });
    }

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
                    '--disable-background-networking',
                    '--disable-default-apps',
                    '--disable-extensions',
                    '--disable-sync',
                    '--metrics-recording-only',
                    '--no-first-run',
                ],
            },
        },

        useSessionPool: true,
        persistCookiesPerSession: true,
        sessionPoolOptions: {
            maxPoolSize: 5,
            sessionOptions: { maxUsageCount: 30 },
        },

        maxConcurrency: 5,
        minConcurrency: 1,
        maxRequestRetries: 2,
        navigationTimeoutSecs: 25,
        requestHandlerTimeoutSecs: 45,

        preNavigationHooks: [
            async ({ page }, gotoOptions) => {
                const ua = getRandomUserAgent();
                await page.setExtraHTTPHeaders({
                    'User-Agent': ua,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                });

                await page.setViewportSize({
                    width: 1280 + Math.floor(Math.random() * 200),
                    height: 720 + Math.floor(Math.random() * 200),
                });

                // Block heavy resources
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

            if (saved >= RESULTS_WANTED) {
                crawlerLog.debug('Target reached, skipping.');
                return;
            }

            await page.waitForTimeout(randomDelay(200, 500));

            // ----- SITEMAP -----
            if (label === 'SITEMAP') {
                crawlerLog.info('📥 Fetching sitemap...');

                const content = await page.content();
                const urlMatches = content.match(/<loc>(https:\/\/www\.rozee\.pk\/[^<]+jobs-\d+)<\/loc>/gi) || [];

                const jobUrls = urlMatches
                    .map((m) => m.replace(/<\/?loc>/gi, ''))
                    .filter((url) => matchesKeyword(url, keyword));

                crawlerLog.info(`📄 Sitemap: found ${jobUrls.length} job URLs matching "${keyword || 'all'}"`);

                let enqueued = 0;
                for (const jobUrl of jobUrls) {
                    if (saved + enqueued >= RESULTS_WANTED) break;

                    const jobId = extractJobIdFromUrl(jobUrl);
                    if (!jobId || seenJobIds.has(jobId)) continue;

                    seenJobIds.add(jobId);

                    await requestQueue.addRequest({
                        url: jobUrl,
                        userData: { label: 'DETAIL', jobId },
                        uniqueKey: `detail-${jobId}`,
                    });
                    enqueued++;
                }

                crawlerLog.info(`📄 Enqueued ${enqueued} detail pages from sitemap`);
                return;
            }

            // ----- LIST PAGES -----
            if (label === 'LIST') {
                const pageNo = request.userData.pageNo || 1;
                const kw = request.userData.keyword || keyword || '';

                await page.waitForLoadState('domcontentloaded');

                // Check for blocking
                const isBlocked = await page.evaluate(() => {
                    const text = document.body?.innerText?.toLowerCase() || '';
                    return text.includes('forbidden') || text.includes('access denied') || text.includes('blocked');
                });

                if (isBlocked) {
                    crawlerLog.warning(`🚫 Blocked: ${request.url}`);
                    throw new Error('Blocked - will retry');
                }

                // Extract job URLs
                const jobUrls = await page.$$eval('a[href*="-jobs-"]', (anchors) => {
                    const urls = new Set();
                    for (const a of anchors) {
                        const href = a.getAttribute('href') || '';
                        if (/-jobs-\d+/i.test(href)) {
                            try {
                                urls.add(new URL(href, window.location.origin).href);
                            } catch { }
                        }
                    }
                    return Array.from(urls);
                });

                let enqueued = 0;
                for (const jobUrl of jobUrls) {
                    if (saved + enqueued >= RESULTS_WANTED) break;

                    const jobId = extractJobIdFromUrl(jobUrl);
                    if (!jobId || seenJobIds.has(jobId)) continue;

                    seenJobIds.add(jobId);

                    await requestQueue.addRequest({
                        url: jobUrl,
                        userData: { label: 'DETAIL', jobId },
                    });
                    enqueued++;
                }

                crawlerLog.info(`📄 LIST #${pageNo} | found=${jobUrls.length}, enqueued=${enqueued}, saved=${saved}`);

                // Pagination
                if (pageNo < MAX_PAGES && saved + enqueued < RESULTS_WANTED && jobUrls.length > 0) {
                    const nextUrl = buildSearchUrl(kw, pageNo + 1);
                    await requestQueue.addRequest({
                        url: nextUrl,
                        userData: { label: 'LIST', pageNo: pageNo + 1, keyword: kw },
                        uniqueKey: `list-${pageNo + 1}`,
                    });
                }

                return;
            }

            // ----- DETAIL PAGES -----
            if (label === 'DETAIL') {
                if (saved >= RESULTS_WANTED) return;

                await page.waitForLoadState('domcontentloaded');

                // Extract JSON-LD first (most reliable)
                const jobData = await page.evaluate(() => {
                    const result = {};

                    // JSON-LD extraction
                    try {
                        const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                        for (const script of scripts) {
                            try {
                                const json = JSON.parse(script.textContent);
                                const items = Array.isArray(json) ? json : [json];
                                for (const item of items) {
                                    if (item['@type'] === 'JobPosting') {
                                        result.title = item.title || null;

                                        if (item.hiringOrganization) {
                                            const org = item.hiringOrganization;
                                            result.company = typeof org === 'string' ? org : org.name || null;
                                        }

                                        if (item.jobLocation) {
                                            const loc = Array.isArray(item.jobLocation) ? item.jobLocation[0] : item.jobLocation;
                                            const addr = loc?.address;
                                            if (addr) {
                                                result.location = [addr.addressLocality, addr.addressRegion, addr.addressCountry].filter(Boolean).join(', ');
                                            }
                                        }

                                        if (item.baseSalary?.value) {
                                            const val = item.baseSalary.value;
                                            if (typeof val === 'object') {
                                                const { minValue, maxValue, currency } = val;
                                                result.salary = `${currency || ''} ${minValue && maxValue ? `${minValue}–${maxValue}` : minValue || maxValue || ''}`.trim();
                                            } else {
                                                result.salary = String(val);
                                            }
                                        }

                                        result.contract_type = item.employmentType || null;
                                        result.description = item.description || null;
                                        result.date_posted = item.datePosted || null;
                                        result.valid_through = item.validThrough || null;
                                    }
                                }
                            } catch { }
                        }
                    } catch { }

                    // HTML fallback
                    if (!result.title) {
                        result.title = document.querySelector('h1')?.textContent?.trim() || null;
                    }
                    if (!result.company) {
                        result.company = document.querySelector('.company-name, .cp-name')?.textContent?.trim() || null;
                    }
                    if (!result.location) {
                        result.location = document.querySelector('.location, .job-location')?.textContent?.trim() || null;
                    }
                    if (!result.description) {
                        result.description = document.querySelector('.job-description, #job-description')?.innerHTML || null;
                    }

                    return result;
                });

                if (!jobData.title) {
                    crawlerLog.debug(`No title found: ${request.url}`);
                    return;
                }

                const job = {
                    source: 'rozee.pk',
                    job_id: request.userData.jobId || extractJobIdFromUrl(request.url),
                    url: request.url,
                    title: cleanText(jobData.title),
                    company: cleanText(jobData.company) || null,
                    location: normalizeLocation(jobData.location),
                    salary: cleanText(jobData.salary) || null,
                    contract_type: cleanText(jobData.contract_type) || null,
                    description_html: jobData.description || null,
                    description_text: htmlToText(jobData.description || ''),
                    date_posted: jobData.date_posted || null,
                    valid_through: jobData.valid_through || null,
                    scraped_at: new Date().toISOString(),
                };

                await Dataset.pushData(job);
                saved++;

                if (saved % 10 === 0) {
                    crawlerLog.info(`💾 Saved ${saved} jobs`);
                }
            }
        },

        failedRequestHandler: async ({ request, error, log: crawlerLog }) => {
            crawlerLog.error(`❌ Failed: ${request.url} - ${error.message}`);
        },
    });

    await crawler.run();

    log.info(`✅ Complete. Total: ${saved} jobs`);

    if (saved === 0) {
        log.warning('⚠️ No jobs scraped.');
    } else {
        log.info(`🎉 Successfully scraped ${saved} jobs`);
    }
});
