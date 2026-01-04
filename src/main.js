import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import * as cheerio from 'cheerio';

// ----------------- Constants -----------------

const JOBS_PER_PAGE = 20;
const BASE_URL = 'https://www.rozee.pk';

// User-Agent rotation pool for stealth
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
];

const getRandomUserAgent = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

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

// Convert description HTML to plain text
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
const parseJsonLd = ($) => {
    const result = {};
    try {
        $('script[type="application/ld+json"]').each((_, el) => {
            try {
                const json = JSON.parse($(el).html());
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
                                const parts = [addr.addressLocality, addr.addressRegion, addr.addressCountry].filter(Boolean);
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
        });
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
    if (Array.isArray(startUrls) && startUrls.length > 0) {
        for (const req of startUrls) {
            if (!req?.url) continue;
            await requestQueue.addRequest({
                url: req.url,
                userData: { label: 'LIST', pageNo: 1 },
            });
        }
    } else if (typeof startUrl === 'string' && startUrl.trim()) {
        await requestQueue.addRequest({
            url: startUrl.trim(),
            userData: { label: 'LIST', pageNo: 1 },
        });
    } else if (typeof url === 'string' && url.trim()) {
        await requestQueue.addRequest({
            url: url.trim(),
            userData: { label: 'LIST', pageNo: 1 },
        });
    } else {
        const searchUrl = buildSearchUrl(keyword, 1);
        await requestQueue.addRequest({
            url: searchUrl,
            userData: { label: 'LIST', pageNo: 1, keyword },
        });
    }

    // Proxy configuration
    const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

    // Tracking
    let saved = 0;
    let detailEnqueued = 0;
    const seenJobIds = new Set();

    log.info(`🚀 RozeePk Scraper starting | target=${RESULTS_WANTED}, maxPages=${MAX_PAGES}`);

    const crawler = new CheerioCrawler({
        requestQueue,
        proxyConfiguration: proxyConfig,

        // Fast HTTP-only requests
        maxConcurrency: 10,
        minConcurrency: 3,
        maxRequestRetries: 3,
        requestHandlerTimeoutSecs: 30,

        // Stealth headers
        additionalMimeTypes: ['application/json'],
        preNavigationHooks: [
            async ({ request }) => {
                request.headers = {
                    'User-Agent': getRandomUserAgent(),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'max-age=0',
                };
            },
        ],

        requestHandler: async ({ $, request, log: crawlerLog }) => {
            const label = request.userData.label || 'LIST';

            // Early exit if target reached
            if (saved >= RESULTS_WANTED) {
                return;
            }

            // ----- LIST PAGES -----
            if (label === 'LIST') {
                const pageNo = request.userData.pageNo || 1;
                const kw = request.userData.keyword || keyword || '';

                // Check for blocking
                const bodyText = $('body').text().toLowerCase();
                if (bodyText.includes('forbidden') || bodyText.includes('access denied') || bodyText.includes('blocked')) {
                    crawlerLog.warning(`🚫 Page blocked: ${request.url}`);
                    throw new Error('Page blocked - will retry');
                }

                // Extract job URLs
                const jobUrls = [];
                $('a[href*="-jobs-"]').each((_, el) => {
                    const href = $(el).attr('href');
                    if (href && /-jobs-\d+/i.test(href)) {
                        const absUrl = toAbs(href, request.url);
                        if (absUrl && !jobUrls.includes(absUrl)) {
                            jobUrls.push(absUrl);
                        }
                    }
                });

                // Enqueue detail pages
                let newDetails = 0;
                for (const absUrl of jobUrls) {
                    if (saved + detailEnqueued >= RESULTS_WANTED) break;

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

                // Try JSON-LD first (most reliable)
                const jsonLdData = parseJsonLd($);

                // Fallback to HTML parsing
                const htmlData = {
                    title: cleanText($('h1').first().text() || $('h2').first().text() || $('.job-title').text()),
                    company: cleanText($('.company-name').text() || $('.cp-name').text() || $('[itemprop="hiringOrganization"]').text()),
                    location: cleanText($('.location').text() || $('.job-location').text() || $('[itemprop="jobLocation"]').text()),
                    description_html: $('.job-description').html() || $('#job-description').html() || $('[itemprop="description"]').html(),
                    salary: cleanText($('.salary').text() || $('.job-salary').text()),
                    contract_type: cleanText($('.job-type').text() || $('.employment-type').text()),
                };

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

                // Skip if no title
                if (!merged.title) {
                    crawlerLog.debug(`Skipping - no title: ${request.url}`);
                    return;
                }

                const job = {
                    source: 'rozee.pk',
                    job_id: request.userData.jobId || extractJobIdFromUrl(request.url),
                    url: request.url,
                    title: merged.title,
                    company: merged.company || null,
                    location: normalizeLocation(merged.location),
                    salary: merged.salary || null,
                    contract_type: merged.contract_type || null,
                    description_html: merged.description_html || null,
                    description_text: htmlToText(merged.description_html || ''),
                    date_posted: merged.date_posted || null,
                    valid_through: merged.valid_through || null,
                    scraped_at: new Date().toISOString(),
                };

                await Dataset.pushData(job);
                saved++;

                if (saved % 10 === 0 || saved >= RESULTS_WANTED) {
                    crawlerLog.info(`💾 Saved ${saved} jobs`);
                }

                return;
            }
        },

        failedRequestHandler: async ({ request, error, log: crawlerLog }) => {
            crawlerLog.error(`❌ Failed: ${request.url} - ${error.message}`);
        },
    });

    await crawler.run();

    log.info(`✅ Scraping complete. Total jobs saved: ${saved}`);

    if (saved === 0) {
        log.warning('⚠️ No jobs scraped. The site may be blocking or structure changed.');
    } else {
        log.info(`🎉 Successfully scraped ${saved} jobs from Rozee.pk`);
    }
});
