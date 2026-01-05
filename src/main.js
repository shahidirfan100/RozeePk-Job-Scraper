import { Actor, log } from 'apify';
import { Dataset } from 'crawlee';
import { chromium } from 'playwright';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';

// ----------------- Constants -----------------

const BASE_URL = 'https://www.rozee.pk';
const SITEMAP_URL = 'https://www.rozee.pk/sitemap/jobs.xml';
const JOBS_PER_PAGE = 20;

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
];

const getRandomUserAgent = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
const randomDelay = (min = 100, max = 400) => new Promise((r) => setTimeout(r, Math.floor(Math.random() * (max - min)) + min));

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
    const match = url.match(/-jobs-(\d+)/i) || url.match(/(\d+)(?:\/)?$/);
    return match ? match[1] : null;
};

const buildSearchUrl = (keyword, page = 1) => {
    const kw = keyword?.trim() || 'all';
    const encodedKw = encodeURIComponent(kw);
    if (page === 1) return `${BASE_URL}/job/jsearch/q/${encodedKw}/fc/1`;
    return `${BASE_URL}/job/jsearch/q/${encodedKw}/fpn/${(page - 1) * JOBS_PER_PAGE}`;
};

const htmlToText = (html) => {
    if (!html) return '';
    return cleanText(
        html
            .replace(/<\s*br\s*\/?>/gi, '\n')
            .replace(/<\/\s*(p|div|li|h[1-6])\s*>/gi, '\n')
            .replace(/<[^>]+>/g, ' ')
            .replace(/\n\s*\n+/g, '\n\n')
    );
};

const normalizeLocation = (loc) => {
    if (!loc) return '';
    const parts = loc
        .split(',')
        .map((p) => cleanText(p))
        .filter((p) => p && !p.includes('[object') && !/^\d+$/.test(p));
    const unique = [...new Set(parts.map((p) => p.toLowerCase()))].map(
        (l) => parts.find((p) => p.toLowerCase() === l)
    );
    return unique.slice(0, 3).join(', ');
};

const matchesKeyword = (url, keyword) => {
    if (!keyword?.trim()) return true;
    const urlLower = url.toLowerCase();
    return keyword.toLowerCase().split(/\s+/).some((kw) => urlLower.includes(kw));
};

// Parse JSON-LD from HTML
const parseJsonLd = ($) => {
    const result = {};
    $('script[type="application/ld+json"]').each((_, el) => {
        try {
            const json = JSON.parse($(el).html());
            const items = Array.isArray(json) ? json : [json];
            for (const item of items) {
                if (item['@type'] === 'JobPosting') {
                    result.title = result.title || item.title;
                    if (item.hiringOrganization) {
                        const org = item.hiringOrganization;
                        result.company = result.company || (typeof org === 'string' ? org : org.name);
                    }
                    if (item.jobLocation && !result.location) {
                        const loc = Array.isArray(item.jobLocation) ? item.jobLocation[0] : item.jobLocation;
                        const addr = loc?.address;
                        if (addr) {
                            result.location = [addr.addressLocality, addr.addressRegion, addr.addressCountry]
                                .filter(Boolean)
                                .join(', ');
                        }
                    }
                    if (item.baseSalary?.value && !result.salary) {
                        const val = item.baseSalary.value;
                        if (typeof val === 'object') {
                            const { minValue, maxValue, currency } = val;
                            result.salary = `${currency || ''} ${minValue && maxValue ? `${minValue}–${maxValue}` : minValue || maxValue || ''}`.trim();
                        } else {
                            result.salary = String(val);
                        }
                    }
                    result.contract_type = result.contract_type || item.employmentType;
                    result.description = result.description || item.description;
                    result.date_posted = result.date_posted || item.datePosted;
                    result.valid_through = result.valid_through || item.validThrough;
                }
            }
        } catch { }
    });
    return result;
};

// ----------------- MAIN -----------------

Actor.main(async () => {
    log.setLevel(log.LEVELS.INFO);

    const input = (await Actor.getInput()) || {};
    const {
        keyword = '',
        results_wanted: RESULTS_WANTED_RAW = 100,
        max_pages: MAX_PAGES_RAW = 50,
        startUrl,
        proxyConfiguration,
    } = input;

    const RESULTS_WANTED = parseNumber(RESULTS_WANTED_RAW, 100);
    const MAX_PAGES = parseNumber(MAX_PAGES_RAW, 50);

    const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);
    const proxyUrl = proxyConfig ? await proxyConfig.newUrl() : null;

    log.info(`🚀 RozeePk Hybrid Scraper | target=${RESULTS_WANTED}`);

    // ===== STEP 1: Launch Playwright to get cookies =====
    log.info('🌐 Launching browser to get cookies...');

    const browser = await chromium.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-blink-features=AutomationControlled'],
    });

    const context = await browser.newContext({
        userAgent: getRandomUserAgent(),
        viewport: { width: 1280, height: 720 },
        proxy: proxyUrl ? { server: proxyUrl } : undefined,
    });

    const page = await context.newPage();

    // Block heavy resources
    await page.route('**/*', (route) => {
        const type = route.request().resourceType();
        if (['image', 'media', 'font', 'stylesheet'].includes(type)) {
            return route.abort();
        }
        return route.continue();
    });

    // Navigate to get cookies
    const initUrl = startUrl || buildSearchUrl(keyword, 1);
    try {
        await page.goto(initUrl, { waitUntil: 'domcontentloaded', timeout: 20000 });
        await page.waitForTimeout(1500);
    } catch (err) {
        log.warning(`Browser navigation warning: ${err.message}`);
    }

    // Extract cookies and headers
    const cookies = await context.cookies();
    const cookieString = cookies.map((c) => `${c.name}=${c.value}`).join('; ');

    log.info(`🍪 Got ${cookies.length} cookies from browser`);

    // Extract job URLs from initial page if it's a search page
    let jobUrls = [];
    const seenJobIds = new Set();

    try {
        const pageContent = await page.content();
        const $ = cheerio.load(pageContent);
        $('a[href*="-jobs-"]').each((_, el) => {
            const href = $(el).attr('href');
            if (href && /-jobs-\d+/i.test(href)) {
                try {
                    const absUrl = new URL(href, BASE_URL).href;
                    const jobId = extractJobIdFromUrl(absUrl);
                    if (jobId && !seenJobIds.has(jobId)) {
                        seenJobIds.add(jobId);
                        jobUrls.push(absUrl);
                    }
                } catch { }
            }
        });
        log.info(`📄 Extracted ${jobUrls.length} job URLs from initial page`);
    } catch (err) {
        log.warning(`Failed to extract from initial page: ${err.message}`);
    }

    // If no keyword and no startUrl, use sitemap
    if (jobUrls.length === 0 && !startUrl && !keyword?.trim()) {
        log.info('📥 Fetching sitemap for all jobs...');
        try {
            await page.goto(SITEMAP_URL, { waitUntil: 'domcontentloaded', timeout: 15000 });
            const sitemapContent = await page.content();
            const urlMatches = sitemapContent.match(/<loc>(https:\/\/www\.rozee\.pk\/[^<]+jobs-\d+)<\/loc>/gi) || [];
            jobUrls = urlMatches
                .map((m) => m.replace(/<\/?loc>/gi, ''))
                .filter((url) => matchesKeyword(url, keyword))
                .slice(0, RESULTS_WANTED);
            log.info(`📄 Sitemap: found ${jobUrls.length} job URLs`);
        } catch (err) {
            log.warning(`Sitemap fetch failed: ${err.message}`);
        }
    }

    // Paginate if we need more jobs
    let currentPage = 1;
    while (jobUrls.length < RESULTS_WANTED && currentPage < MAX_PAGES) {
        currentPage++;
        const nextUrl = buildSearchUrl(keyword, currentPage);
        try {
            await page.goto(nextUrl, { waitUntil: 'domcontentloaded', timeout: 15000 });
            await page.waitForTimeout(800);

            const pageContent = await page.content();
            const $ = cheerio.load(pageContent);
            let foundOnPage = 0;

            $('a[href*="-jobs-"]').each((_, el) => {
                const href = $(el).attr('href');
                if (href && /-jobs-\d+/i.test(href)) {
                    try {
                        const absUrl = new URL(href, BASE_URL).href;
                        const jobId = extractJobIdFromUrl(absUrl);
                        if (jobId && !seenJobIds.has(jobId)) {
                            seenJobIds.add(jobId);
                            jobUrls.push(absUrl);
                            foundOnPage++;
                        }
                    } catch { }
                }
            });

            log.info(`📄 Page ${currentPage}: found ${foundOnPage} new job URLs (total: ${jobUrls.length})`);

            if (foundOnPage === 0) break;
        } catch (err) {
            log.warning(`Pagination error on page ${currentPage}: ${err.message}`);
            break;
        }
    }

    // Close browser - we have cookies now
    await browser.close();
    log.info('🔒 Browser closed. Switching to fast HTTP requests...');

    // ===== STEP 2: Use got-scraping with cookies for fast detail fetching =====
    const headers = {
        'User-Agent': getRandomUserAgent(),
        Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        Cookie: cookieString,
        Connection: 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    };

    let saved = 0;
    const urlsToProcess = jobUrls.slice(0, RESULTS_WANTED);

    log.info(`⚡ Fetching ${urlsToProcess.length} job details with got-scraping...`);

    for (const jobUrl of urlsToProcess) {
        if (saved >= RESULTS_WANTED) break;

        try {
            await randomDelay(100, 300);

            const response = await gotScraping({
                url: jobUrl,
                headers,
                proxyUrl: proxyUrl || undefined,
                timeout: { request: 15000 },
                retry: { limit: 2 },
            });

            if (response.statusCode !== 200) {
                log.debug(`Skip ${jobUrl}: status ${response.statusCode}`);
                continue;
            }

            const $ = cheerio.load(response.body);
            const jsonLdData = parseJsonLd($);

            // HTML fallback
            const htmlData = {
                title: cleanText($('h1').first().text() || $('h2').first().text()),
                company: cleanText($('.company-name').text() || $('.cp-name').text()),
                location: cleanText($('.location').text() || $('.job-location').text()),
                description: $('.job-description').html() || $('#job-description').html(),
            };

            const merged = {
                title: jsonLdData.title || htmlData.title,
                company: jsonLdData.company || htmlData.company,
                location: jsonLdData.location || htmlData.location,
                salary: jsonLdData.salary,
                contract_type: jsonLdData.contract_type,
                description: jsonLdData.description || htmlData.description,
                date_posted: jsonLdData.date_posted,
                valid_through: jsonLdData.valid_through,
            };

            if (!merged.title) continue;

            const job = {
                source: 'rozee.pk',
                job_id: extractJobIdFromUrl(jobUrl),
                url: jobUrl,
                title: merged.title,
                company: merged.company || null,
                location: normalizeLocation(merged.location),
                salary: merged.salary || null,
                contract_type: merged.contract_type || null,
                description_html: merged.description || null,
                description_text: htmlToText(merged.description || ''),
                date_posted: merged.date_posted || null,
                valid_through: merged.valid_through || null,
                scraped_at: new Date().toISOString(),
            };

            await Dataset.pushData(job);
            saved++;

            if (saved % 10 === 0) {
                log.info(`💾 Saved ${saved}/${RESULTS_WANTED} jobs`);
            }
        } catch (err) {
            log.debug(`Error fetching ${jobUrl}: ${err.message}`);
        }
    }

    log.info(`✅ Complete! Saved ${saved} jobs`);

    if (saved === 0) {
        log.warning('⚠️ No jobs scraped.');
    } else {
        log.info(`🎉 Successfully scraped ${saved} jobs from Rozee.pk`);
    }
});
