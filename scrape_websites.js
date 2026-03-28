const fs = require("fs");
const path = require("path");
const https = require("https");
const http = require("http");

// === CONFIG ===
const UNIFIED_CSV = path.join(__dirname, "artisans_unified.csv");
const PROGRESS_FILE = path.join(__dirname, ".website_progress.json");
const BATCH_SIZE = 20;
const DELAY_BETWEEN_BATCHES = 300;
const SAVE_EVERY = 3; // batches
const MAX_CONSECUTIVE_ERRORS = 50;
const REQUEST_TIMEOUT = 10000; // 10s per site
const MAX_BODY_SIZE = 500000; // 500KB max per page

// === CSV PARSER ===
function parseCSVLine(line) {
  const fields = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') { current += '"'; i++; }
      else if (ch === '"') inQuotes = false;
      else current += ch;
    } else {
      if (ch === '"') inQuotes = true;
      else if (ch === ",") { fields.push(current); current = ""; }
      else current += ch;
    }
  }
  fields.push(current);
  return fields;
}

// === EXTRACT EMAILS FROM HTML ===
function extractEmails(html) {
  const emails = new Set();

  // mailto: links
  const mailtoRegex = /mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/gi;
  let m;
  while ((m = mailtoRegex.exec(html)) !== null) {
    emails.add(m[1].toLowerCase());
  }

  // Email patterns in text (more conservative)
  const emailRegex = /\b([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,6})\b/g;
  while ((m = emailRegex.exec(html)) !== null) {
    const email = m[1].toLowerCase();
    // Filter out false positives
    if (email.endsWith(".png") || email.endsWith(".jpg") || email.endsWith(".gif") ||
        email.endsWith(".svg") || email.endsWith(".css") || email.endsWith(".js") ||
        email.includes("@2x") || email.includes("@media") ||
        email.includes("sentry") || email.includes("webpack") ||
        email.includes("example.com") || email.includes("domain.com")) continue;
    emails.add(email);
  }

  return [...emails];
}

// === EXTRACT PHONES FROM HTML ===
function extractPhones(html) {
  // Remove HTML tags but keep text
  const text = html.replace(/<[^>]+>/g, " ");

  const phones = new Set();

  // French phone patterns: 06/07 (mobile), 01-05 (fixe), 09
  // Formats: 06 12 34 56 78, 06.12.34.56.78, 0612345678, +33 6 12 34 56 78
  const phonePatterns = [
    /(?:\+33|0033)[\s.\-]?([67])[\s.\-]?(\d{2})[\s.\-]?(\d{2})[\s.\-]?(\d{2})[\s.\-]?(\d{2})/g,
    /0([67])[\s.\-]?(\d{2})[\s.\-]?(\d{2})[\s.\-]?(\d{2})[\s.\-]?(\d{2})/g,
    /(?:\+33|0033)[\s.\-]?([1-59])[\s.\-]?(\d{2})[\s.\-]?(\d{2})[\s.\-]?(\d{2})[\s.\-]?(\d{2})/g,
    /0([1-59])[\s.\-]?(\d{2})[\s.\-]?(\d{2})[\s.\-]?(\d{2})[\s.\-]?(\d{2})/g,
  ];

  for (const regex of phonePatterns) {
    let m;
    while ((m = regex.exec(text)) !== null) {
      const formatted = "0" + m[1] + " " + m[2] + " " + m[3] + " " + m[4] + " " + m[5];
      phones.add(formatted);
    }
  }

  return [...phones];
}

// === CLASSIFY PHONE ===
function isMobile(phone) {
  const clean = phone.replace(/[\s.\-]/g, "");
  return /^0[67]/.test(clean);
}

// === CLASSIFY EMAIL ===
function isNominatif(email) {
  const local = email.split("@")[0];
  return /^[a-z]+[.\-_][a-z]+$/.test(local);
}

// === HTTP FETCH (follows redirects, handles https/http) ===
function fetchPage(url, redirects = 0) {
  return new Promise((resolve, reject) => {
    if (redirects > 5) return reject(new Error("Too many redirects"));

    // Normalize URL
    if (!url.startsWith("http")) url = "https://" + url;

    const client = url.startsWith("https") ? https : http;
    const options = {
      timeout: REQUEST_TIMEOUT,
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9",
      },
      rejectUnauthorized: false, // Accept self-signed certs (common on artisan sites)
    };

    const req = client.get(url, options, (res) => {
      // Follow redirects
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        let redirectUrl = res.headers.location;
        if (redirectUrl.startsWith("/")) {
          const parsed = new URL(url);
          redirectUrl = parsed.origin + redirectUrl;
        }
        return fetchPage(redirectUrl, redirects + 1).then(resolve).catch(reject);
      }

      if (res.statusCode !== 200) {
        return reject(new Error("HTTP " + res.statusCode));
      }

      let body = "";
      let size = 0;
      res.setEncoding("utf-8");
      res.on("data", (chunk) => {
        size += chunk.length;
        if (size > MAX_BODY_SIZE) {
          res.destroy();
          return;
        }
        body += chunk;
      });
      res.on("end", () => resolve(body));
      res.on("error", reject);
    });

    req.on("timeout", () => { req.destroy(); reject(new Error("Timeout")); });
    req.on("error", reject);
  });
}

// === TRY MULTIPLE PAGES (home + contact) ===
async function crawlSite(baseUrl) {
  const result = { emails: [], phones: [], mobiles: [], fixes: [], pages_crawled: 0 };

  // Normalize base URL
  let base = baseUrl.trim().replace(/\/+$/, "");
  if (!base.startsWith("http")) base = "https://" + base;

  // Fetch home page first
  let homeHtml;
  try {
    homeHtml = await fetchPage(base);
    result.pages_crawled++;
    const emails = extractEmails(homeHtml);
    const phones = extractPhones(homeHtml);
    for (const e of emails) if (!result.emails.includes(e)) result.emails.push(e);
    for (const p of phones) if (!result.phones.includes(p)) result.phones.push(p);
  } catch (e) {
    return result; // Home failed = skip entirely
  }

  // If no email found, try contact pages in parallel
  if (result.emails.length === 0) {
    const contactPages = [
      base + "/contact",
      base + "/contact.html",
      base + "/nous-contacter",
      base + "/contactez-nous",
    ];

    // Also look for contact link in home HTML
    const contactLinkMatch = homeHtml.match(/href=["']([^"']*(?:contact|nous-contacter|contactez)[^"']*)["']/i);
    if (contactLinkMatch) {
      let contactUrl = contactLinkMatch[1];
      if (contactUrl.startsWith("/")) contactUrl = base + contactUrl;
      else if (!contactUrl.startsWith("http")) contactUrl = base + "/" + contactUrl;
      if (!contactPages.includes(contactUrl)) contactPages.unshift(contactUrl);
    }

    const contactResults = await Promise.allSettled(
      contactPages.map(url => fetchPage(url))
    );

    for (const cr of contactResults) {
      if (cr.status === "fulfilled") {
        result.pages_crawled++;
        const emails = extractEmails(cr.value);
        const phones = extractPhones(cr.value);
        for (const e of emails) if (!result.emails.includes(e)) result.emails.push(e);
        for (const p of phones) if (!result.phones.includes(p)) result.phones.push(p);
        if (result.emails.length > 0) break;
      }
    }
  }

  // Classify phones
  for (const p of result.phones) {
    if (isMobile(p)) result.mobiles.push(p);
    else result.fixes.push(p);
  }

  return result;
}

// === LOAD COMPANIES ===
function loadCompanies() {
  console.log("Chargement du CSV unifié...");
  const content = fs.readFileSync(UNIFIED_CSV, "utf-8");
  const lines = content.split("\n");
  const headers = lines[0].split(",");

  const iSiret = headers.indexOf("siret");
  const iNom = headers.indexOf("nom_entreprise");
  const iSiteWeb = headers.indexOf("site_web");
  const iEmail = headers.indexOf("email_generique");
  const iEmailDir = headers.indexOf("email_dirigeant");
  const iTel = headers.indexOf("telephone");

  const companies = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const row = parseCSVLine(lines[i]);
    const siteWeb = (row[iSiteWeb] || "").trim();
    if (!siteWeb) continue;

    const email = (row[iEmail] || "").trim();
    const emailDir = (row[iEmailDir] || "").trim();
    const hasEmail = !!(email || emailDir);

    // Prioritize: those without email first, then those without tel
    companies.push({
      siret: (row[iSiret] || "").trim(),
      nom: (row[iNom] || "").trim(),
      site_web: siteWeb,
      has_email: hasEmail,
      has_tel: !!((row[iTel] || "").trim()),
      existing_email: email,
      existing_email_dir: emailDir,
      existing_tel: (row[iTel] || "").trim(),
    });
  }

  // Sort: no email first
  companies.sort((a, b) => (a.has_email ? 1 : 0) - (b.has_email ? 1 : 0));

  const noEmail = companies.filter(c => !c.has_email).length;
  const noTel = companies.filter(c => !c.has_tel).length;
  console.log(`  ${companies.length} entreprises avec site web`);
  console.log(`  ${noEmail} sans email (prioritaires)`);
  console.log(`  ${noTel} sans téléphone`);

  return companies;
}

// === PROGRESS ===
function loadProgress() {
  if (fs.existsSync(PROGRESS_FILE)) {
    return JSON.parse(fs.readFileSync(PROGRESS_FILE, "utf-8"));
  }
  return {};
}

function saveProgress(progress) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 0));
}

// === MAIN ===
async function main() {
  const companies = loadCompanies();
  const progress = loadProgress();
  const alreadyDone = Object.keys(progress).length;

  const todo = companies.filter(c => !(c.siret in progress));
  console.log(`\nDéjà traités: ${alreadyDone}`);
  console.log(`À traiter: ${todo.length}`);
  if (todo.length === 0) { console.log("Tout est fait !"); return; }

  let consecutiveErrors = 0;
  let processed = 0;
  let foundEmail = 0;
  let foundMobile = 0;
  let foundFixe = 0;
  let errors = 0;
  let noResult = 0;
  const startTime = Date.now();

  // Save on crash
  process.on("SIGINT", () => { saveProgress(progress); process.exit(0); });
  process.on("SIGTERM", () => { saveProgress(progress); process.exit(0); });
  process.on("uncaughtException", (e) => { console.error("\nCrash:", e.message); saveProgress(progress); process.exit(1); });
  process.on("unhandledRejection", (e) => { console.error("\nRejection:", e); saveProgress(progress); });

  console.log("\n--- Début du crawl ---\n");

  // Process in batches
  for (let b = 0; b < todo.length; b += BATCH_SIZE) {
    const batch = todo.slice(b, b + BATCH_SIZE);
    const results = await Promise.allSettled(
      batch.map(company => {
        return Promise.race([
          crawlSite(company.site_web),
          new Promise((_, reject) => setTimeout(() => reject(new Error("Global timeout")), 20000))
        ]);
      })
    );

    for (let j = 0; j < batch.length; j++) {
      const company = batch[j];
      processed++;

      if (results[j].status === "rejected") {
        errors++;
        consecutiveErrors++;
        progress[company.siret] = { _error: true };

        if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
          console.log(`\n\nTrop d'erreurs consécutives. Arrêt.`);
          saveProgress(progress);
          return;
        }
        continue;
      }

      consecutiveErrors = 0;
      const r = results[j].value;

      if (r.emails.length === 0 && r.phones.length === 0) {
        progress[company.siret] = { _empty: true, pages: r.pages_crawled };
        noResult++;
      } else {
        // Classify emails
        const nominatifs = r.emails.filter(isNominatif);
        const generiques = r.emails.filter(e => !isNominatif(e));

        progress[company.siret] = {
          emails: r.emails,
          emails_nominatifs: nominatifs,
          emails_generiques: generiques,
          phones: r.phones,
          mobiles: r.mobiles,
          fixes: r.fixes,
          pages_crawled: r.pages_crawled,
        };

        if (r.emails.length > 0) foundEmail++;
        if (r.mobiles.length > 0) foundMobile++;
        if (r.fixes.length > 0) foundFixe++;
      }
    }

    // Progress display
    const elapsed = (Date.now() - startTime) / 1000;
    const speed = processed / elapsed;
    const remaining = todo.length - processed;
    const eta = speed > 0 ? Math.round(remaining / speed / 60) : "?";
    process.stdout.write(
      `\r  ${alreadyDone + processed}/${alreadyDone + todo.length} | ` +
      `${foundEmail} emails | ${foundMobile} mobiles | ${foundFixe} fixes | ` +
      `${errors} err | ${speed.toFixed(1)}/s | ETA: ${eta}min    `
    );

    // Save every batch
    saveProgress(progress);

    await new Promise(r => setTimeout(r, DELAY_BETWEEN_BATCHES));
  }

  // Final save
  saveProgress(progress);

  // Stats
  const total = Object.keys(progress).length;
  const withEmails = Object.values(progress).filter(v => v.emails && v.emails.length > 0).length;
  const withMobiles = Object.values(progress).filter(v => v.mobiles && v.mobiles.length > 0).length;
  const withFixes = Object.values(progress).filter(v => v.fixes && v.fixes.length > 0).length;
  const withNominatifs = Object.values(progress).filter(v => v.emails_nominatifs && v.emails_nominatifs.length > 0).length;
  const empty = Object.values(progress).filter(v => v._empty).length;
  const errCount = Object.values(progress).filter(v => v._error).length;

  console.log("\n\n=== RÉSULTATS ===");
  console.log(`  Total crawlés:       ${total}`);
  console.log(`  Avec email(s):       ${withEmails}`);
  console.log(`    dont nominatif:    ${withNominatifs}`);
  console.log(`  Avec mobile(s):      ${withMobiles}`);
  console.log(`  Avec fixe(s):        ${withFixes}`);
  console.log(`  Rien trouvé:         ${empty}`);
  console.log(`  Erreurs (timeout…):  ${errCount}`);
  console.log(`\n  Progress: ${PROGRESS_FILE}`);
}

main().catch(e => {
  console.error("\nErreur fatale:", e.message);
  process.exit(1);
});
