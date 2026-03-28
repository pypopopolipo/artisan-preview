const fs = require("fs");
const path = require("path");
const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");

puppeteer.use(StealthPlugin());

// === CONFIG ===
const UNIFIED_CSV = path.join(__dirname, "artisans_unified.csv");
const PROGRESS_FILE = path.join(__dirname, ".pj_progress.json");
const SAMPLE_SIZE = 500; // 0 = tout
const DELAY_MIN = 3000;
const DELAY_MAX = 5000;
const SAVE_EVERY = 10;
const MAX_CONSECUTIVE_ERRORS = 20;
const SEARCH_TIMEOUT = 15000;
const PJ_BASE = "https://www.pagesjaunes.fr";

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

// === TRIGRAMMES (matching) ===
function trigrams(s) {
  s = s.toLowerCase().replace(/[^a-z0-9àâäéèêëïîôùûüç]/g, " ").replace(/\s+/g, " ").trim();
  const t = new Set();
  for (let i = 0; i <= s.length - 3; i++) t.add(s.substring(i, i + 3));
  return t;
}

function trigramScore(a, b) {
  const ta = trigrams(a);
  const tb = trigrams(b);
  if (ta.size === 0 || tb.size === 0) return 0;
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter++;
  return inter / Math.max(ta.size, tb.size);
}

// === LOAD COMPANIES TO SCRAPE ===
function loadCompanies() {
  console.log("Chargement du CSV unifié...");
  const content = fs.readFileSync(UNIFIED_CSV, "utf-8");
  const lines = content.split("\n");
  const headers = lines[0].split(",");

  const iSiret = headers.indexOf("siret");
  const iNom = headers.indexOf("nom_entreprise");
  const iCP = headers.indexOf("code_postal");
  const iVille = headers.indexOf("ville");
  const iSources = headers.indexOf("sources");
  const iEmail = headers.indexOf("email_generique");
  const iEmailDir = headers.indexOf("email_dirigeant");
  const iTel = headers.indexOf("telephone");

  const companies = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const row = parseCSVLine(lines[i]);
    const sources = (row[iSources] || "").trim();

    // Filtrer: CAPEB-only (avec ou sans api_sirene)
    const srcList = sources.split("|").filter(s => s && s !== "api_sirene");
    if (srcList.length !== 1 || srcList[0] !== "capeb") continue;

    const nom = (row[iNom] || "").trim();
    const cp = (row[iCP] || "").trim();
    if (!nom || !cp) continue;

    companies.push({
      siret: (row[iSiret] || "").trim(),
      nom,
      code_postal: cp,
      ville: (row[iVille] || "").trim(),
      has_email: !!((row[iEmail] || "").trim() || (row[iEmailDir] || "").trim()),
      has_tel: !!((row[iTel] || "").trim()),
    });
  }

  console.log(`  ${companies.length} entreprises CAPEB-only trouvées`);
  return companies;
}

// === LOAD PROGRESS ===
function loadProgress() {
  if (fs.existsSync(PROGRESS_FILE)) {
    return JSON.parse(fs.readFileSync(PROGRESS_FILE, "utf-8"));
  }
  return {};
}

function saveProgress(progress) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 0));
}

// === RANDOM DELAY ===
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function randomDelay() { return sleep(DELAY_MIN + Math.random() * (DELAY_MAX - DELAY_MIN)); }

// === SCRAPE SEARCH RESULTS PAGE ===
async function scrapeSearchResults(page) {
  return await page.evaluate(() => {
    const results = [];
    const articles = document.querySelectorAll("#listResults article, .bi-list article, .list-results article, [id*='bloc'] .bi");

    for (const article of articles) {
      // Nom
      let name = "";
      const nameEl = article.querySelector(".denomination-links, .bi-denomination a, .denomination a, h2 a, h3 a");
      if (nameEl) name = nameEl.innerText.trim();

      // Lien détail
      let detailUrl = "";
      if (nameEl && nameEl.href) detailUrl = nameEl.href;

      // Adresse
      let address = "";
      const addrEl = article.querySelector(".adresse, .bi-adresse, .address");
      if (addrEl) address = addrEl.innerText.trim();

      // Téléphone
      let phone = "";
      const phoneEl = article.querySelector(".tel-zone .num, .bi-phone .num, .click_memory_number, [data-phone]");
      if (phoneEl) phone = phoneEl.getAttribute("title") || phoneEl.innerText.trim();

      // Website (encodé en base64 dans data-pjlb)
      let website = "";
      const siteEl = article.querySelector(".site-internet a, .bi-website a");
      if (siteEl) {
        const pjlb = siteEl.getAttribute("data-pjlb");
        if (pjlb) {
          try {
            const parsed = JSON.parse(pjlb);
            if (parsed.url) website = atob(parsed.url);
          } catch(e) {}
        }
        if (!website && siteEl.href) website = siteEl.href;
      }

      // Catégorie
      let category = "";
      const catEl = article.querySelector(".activites, .bi-activite, .activity");
      if (catEl) category = catEl.innerText.trim();

      // Avis
      let rating = null;
      let reviewCount = 0;
      const nbAvis = article.querySelector(".nb_avis, .bi-nb-avis");
      if (nbAvis) reviewCount = parseInt(nbAvis.innerText) || 0;
      const stars = article.querySelectorAll(".icon-etoile, .star-active");
      const nullStars = article.querySelectorAll(".nullnote, .star-inactive");
      if (stars.length > 0) {
        rating = stars.length;
        const halfStar = article.querySelector(".halfnote, .star-half");
        if (halfStar) rating += 0.5;
      } else if (nullStars.length > 0) {
        rating = 5 - nullStars.length;
      }

      if (name) {
        results.push({ name, address, phone, website, category, rating, reviewCount, detailUrl });
      }
    }
    return results;
  });
}

// === SCRAPE DETAIL PAGE (for email) ===
async function scrapeDetailPage(page, url) {
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: SEARCH_TIMEOUT });
    await sleep(1500);

    return await page.evaluate(() => {
      const data = { email: "", phones: [], horaires: "", description: "", siret: "" };

      // Email
      const emailEl = document.querySelector("a[href^='mailto:'], .icon-mail + a, .icon-enveloppe + a, [data-pjlb*='mailto']");
      if (emailEl) {
        const href = emailEl.getAttribute("href") || "";
        if (href.startsWith("mailto:")) {
          data.email = href.replace("mailto:", "").split("?")[0].trim();
        } else {
          // Try data-pjlb
          const pjlb = emailEl.getAttribute("data-pjlb");
          if (pjlb) {
            try {
              const parsed = JSON.parse(pjlb);
              if (parsed.url) {
                const decoded = atob(parsed.url);
                if (decoded.includes("mailto:")) data.email = decoded.replace("mailto:", "").split("?")[0].trim();
                else if (decoded.includes("@")) data.email = decoded.trim();
              }
            } catch(e) {}
          }
        }
      }

      // Try harder for email
      if (!data.email) {
        const allLinks = document.querySelectorAll("a[href*='mailto:']");
        for (const link of allLinks) {
          const href = link.getAttribute("href") || "";
          if (href.startsWith("mailto:")) {
            data.email = href.replace("mailto:", "").split("?")[0].trim();
            break;
          }
        }
      }

      // Additional phones
      const phoneEls = document.querySelectorAll(".tel-zone .num, .coord-numero, [data-phone], .click_memory_number");
      for (const el of phoneEls) {
        const p = (el.getAttribute("title") || el.innerText || "").trim();
        if (p && !data.phones.includes(p)) data.phones.push(p);
      }

      // Horaires
      const horaireEl = document.querySelector(".zone-horaires, .horaires, .opening-hours");
      if (horaireEl) data.horaires = horaireEl.innerText.trim().replace(/\n+/g, " | ");

      // Description
      const descEl = document.querySelector(".zone-cvi-cviv, .bi-description, .description-pro");
      if (descEl) data.description = descEl.innerText.trim().substring(0, 500);

      // SIRET
      const siretEl = document.querySelector(".siret, [itemprop='taxID']");
      if (siretEl) data.siret = siretEl.innerText.replace(/\s/g, "").trim();

      // Try finding SIRET in page text
      if (!data.siret) {
        const body = document.body.innerText;
        const siretMatch = body.match(/SIRET\s*:?\s*(\d{14})/i);
        if (siretMatch) data.siret = siretMatch[1];
      }

      return data;
    });
  } catch (e) {
    return { email: "", phones: [], horaires: "", description: "", siret: "" };
  }
}

// === ACCEPT COOKIES ===
async function acceptCookies(page) {
  try {
    await sleep(2000);
    const btn = await page.$('#didomi-notice-agree-button, [id*="accept"], .didomi-popup-notice-buttons button:first-child');
    if (btn) {
      await btn.click();
      await sleep(1000);
    }
  } catch(e) {}
}

// === MAIN ===
async function main() {
  const companies = loadCompanies();
  const progress = loadProgress();
  const alreadyDone = Object.keys(progress).length;

  // Filter already done
  let todo = companies.filter(c => !(c.siret in progress));
  if (SAMPLE_SIZE > 0) todo = todo.slice(0, SAMPLE_SIZE);

  console.log(`\nDéjà traités: ${alreadyDone}`);
  console.log(`À traiter: ${todo.length}`);
  if (todo.length === 0) {
    console.log("Tout est fait !");
    return;
  }

  // Launch browser
  console.log("\nLancement du navigateur...");
  const browser = await puppeteer.launch({
    headless: "new",
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--window-size=1920,1080",
    ],
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1920, height: 1080 });
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
  );
  await page.setExtraHTTPHeaders({ "Accept-Language": "fr-FR,fr;q=0.9" });

  // First visit to get cookies
  console.log("Visite initiale PJ...");
  await page.goto(PJ_BASE, { waitUntil: "domcontentloaded", timeout: 30000 });
  await acceptCookies(page);

  let consecutiveErrors = 0;
  let found = 0;
  let notFound = 0;
  let errors = 0;
  let withEmail = 0;
  let withPhone = 0;
  const startTime = Date.now();

  console.log("\n--- Début du scraping ---\n");

  for (let i = 0; i < todo.length; i++) {
    const company = todo[i];
    const elapsed = (Date.now() - startTime) / 1000;
    const speed = (i + 1) / elapsed || 0;
    const eta = speed > 0 ? Math.round((todo.length - i - 1) / speed / 60) : "?";

    process.stdout.write(
      `\r  ${alreadyDone + i + 1}/${alreadyDone + todo.length} | ` +
      `${found} matchés | ${notFound} pas trouvés | ${errors} err | ` +
      `${withEmail} emails | ${withPhone} tels | ` +
      `ETA: ${eta}min    `
    );

    try {
      // Search on PJ
      const searchQuery = encodeURIComponent(company.nom);
      const searchLocation = encodeURIComponent(company.code_postal);
      const searchUrl = `${PJ_BASE}/annuaire/chercherlespros?quoiqui=${searchQuery}&ou=${searchLocation}`;

      await page.goto(searchUrl, { waitUntil: "domcontentloaded", timeout: SEARCH_TIMEOUT });
      await sleep(1500 + Math.random() * 1000); // Wait for dynamic content

      // Parse results
      const results = await scrapeSearchResults(page);

      if (results.length === 0) {
        progress[company.siret] = { _not_found: true };
        notFound++;
        consecutiveErrors = 0;
      } else {
        // Find best match
        let bestMatch = null;
        let bestScore = 0;

        for (const result of results) {
          let score = trigramScore(company.nom, result.name);

          // Bonus: code postal dans l'adresse
          if (result.address && result.address.includes(company.code_postal)) {
            score += 0.15;
          }

          // Bonus: ville dans l'adresse
          if (result.address && company.ville &&
              result.address.toLowerCase().includes(company.ville.toLowerCase())) {
            score += 0.1;
          }

          if (score > bestScore) {
            bestScore = score;
            bestMatch = result;
          }
        }

        if (bestMatch && bestScore >= 0.45) {
          // Confidence level
          let confidence = "low";
          if (bestScore >= 0.85) confidence = "high";
          else if (bestScore >= 0.65) confidence = "medium";

          const entry = {
            pj_nom: bestMatch.name,
            telephone: bestMatch.phone || "",
            site_web: bestMatch.website || "",
            categorie_pj: bestMatch.category || "",
            avis_note: bestMatch.rating,
            avis_nb: bestMatch.reviewCount,
            pj_confidence: confidence,
            pj_score: Math.round(bestScore * 100) / 100,
            pj_url: bestMatch.detailUrl || "",
            email: "",
            telephones: [],
            horaires: "",
            description: "",
            pj_siret: "",
          };

          // Visit detail page for email + more data
          if (bestMatch.detailUrl && bestScore >= 0.55) {
            const detail = await scrapeDetailPage(page, bestMatch.detailUrl);
            entry.email = detail.email || "";
            entry.telephones = detail.phones || [];
            entry.horaires = detail.horaires || "";
            entry.description = detail.description || "";
            entry.pj_siret = detail.siret || "";

            // Cross-validate SIRET if found
            if (entry.pj_siret && entry.pj_siret !== company.siret) {
              // SIRET mismatch → lower confidence
              if (entry.pj_siret.length === 14) {
                entry.pj_confidence = "siret_mismatch";
              }
            } else if (entry.pj_siret && entry.pj_siret === company.siret) {
              entry.pj_confidence = "verified"; // SIRET match = certain
            }
          }

          if (entry.email) withEmail++;
          if (entry.telephone || entry.telephones.length > 0) withPhone++;

          progress[company.siret] = entry;
          found++;
          consecutiveErrors = 0;
        } else {
          progress[company.siret] = { _not_found: true, best_score: bestScore };
          notFound++;
          consecutiveErrors = 0;
        }
      }
    } catch (e) {
      errors++;
      consecutiveErrors++;

      if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
        console.log(`\n\nTrop d'erreurs consécutives (${MAX_CONSECUTIVE_ERRORS}). Arrêt propre.`);
        console.log(`Dernière erreur: ${e.message}`);
        break;
      }

      // Don't save errors → will retry next run
      continue;
    }

    // Save progress
    if ((i + 1) % SAVE_EVERY === 0) {
      saveProgress(progress);
    }

    // Random delay
    await randomDelay();
  }

  // Final save
  saveProgress(progress);
  await browser.close();

  // Stats
  const totalDone = Object.keys(progress).length;
  const totalFound = Object.values(progress).filter(v => !v._not_found).length;
  const totalEmails = Object.values(progress).filter(v => v.email).length;
  const totalPhones = Object.values(progress).filter(v => v.telephone || (v.telephones && v.telephones.length > 0)).length;

  const byConfidence = {};
  for (const v of Object.values(progress)) {
    if (v._not_found) continue;
    const c = v.pj_confidence || "unknown";
    byConfidence[c] = (byConfidence[c] || 0) + 1;
  }

  console.log("\n\n=== RÉSULTATS ===");
  console.log(`  Total traités:  ${totalDone}`);
  console.log(`  Matchés:        ${totalFound}`);
  console.log(`  Non trouvés:    ${totalDone - totalFound}`);
  console.log(`  Avec email:     ${totalEmails}`);
  console.log(`  Avec téléphone: ${totalPhones}`);
  console.log(`  Par confidence:`);
  for (const [c, n] of Object.entries(byConfidence).sort((a, b) => b[1] - a[1])) {
    console.log(`    ${c}: ${n}`);
  }
  console.log(`\n  Progress: ${PROGRESS_FILE}`);
}

main().catch(e => {
  console.error("\nErreur fatale:", e.message);
  process.exit(1);
});
