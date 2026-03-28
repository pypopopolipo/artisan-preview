/**
 * Data Quality Audit for artisans_unified.csv
 * Uses a proper CSV parser to handle quoted fields with commas.
 */
const fs = require('fs');
const path = require('path');

// ── Minimal RFC 4180 CSV parser ──────────────────────────────────────
function parseCSV(text) {
  const rows = [];
  let i = 0;
  const len = text.length;

  function parseField() {
    if (i >= len || text[i] === '\n' || text[i] === '\r') return '';
    if (text[i] === '"') {
      i++; // skip opening quote
      let field = '';
      while (i < len) {
        if (text[i] === '"') {
          if (i + 1 < len && text[i + 1] === '"') {
            field += '"';
            i += 2;
          } else {
            i++; // skip closing quote
            break;
          }
        } else {
          field += text[i];
          i++;
        }
      }
      return field;
    } else {
      let field = '';
      while (i < len && text[i] !== ',' && text[i] !== '\n' && text[i] !== '\r') {
        field += text[i];
        i++;
      }
      return field;
    }
  }

  while (i < len) {
    const row = [];
    while (true) {
      row.push(parseField());
      if (i < len && text[i] === ',') {
        i++; // skip comma
        continue;
      }
      break;
    }
    // skip line ending
    if (i < len && text[i] === '\r') i++;
    if (i < len && text[i] === '\n') i++;
    if (row.length > 1 || row[0] !== '') rows.push(row);
  }
  return rows;
}

// ── Load & parse ─────────────────────────────────────────────────────
console.log('Loading CSV...');
let raw = fs.readFileSync(path.join(__dirname, 'artisans_unified.csv'), 'utf-8');
// Strip BOM
if (raw.charCodeAt(0) === 0xFEFF) raw = raw.slice(1);

const allRows = parseCSV(raw);
const headers = allRows[0];
const data = allRows.slice(1);
console.log(`Parsed ${data.length} rows, ${headers.length} columns\n`);

// Column index map
const col = {};
headers.forEach((h, idx) => col[h.trim()] = idx);

function val(row, name) {
  const idx = col[name];
  if (idx === undefined) return '';
  return (row[idx] || '').trim();
}

// =====================================================================
// 1. SIRET active/inactive (etat_administratif)
// =====================================================================
console.log('='.repeat(55));
console.log('1. SIRET ACTIVE / INACTIVE (api_etat_administratif)');
console.log('='.repeat(55));
let etatF = 0, etatA = 0, etatC = 0, etatEmpty = 0, etatOther = 0;
for (const row of data) {
  const v = val(row, 'api_etat_administratif');
  if (v === 'F') etatF++;
  else if (v === 'A') etatA++;
  else if (v === 'C') etatC++;
  else if (v === '') etatEmpty++;
  else etatOther++;
}
console.log(`  Active (A):          ${etatA.toLocaleString()}`);
console.log(`  Cessée (C):          ${etatC.toLocaleString()}`);
console.log(`  Fermé (F):           ${etatF.toLocaleString()}`);
console.log(`  Empty (not checked): ${etatEmpty.toLocaleString()}`);
if (etatOther > 0) console.log(`  Other values:        ${etatOther.toLocaleString()}`);
console.log(`  Total:               ${data.length.toLocaleString()}`);
console.log(`  % inactive (C+F):    ${((etatC + etatF) / data.length * 100).toFixed(2)}%`);

// =====================================================================
// 2. LinkedIn-only entries
// =====================================================================
console.log('\n' + '='.repeat(55));
console.log('2. LINKEDIN-ONLY ENTRIES');
console.log('='.repeat(55));
let linkedinOnly = 0;
let sourceOnlyFlag = 0;
for (const row of data) {
  const src = val(row, 'sources');
  if (src === 'linkedin') linkedinOnly++;
  if (val(row, 'source_only_linkedin') === 'oui') sourceOnlyFlag++;
}
console.log(`  sources == "linkedin":         ${linkedinOnly.toLocaleString()}`);
console.log(`  source_only_linkedin == "oui": ${sourceOnlyFlag.toLocaleString()}`);

// =====================================================================
// 3. Site web pollution
// =====================================================================
console.log('\n' + '='.repeat(55));
console.log('3. SITE WEB POLLUTION');
console.log('='.repeat(55));

// Exact domain matches (checked against the extracted domain)
const suspiciousExact = new Set([
  'facebook.com', 'instagram.com', 'pagesjaunes.fr', 'youtube.com',
  'twitter.com', 'x.com', 'linkedin.com', 'tiktok.com',
  'google.com', 'google.fr', 'yelp.com', 'trustpilot.com', 'leboncoin.fr',
  'indeed.fr', 'indeed.com', 'pinterest.com', 'flickr.com', 'dailymotion.com',
  'viadeo.com', 'societe.com', 'infogreffe.fr', 'pappers.fr', 'verif.com',
  'kompass.com', 'europages.fr', 'batiweb.com', 'houzz.fr', 'houzz.com'
]);
// Substring patterns (for subdomains like m.facebook.com, fr-fr.facebook.com)
const suspiciousSubstrings = [
  'facebook.com', 'instagram.com', 'pagesjaunes.fr', 'paramount',
  'youtube.com', 'linkedin.com', 'tiktok.com', 'annuaire-entreprises',
  'societe.com', 'leboncoin.fr'
];

const domainCount = {};
let suspiciousTotal = 0;
let siteWebFilled = 0;

for (const row of data) {
  const sw = val(row, 'site_web');
  if (!sw) continue;
  siteWebFilled++;

  // Extract domain
  let domain = sw.replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0].toLowerCase();

  const isSuspicious = suspiciousExact.has(domain) || suspiciousSubstrings.some(p => domain.includes(p));
  if (isSuspicious) {
    suspiciousTotal++;
    domainCount[domain] = (domainCount[domain] || 0) + 1;
  }
}

console.log(`  Total with site_web:   ${siteWebFilled.toLocaleString()}`);
console.log(`  Suspicious URLs:       ${suspiciousTotal.toLocaleString()}`);
console.log(`  Clean URLs:            ${(siteWebFilled - suspiciousTotal).toLocaleString()}`);
console.log(`\n  Top 20 suspicious domains:`);
const topDomains = Object.entries(domainCount).sort((a, b) => b[1] - a[1]).slice(0, 20);
for (const [dom, cnt] of topDomains) {
  console.log(`    ${cnt.toString().padStart(5)}  ${dom}`);
}

// =====================================================================
// 4. Phone quality
// =====================================================================
console.log('\n' + '='.repeat(55));
console.log('4. PHONE QUALITY');
console.log('='.repeat(55));

function phoneStats(fieldName) {
  let total = 0, mobile = 0, landline = 0, other = 0;
  for (const row of data) {
    let ph = val(row, fieldName);
    if (!ph) continue;
    total++;
    ph = ph.replace(/[\s.\-()]/g, '');
    if (ph.startsWith('+33')) ph = '0' + ph.slice(3);

    if (/^0[67]/.test(ph)) mobile++;
    else if (/^0[1-5]/.test(ph)) landline++;
    else other++;
  }
  return { total, mobile, landline, other };
}

const telStats = phoneStats('telephone');
console.log(`  telephone:`);
console.log(`    Total filled:     ${telStats.total.toLocaleString()}`);
console.log(`    Mobile (06/07):   ${telStats.mobile.toLocaleString()} (${(telStats.mobile / telStats.total * 100).toFixed(1)}%)`);
console.log(`    Landline (01-05): ${telStats.landline.toLocaleString()} (${(telStats.landline / telStats.total * 100).toFixed(1)}%)`);
console.log(`    Other/intl:       ${telStats.other.toLocaleString()}`);

const dirTelStats = phoneStats('dirigeant_telephone');
console.log(`\n  dirigeant_telephone:`);
console.log(`    Total filled:     ${dirTelStats.total.toLocaleString()}`);
console.log(`    Mobile (06/07):   ${dirTelStats.mobile.toLocaleString()} (${(dirTelStats.mobile / dirTelStats.total * 100).toFixed(1)}%)`);
console.log(`    Landline (01-05): ${dirTelStats.landline.toLocaleString()} (${(dirTelStats.landline / dirTelStats.total * 100).toFixed(1)}%)`);
console.log(`    Other/intl:       ${dirTelStats.other.toLocaleString()}`);

// =====================================================================
// 5. Email quality spot check
// =====================================================================
console.log('\n' + '='.repeat(55));
console.log('5. EMAIL QUALITY SPOT CHECK (10 random each)');
console.log('='.repeat(55));

function sampleEmails(fieldName, n) {
  const emails = [];
  for (const row of data) {
    const e = val(row, fieldName);
    if (e) emails.push(e);
  }
  const sampled = [];
  const used = new Set();
  for (let i = 0; i < Math.min(n, emails.length); i++) {
    let idx;
    do { idx = Math.floor(Math.random() * emails.length); } while (used.has(idx));
    used.add(idx);
    sampled.push(emails[idx]);
  }
  return sampled;
}

console.log(`\n  email_generique (10 random):`);
for (const e of sampleEmails('email_generique', 10)) {
  console.log(`    ${e}`);
}

console.log(`\n  email_dirigeant (10 random):`);
for (const e of sampleEmails('email_dirigeant', 10)) {
  console.log(`    ${e}`);
}

// =====================================================================
// 6 & 7. Department stats
// =====================================================================
function deptStats(dept) {
  let total = 0, withEmail = 0, withMobile = 0, withRge = 0, withAssuranceRc = 0;
  let scoreSum = 0, scoreCount = 0;

  for (const row of data) {
    const cp = val(row, 'code_postal');
    if (!cp.startsWith(dept)) continue;
    total++;

    if (val(row, 'email_generique') || val(row, 'email_dirigeant') || val(row, 'website_emails')) withEmail++;

    const allPhones = [val(row, 'telephone'), val(row, 'dirigeant_telephone'), val(row, 'website_mobiles')].join(' ');
    const normalized = allPhones.replace(/[\s.\-()]/g, ' ').replace(/\+33/g, '0');
    if (/0[67]\d{8}/.test(normalized)) withMobile++;

    const rge = val(row, 'is_rge');
    if (rge === 'oui' || rge === '1' || rge === 'true') withRge++;

    if (val(row, 'assurance_rc')) withAssuranceRc++;

    const score = val(row, 'score_completude');
    if (score) {
      const s = parseFloat(score);
      if (!isNaN(s)) { scoreSum += s; scoreCount++; }
    }
  }

  return { total, withEmail, withMobile, withRge, withAssuranceRc, avgScore: scoreCount > 0 ? (scoreSum / scoreCount).toFixed(1) : 'N/A' };
}

// Verify score column
if (col['score_completude'] === undefined) {
  console.log(`\n  Note: score_completude column not found.`);
} else {
  console.log(`\n  score_completude column found at index ${col['score_completude']}`);
}

for (const dept of ['92', '76']) {
  console.log(`\n` + '='.repeat(55));
  console.log(`${dept === '92' ? '6' : '7'}. DEPARTMENT ${dept} STATS`);
  console.log('='.repeat(55));
  const s = deptStats(dept);
  if (s.total === 0) {
    console.log('  No entries found for this department.');
    continue;
  }
  console.log(`  Total fiches:       ${s.total.toLocaleString()}`);
  console.log(`  With email:         ${s.withEmail.toLocaleString()} (${(s.withEmail / s.total * 100).toFixed(1)}%)`);
  console.log(`  With mobile:        ${s.withMobile.toLocaleString()} (${(s.withMobile / s.total * 100).toFixed(1)}%)`);
  console.log(`  With RGE:           ${s.withRge.toLocaleString()} (${(s.withRge / s.total * 100).toFixed(1)}%)`);
  console.log(`  With assurance RC:  ${s.withAssuranceRc.toLocaleString()} (${(s.withAssuranceRc / s.total * 100).toFixed(1)}%)`);
  console.log(`  Avg completude:     ${s.avgScore}%`);
}

console.log('\n' + '='.repeat(55));
console.log('AUDIT COMPLETE');
console.log('='.repeat(55));
