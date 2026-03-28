/**
 * generate_demo_page.js
 * Generates a standalone HTML demo page for a specific department
 * showing N representative unlocked artisan fiches.
 *
 * Usage: node generate_demo_page.js <dept> [count=50]
 * Example: node generate_demo_page.js 30 50
 */

const fs = require('fs');
const path = require('path');

// --- Args ---
const dept = process.argv[2];
const count = parseInt(process.argv[3] || '50', 10);
const excludeFlag = process.argv.find(a => a.startsWith('--exclude='));
const excludeAssureur = excludeFlag ? excludeFlag.split('=')[1].toUpperCase() : null;
const suffixFlag = process.argv.find(a => a.startsWith('--suffix='));
const fileSuffix = suffixFlag ? suffixFlag.split('=')[1] : '';
const latFlag = process.argv.find(a => a.startsWith('--lat='));
const lonFlag = process.argv.find(a => a.startsWith('--lon='));
const radiusFlag = process.argv.find(a => a.startsWith('--radius='));
const geoLat = latFlag ? parseFloat(latFlag.split('=')[1]) : null;
const geoLon = lonFlag ? parseFloat(lonFlag.split('=')[1]) : null;
const geoRadius = radiusFlag ? parseFloat(radiusFlag.split('=')[1]) : null;
const useGeo = geoLat !== null && geoLon !== null && geoRadius !== null;

function haversine(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = (lat2-lat1)*Math.PI/180;
  const dLon = (lon2-lon1)*Math.PI/180;
  const a = Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLon/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

if (!dept) { console.error('Usage: node generate_demo_page.js <dept|zone> [count] [--lat=X --lon=Y --radius=Zkm] [--exclude=AXA] [--suffix=-noaxa]'); process.exit(1); }

// --- CSV Parser (handles BOM, quoted fields with commas/newlines) ---
function parseCSV(text) {
  // Strip BOM
  if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);

  const rows = [];
  let i = 0;
  const len = text.length;

  function parseField() {
    if (i >= len) return '';
    if (text[i] === '"') {
      i++; // skip opening quote
      let field = '';
      while (i < len) {
        if (text[i] === '"') {
          if (i + 1 < len && text[i + 1] === '"') { field += '"'; i += 2; }
          else { i++; break; }
        } else { field += text[i]; i++; }
      }
      return field;
    } else {
      let field = '';
      while (i < len && text[i] !== ',' && text[i] !== '\n' && text[i] !== '\r') {
        field += text[i]; i++;
      }
      return field;
    }
  }

  while (i < len) {
    const row = [];
    while (true) {
      row.push(parseField());
      if (i < len && text[i] === ',') { i++; continue; }
      break;
    }
    // skip line endings
    if (i < len && text[i] === '\r') i++;
    if (i < len && text[i] === '\n') i++;
    rows.push(row);
  }

  if (rows.length === 0) return [];
  const headers = rows[0];
  return rows.slice(1).filter(r => r.length >= headers.length / 2).map(r => {
    const obj = {};
    headers.forEach((h, idx) => { obj[h.trim()] = (r[idx] || '').trim(); });
    return obj;
  });
}

// --- Load data ---
console.log(`Loading artisans_unified.csv...`);
const csvPath = path.join(__dirname, 'artisans_unified.csv');
const raw = fs.readFileSync(csvPath, 'utf-8');
const allRows = parseCSV(raw);
console.log(`Loaded ${allRows.length} rows total.`);

// --- Filter by department or GPS radius ---
let deptRows;
if (useGeo) {
  deptRows = allRows.filter(r => {
    const lat = parseFloat(r.latitude);
    const lon = parseFloat(r.longitude);
    if (isNaN(lat) || isNaN(lon)) return false;
    return haversine(geoLat, geoLon, lat, lon) <= geoRadius;
  });
  console.log(`Radius ${geoRadius}km from ${geoLat},${geoLon}: ${deptRows.length} artisans found.`);
} else {
  deptRows = allRows.filter(r => r.code_postal && r.code_postal.startsWith(dept));
  console.log(`Department ${dept}: ${deptRows.length} artisans found.`);
}

if (deptRows.length === 0) { console.error(`No artisans found.`); process.exit(1); }

// --- Filter: must have at least 1 contact ---
const withContact = deptRows.filter(r => {
  return r.email_generique || r.email_dirigeant || r.telephone || r.dirigeant_telephone ||
    r.website_emails || r.website_mobiles;
});
console.log(`With at least 1 contact: ${withContact.length}`);

// --- Exclude assureur if specified ---
const pool = excludeAssureur ? withContact.filter(r => {
  const all = [r.assurance_rc, r.assurance_dc, r.qualibat_assurance].filter(Boolean).join(' ').toUpperCase();
  return !all.includes(excludeAssureur);
}) : withContact;
if (excludeAssureur) console.log(`After excluding ${excludeAssureur}: ${pool.length}`);

// --- Sort by score_completude desc, then take every Nth ---
pool.sort((a, b) => (parseFloat(b.score_completude) || 0) - (parseFloat(a.score_completude) || 0));
const step = Math.max(1, Math.floor(pool.length / count));
const sample = [];
for (let idx = 0; idx < pool.length && sample.length < count; idx += step) {
  sample.push(pool[idx]);
}
// If we don't have enough, fill from the top
if (sample.length < count) {
  for (const r of pool) {
    if (sample.length >= count) break;
    if (!sample.includes(r)) sample.push(r);
  }
}
console.log(`Sample size: ${sample.length}`);

// --- Compute stats on filtered fiches ---
const total = pool.length;
const hasMobile = pool.filter(r => {
  const phones = [r.telephone, r.dirigeant_telephone, r.website_mobiles].filter(Boolean).join(' ');
  return /0[67]/.test(phones.replace(/[\s.\-]/g, ''));
}).length;
const pctMobile = Math.round(100 * hasMobile / total);
const hasRGE = pool.filter(r => r.is_rge === 'oui' || r.is_rge === 'Oui' || r.is_rge === '1' || r.is_rge === 'true').length;
const pctRGE = Math.round(100 * hasRGE / total);
const hasAssurance = pool.filter(r => r.assurance_rc || r.assurance_dc).length;
const pctAssurance = Math.round(100 * hasAssurance / total);

// --- Helpers ---
function esc(s) {
  if (!s) return '';
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
function fmtPhone(p) {
  if (!p) return '';
  const clean = p.replace(/[\s.\-()]/g, '');
  if (/^\d{10}$/.test(clean)) return clean.replace(/(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/, '$1 $2 $3 $4 $5');
  return p;
}
function fmtCA(ca) {
  if (!ca) return '';
  const n = parseInt(String(ca).replace(/\s/g, ''), 10);
  if (isNaN(n) || n <= 0) return '';
  if (n >= 1000000) return (n / 1000000).toFixed(1).replace('.0', '') + ' M\u20ac';
  if (n >= 1000) return Math.round(n / 1000) + ' K\u20ac';
  return n + ' \u20ac';
}
function fmtEffectif(e) {
  if (!e || e === 'NN' || e === '00' || e === 'NR' || e === '0') return '';
  return e;
}

// --- Build cards HTML ---
function buildCard(r) {
  const name = esc(r.nom_entreprise || r.nom_commercial || 'Sans nom');
  const city = esc(r.ville || '');
  const cp = esc(r.code_postal || '');
  const score = parseInt(r.score_completude) || 0;

  // Score color
  let scoreColor = '#e74c3c';
  if (score >= 80) scoreColor = '#00b894';
  else if (score >= 60) scoreColor = '#4fc3f7';
  else if (score >= 40) scoreColor = '#f39c12';

  // Contact block
  let contactHTML = '';

  const phone = r.telephone;
  const mobile = r.dirigeant_telephone;
  const emailGen = r.email_generique;
  const emailDir = r.email_dirigeant;
  const site = r.site_web;
  const webEmails = r.website_emails;
  const webMobiles = r.website_mobiles;
  const webFixes = r.website_fixes;

  if (phone) {
    contactHTML += `<div class="contact-row">\u{1F4DE} <span class="contact-value">${esc(fmtPhone(phone))}</span></div>`;
  }
  if (webFixes && webFixes !== phone) {
    const fixes = webFixes.split('|').filter(f => f && f !== phone).slice(0, 2);
    fixes.forEach(f => {
      contactHTML += `<div class="contact-row">\u{1F4DE} <span class="contact-value">${esc(fmtPhone(f.trim()))}</span> <span class="contact-tag">site</span></div>`;
    });
  }

  const isMobile = (s) => s && /^0[67]/.test(s.trim());

  if (mobile) {
    const cls = isMobile(mobile) ? ' mobile-highlight' : '';
    contactHTML += `<div class="contact-row${cls}">\u{1F4F1} <span class="contact-value">${esc(fmtPhone(mobile))}</span> <span class="contact-tag">dirigeant</span></div>`;
  }
  if (webMobiles) {
    const mobs = webMobiles.split('|').filter(m => m && m !== mobile).slice(0, 2);
    mobs.forEach(m => {
      const cls = isMobile(m) ? ' mobile-highlight' : '';
      contactHTML += `<div class="contact-row${cls}">\u{1F4F1} <span class="contact-value">${esc(fmtPhone(m.trim()))}</span> <span class="contact-tag">site</span></div>`;
    });
  }

  if (emailGen) {
    contactHTML += `<div class="contact-row">\u{1F4E7} <span class="contact-value">${esc(emailGen)}</span></div>`;
  }
  if (emailDir && emailDir !== emailGen) {
    contactHTML += `<div class="contact-row">\u{1F4E7} <span class="contact-value">${esc(emailDir)}</span> <span class="contact-tag">dirigeant</span></div>`;
  }
  if (webEmails) {
    const emails = webEmails.split('|').filter(e => e && e !== emailGen && e !== emailDir).slice(0, 2);
    emails.forEach(e => {
      contactHTML += `<div class="contact-row">\u{1F4E7} <span class="contact-value">${esc(e.trim())}</span> <span class="contact-tag">site</span></div>`;
    });
  }
  if (site) {
    contactHTML += `<div class="contact-row">\u{1F310} <a href="${esc(site.startsWith('http') ? site : 'https://' + site)}" class="contact-link" target="_blank">${esc(site.replace(/^https?:\/\//, '').replace(/\/$/, ''))}</a></div>`;
  }

  // Dirigeant
  let dirigeantHTML = '';
  const dirName = r.dirigeant_nom_complet || [r.dirigeant_prenom, r.dirigeant_nom].filter(Boolean).join(' ');
  if (dirName) {
    const jobTitle = r.linkedin_job_title ? ` &mdash; ${esc(r.linkedin_job_title)}` : '';
    dirigeantHTML += `<div class="dir-name">\u{1F464} ${esc(dirName)}${jobTitle}</div>`;
  }
  if (r.linkedin_profil) {
    dirigeantHTML += `<div class="dir-linkedin"><a href="${esc(r.linkedin_profil)}" target="_blank" class="linkedin-link">LinkedIn \u2197</a></div>`;
  }

  // Metier — fallback sur code APE si pas de spécialité
  const APE_LABELS = {
    '4120A':'Construction de maisons','4120B':'Construction de bâtiments',
    '4211Z':'Routes et autoroutes','4221Z':'Réseaux pour fluides',
    '4222Z':'Réseaux électriques/télécoms','4291Z':'Ouvrages maritimes/fluviaux',
    '4299Z':'Autres travaux de construction','4311Z':'Travaux de démolition',
    '4312A':'Travaux de terrassement courants','4312B':'Travaux de terrassement spécialisés',
    '4313Z':'Forages et sondages','4321A':'Installation électrique',
    '4321B':'Travaux d\'isolation','4322A':'Plomberie, chauffage',
    '4322B':'Climatisation, ventilation','4329A':'Travaux d\'isolation',
    '4329B':'Autres travaux d\'installation','4331Z':'Travaux de plâtrerie',
    '4332A':'Menuiserie bois','4332B':'Menuiserie métallique, serrurerie',
    '4332C':'Agencement de lieux de vente','4333Z':'Revêtement des sols et murs',
    '4334Z':'Peinture, vitrerie','4339Z':'Autres travaux de finition',
    '4391A':'Travaux de charpente','4391B':'Travaux de couverture',
    '4399A':'Travaux d\'étanchéité','4399B':'Travaux de montage de structures',
    '4399C':'Travaux de maçonnerie générale','4399D':'Autres travaux spécialisés',
    '4399E':'Location avec opérateur de matériel',
    '7111Z':'Activités d\'architecture','7112B':'Ingénierie, études techniques',
  };
  let metierHTML = '';
  const spec = r.specialite || r.activites_principales;
  if (spec) {
    const display = spec.split('|').slice(0, 4).map(a => esc(a.trim())).join(', ');
    metierHTML += `<div class="metier-spec">\u{1F3D7}\u{FE0F} ${display}</div>`;
  } else if (r.code_ape && APE_LABELS[r.code_ape]) {
    metierHTML += `<div class="metier-spec">\u{1F3D7}\u{FE0F} ${esc(APE_LABELS[r.code_ape])}</div>`;
  } else if (r.code_ape) {
    metierHTML += `<div class="metier-spec">\u{1F3D7}\u{FE0F} Code APE : ${esc(r.code_ape)}</div>`;
  }
  if (r.specialite && r.activites_principales) {
    const acts = r.activites_principales.split('|').slice(0, 3).map(a => esc(a.trim())).join(', ');
    metierHTML += `<div class="metier-acts">${acts}</div>`;
  }

  // Certifications badges
  let badges = '';
  if (r.is_rge === 'oui' || r.is_rge === 'Oui' || r.is_rge === '1' || r.is_rge === 'true') {
    badges += `<span class="badge badge-rge">RGE</span>`;
  }
  if (r.qualibat && r.qualibat !== '0' && r.qualibat.toLowerCase() !== 'non') {
    badges += `<span class="badge badge-blue">Qualibat</span>`;
  }
  if (r.qualienr && r.qualienr !== '0' && r.qualienr.toLowerCase() !== 'non') {
    badges += `<span class="badge badge-teal">QualiENR</span>`;
  }
  if (r.qualifelec && r.qualifelec !== '0' && r.qualifelec.toLowerCase() !== 'non') {
    badges += `<span class="badge badge-orange">Qualifelec</span>`;
  }

  // Assurance
  let assuranceHTML = '';
  const assureur = r.assurance_rc || r.assurance_dc || '';
  if (assureur) {
    assuranceHTML = `<div class="card-assurance">\u{1F6E1}\u{FE0F} Assuré : ${esc(assureur)}</div>`;
  } else {
    assuranceHTML = `<div class="card-no-assurance">\u{26A0}\u{FE0F} Pas d'assurance identifiée</div>`;
  }

  // Financier
  let finHTML = '';
  const finParts = [];
  const caFmt = fmtCA(r.chiffre_affaires);
  if (caFmt) finParts.push(`CA: ${caFmt}`);
  const effFmt = fmtEffectif(r.api_tranche_effectif);
  if (effFmt) finParts.push(`Effectif: ${esc(effFmt)}`);
  else if (fmtEffectif(r.nb_salaries)) finParts.push(`Salariés: ${esc(fmtEffectif(r.nb_salaries))}`);
  if (r.api_categorie_entreprise) finParts.push(esc(r.api_categorie_entreprise));
  if (finParts.length) {
    finHTML = `<div class="card-financier">${finParts.join(' · ')}</div>`;
  }

  return `
    <div class="fiche-card">
      <div class="fiche-header">
        <div class="fiche-name">${name}</div>
        <div class="score-ring" style="background: conic-gradient(${scoreColor} ${score * 3.6}deg, #2a2a4a ${score * 3.6}deg);">
          <span>${score}%</span>
        </div>
      </div>
      <div class="fiche-location">${city}${cp ? ' (' + cp + ')' : ''}</div>
      ${contactHTML ? `<div class="contact-block">${contactHTML}</div>` : ''}
      ${dirigeantHTML ? `<div class="dirigeant-block">${dirigeantHTML}</div>` : ''}
      ${metierHTML ? `<div class="metier-block">${metierHTML}</div>` : ''}
      ${badges ? `<div class="badges-block">${badges}</div>` : ''}
      ${assuranceHTML}
      ${finHTML}
    </div>`;
}

const cardsHTML = sample.map(buildCard).join('\n');

// --- Full HTML ---
const html = `<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Base Artisans BTP — Département ${esc(dept)}</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: #0d0d1a; color: #e0e0e0; line-height: 1.6; }

  /* Hero */
  .hero {
    background: linear-gradient(135deg, #1a1a2e, #16213e);
    color: white; padding: 50px 40px 40px; text-align: center;
  }
  .hero h1 { font-size: 2.4em; font-weight: 800; letter-spacing: -1px; margin-bottom: 8px; }
  .hero h1 span { color: #4fc3f7; }
  .hero .subtitle { font-size: 1.05em; color: #90a4ae; margin-bottom: 6px; }
  .hero .sample-note { font-size: 0.88em; color: #607d8b; margin-top: 12px; }

  /* Grid */
  .grid-container { max-width: 1400px; margin: 0 auto; padding: 30px 20px; }
  .grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 18px;
  }
  @media (max-width: 1100px) { .grid { grid-template-columns: repeat(2, 1fr); } }
  @media (max-width: 700px) { .grid { grid-template-columns: 1fr; } }

  /* Cards */
  .fiche-card {
    background: #151528;
    border: 1px solid #2a2a4a;
    border-radius: 14px;
    padding: 22px;
    transition: border-color 0.2s, box-shadow 0.2s;
  }
  .fiche-card:hover {
    border-color: #4fc3f7;
    box-shadow: 0 4px 24px rgba(79,195,247,0.1);
  }

  .fiche-header {
    display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 6px;
  }
  .fiche-name {
    font-size: 1.12em; font-weight: 800; color: #ffffff; line-height: 1.3; flex: 1; margin-right: 10px;
  }
  .fiche-location {
    font-size: 0.85em; color: #78909c; margin-bottom: 14px;
  }

  /* Score ring */
  .score-ring {
    width: 42px; height: 42px; border-radius: 50%; flex-shrink: 0;
    display: flex; align-items: center; justify-content: center;
    position: relative;
  }
  .score-ring span {
    background: #151528; width: 32px; height: 32px; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 0.68em; font-weight: 800; color: #b0bec5;
  }

  /* Contact block — THE STAR */
  .contact-block {
    background: #1a1a35;
    border: 1px solid #2e2e50;
    border-radius: 10px;
    padding: 12px 14px;
    margin-bottom: 12px;
  }
  .contact-row {
    display: flex; align-items: center; gap: 8px;
    padding: 5px 0; font-size: 0.92em; color: #cfd8dc;
  }
  .contact-value { font-weight: 600; color: #e8e8e8; font-size: 1.02em; }
  .contact-link { color: #4fc3f7; text-decoration: none; font-weight: 600; }
  .contact-link:hover { text-decoration: underline; }
  .contact-tag {
    font-size: 0.68em; background: #2a2a4a; color: #90a4ae; padding: 1px 7px;
    border-radius: 8px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;
  }
  .mobile-highlight {
    background: rgba(0,184,148,0.1); border-radius: 6px; padding: 5px 8px; margin: 2px -8px;
  }
  .mobile-highlight .contact-value { color: #00e6b8; }

  /* Dirigeant */
  .dirigeant-block { margin-bottom: 10px; }
  .dir-name { font-size: 0.9em; color: #b0bec5; font-weight: 600; }
  .dir-linkedin { margin-top: 3px; }
  .linkedin-link { color: #4fc3f7; font-size: 0.82em; text-decoration: none; font-weight: 600; }
  .linkedin-link:hover { text-decoration: underline; }

  /* Metier */
  .metier-block { margin-bottom: 10px; }
  .metier-spec { font-size: 0.88em; color: #90a4ae; font-weight: 600; margin-bottom: 2px; }
  .metier-acts { font-size: 0.8em; color: #607d8b; }

  /* Badges */
  .badges-block { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px; }
  .badge {
    display: inline-block; padding: 3px 10px; border-radius: 12px;
    font-size: 0.75em; font-weight: 700; letter-spacing: 0.3px;
  }
  .badge-rge { background: linear-gradient(135deg, #00b894, #00cec9); color: white; }
  .badge-blue { background: #1a3a5c; color: #4fc3f7; }
  .badge-teal { background: #0d3d3d; color: #00cec9; }
  .badge-orange { background: #3d2a0d; color: #f5a623; }

  /* Assurance & Financier */
  .card-assurance { display: inline-block; font-size: 0.85em; font-weight: 600; color: #fff; background: linear-gradient(135deg, #6a1b9a, #8e24aa); padding: 5px 12px; border-radius: 8px; margin-bottom: 8px; }
  .card-no-assurance { display: inline-block; font-size: 0.82em; color: #ef5350; background: rgba(239,83,80,0.12); padding: 4px 10px; border-radius: 8px; margin-bottom: 8px; }
  .card-financier { font-size: 0.78em; color: #546e7a; border-top: 1px solid #2a2a4a; padding-top: 8px; margin-top: 6px; }

  /* Footer */
  .footer {
    text-align: center; padding: 40px 20px; color: #546e7a; font-size: 0.9em;
    border-top: 1px solid #1a1a35;
  }
  .footer a { color: #4fc3f7; text-decoration: none; }
  .footer a:hover { text-decoration: underline; }
  .footer .email { font-size: 1.1em; font-weight: 700; color: #78909c; margin-top: 12px; }
</style>
</head>
<body>

<div class="hero">
  <h1>Base Artisans BTP &mdash; Département <span>${esc(dept)}</span></h1>
  <p class="subtitle">${total.toLocaleString('fr-FR')} artisans | ${pctMobile}% avec mobile | ${pctRGE}% RGE | ${pctAssurance}% assurance identifiée</p>
  <p class="sample-note">Échantillon de ${sample.length} fiches représentatives sur ${total.toLocaleString('fr-FR')}</p>
</div>

<div class="grid-container">
  <div class="grid">
    ${cardsHTML}
  </div>
</div>

<div class="footer">
  <p>Cet échantillon contient ${sample.length} fiches sur ${total.toLocaleString('fr-FR')} dans le département ${esc(dept)}.</p>
  <p class="email"><a href="mailto:paul@growth-factory.fr">paul@growth-factory.fr</a></p>
</div>

</body>
</html>`;

// --- Write HTML ---
const outFile = path.join(__dirname, `demo-${dept}${fileSuffix}.html`);
fs.writeFileSync(outFile, html, 'utf-8');
console.log(`\nGenerated: ${outFile}`);

// --- Write matching Excel (same sample) ---
const XLSX = require('xlsx');
const APE_LABELS = {
  '4120A':'Construction de maisons','4120B':'Construction de bâtiments',
  '4321A':'Installation électrique','4321B':'Travaux d\'isolation',
  '4322A':'Plomberie, chauffage','4322B':'Climatisation, ventilation',
  '4329A':'Travaux d\'isolation','4329B':'Autres travaux d\'installation',
  '4331Z':'Travaux de plâtrerie','4332A':'Menuiserie bois',
  '4332B':'Menuiserie métallique, serrurerie','4333Z':'Revêtement des sols et murs',
  '4334Z':'Peinture, vitrerie','4339Z':'Autres travaux de finition',
  '4391A':'Travaux de charpente','4391B':'Travaux de couverture',
  '4399A':'Travaux d\'étanchéité','4399C':'Travaux de maçonnerie générale',
};
const EXCEL_COLS = [
  ['nom_entreprise','Entreprise'],['siret','SIRET'],['forme_juridique','Forme juridique'],
  ['code_ape','Code APE'],['_metier','Métier'],['date_creation','Date création'],
  ['telephone','Téléphone'],['email_generique','Email entreprise'],['site_web','Site web'],
  ['dirigeant_nom_complet','Dirigeant'],['dirigeant_prenom','Prénom'],['dirigeant_nom','Nom'],
  ['dirigeant_telephone','Mobile dirigeant'],['email_dirigeant','Email dirigeant'],
  ['linkedin_job_title','Fonction'],['linkedin_profil','LinkedIn'],
  ['adresse','Adresse'],['code_postal','CP'],['ville','Ville'],
  ['specialite','Spécialité'],['activites_principales','Activités principales'],
  ['activites_secondaires','Activités secondaires'],
  ['is_rge','RGE'],['qualibat','Qualibat'],['qualibat_type','Qualibat détail'],
  ['qualienr','QualiENR'],['qualienr_certifications','Certifs ENR'],['qualifelec','Qualifelec'],
  ['_assureur','Assureur'],
  ['chiffre_affaires','CA'],['nb_salaries','Effectif'],
  ['api_tranche_effectif','Tranche effectif'],['api_categorie_entreprise','Catégorie'],
  ['solvabilite','Solvabilité'],
];
const wb = XLSX.utils.book_new();
const pct = (n) => Math.round(n / total * 100) + '%';
const withEmail = deptRows.filter(r => r.email_generique || r.email_dirigeant).length;
const summaryData = [
  ['Base Artisans BTP — Département ' + dept],[],
  ['Statistiques du département (base complète)'],
  ['Total fiches', total],
  ['Avec email', withEmail, pct(withEmail)],
  ['Avec mobile (06/07)', hasMobile, pct(hasMobile)],
  ['RGE confirmé', hasRGE, pct(hasRGE)],
  ['Assurance identifiée', hasAssurance, pct(hasAssurance)],
  [],
  [`Cet échantillon contient ${sample.length} fiches représentatives sur ${total}.`],
  ['La base complète est disponible sur demande.'],[],
  ['Contact : paul@growth-factory.fr'],
];
XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(summaryData), 'Résumé');

const sheetData = [EXCEL_COLS.map(c => c[1])];
sample.forEach(r => {
  sheetData.push(EXCEL_COLS.map(([k]) => {
    if (k === '_assureur') return (r.assurance_rc || r.assurance_dc || '').trim();
    if (k === '_metier') return (r.specialite || r.activites_principales || APE_LABELS[r.code_ape] || r.code_ape || '').trim();
    const v = (r[k] || '').trim();
    if (k === 'score_completude') return parseFloat(v) || 0;
    if (k === 'nb_sources') return parseInt(v) || 0;
    return v;
  }));
});
const ws = XLSX.utils.aoa_to_sheet(sheetData);
ws['!cols'] = EXCEL_COLS.map(([,h]) => ({ wch: Math.max(h.length + 4, 14) }));
XLSX.utils.book_append_sheet(wb, ws, `Échantillon ${dept}`);

const xlsxFile = path.join(__dirname, `export_dept_${dept}${fileSuffix}_${sample.length}fiches.xlsx`);
XLSX.writeFile(wb, xlsxFile);
console.log(`Generated: ${xlsxFile}`);
console.log(`Stats: ${total} artisans dept ${dept} | ${pctMobile}% mobile | ${pctRGE}% RGE | ${pctAssurance}% assurance | ${sample.length} fiches exportées`);
