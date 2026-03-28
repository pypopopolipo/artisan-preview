// Export échantillon par département — usage : node generate_export_excel_light.js 30 50
const fs = require('fs');
const XLSX = require('xlsx');

const DEPT = process.argv[2];
const SAMPLE_SIZE = parseInt(process.argv[3]) || 50;
if (!DEPT) { console.error('Usage: node generate_export_excel_light.js <dept> [nb_fiches]'); process.exit(1); }

const COLS = [
  // === IDENTITÉ ===
  ['nom_entreprise',       'Entreprise'],
  ['siret',                'SIRET'],
  ['forme_juridique',      'Forme juridique'],
  ['code_ape',             'Code APE'],
  ['api_libelle_naf',      'Activité NAF'],
  ['date_creation',        'Date création'],
  // === CONTACT ===
  ['telephone',            'Téléphone'],
  ['email_generique',      'Email entreprise'],
  ['site_web',             'Site web'],
  // === DIRIGEANT ===
  ['dirigeant_nom_complet','Dirigeant'],
  ['dirigeant_prenom',     'Prénom'],
  ['dirigeant_nom',        'Nom'],
  ['dirigeant_telephone',  'Mobile dirigeant'],
  ['email_dirigeant',      'Email dirigeant'],
  ['linkedin_job_title',   'Fonction (LinkedIn)'],
  ['linkedin_profil',      'Profil LinkedIn'],
  // === LOCALISATION ===
  ['adresse',              'Adresse'],
  ['code_postal',          'CP'],
  ['ville',                'Ville'],
  // === MÉTIER ===
  ['specialite',           'Spécialité'],
  ['activites_principales','Activités principales'],
  ['activites_secondaires','Activités secondaires'],
  // === CERTIFICATIONS ===
  ['is_rge',               'RGE'],
  ['qualibat',             'Qualibat'],
  ['qualibat_type',        'Qualibat détail'],
  ['qualienr',             'QualiENR'],
  ['qualienr_certifications','Certifs ENR (PAC, solaire…)'],
  ['qualifelec',           'Qualifelec'],
  // === ASSURANCE ===
  ['_assureur',            'Assureur'],
  // === FINANCIER ===
  ['chiffre_affaires',     'CA'],
  ['nb_salaries',          'Effectif déclaré'],
  ['api_tranche_effectif', 'Tranche effectif INSEE'],
  ['api_categorie_entreprise','Catégorie (TPE/PME/ETI)'],
  ['solvabilite',          'Solvabilité'],
];

console.log(`Filtre: dept ${DEPT}, ${SAMPLE_SIZE} fiches`);
const raw = fs.readFileSync('artisans_unified.csv', 'utf-8').replace(/^\uFEFF/, '');
const lines = raw.split('\n');
const header = lines[0].split(',');

function parseRow(line) {
  const row = {}; let inQ = false, cur = '', col = 0;
  for (const c of line) {
    if (c === '"') { inQ = !inQ; continue; }
    if (c === ',' && !inQ) { row[header[col++]] = cur; cur = ''; continue; }
    cur += c;
  }
  row[header[col]] = cur;
  return row;
}

// Filtrer par département
const deptRows = [];
for (let i = 1; i < lines.length; i++) {
  if (!lines[i].trim()) continue;
  const r = parseRow(lines[i]);
  const cp = (r.code_postal || '').trim();
  if (cp.startsWith(DEPT)) deptRows.push(r);
}
console.log(`${deptRows.length} fiches dans le ${DEPT}`);
if (deptRows.length === 0) { console.error('Aucune fiche.'); process.exit(1); }

// Stats du dept complet
const withEmail = deptRows.filter(r => r.email_generique || r.email_dirigeant).length;
const withMobile = deptRows.filter(r => {
  const t = (r.dirigeant_telephone || '').replace(/[\s.]/g, '');
  const t2 = (r.telephone || '').replace(/[\s.]/g, '');
  return /^0[67]/.test(t) || /^0[67]/.test(t2);
}).length;
const withRge = deptRows.filter(r => r.is_rge === 'oui').length;
const withRc = deptRows.filter(r => r.assurance_rc || r.assurance_dc).length;
const avgScore = Math.round(deptRows.reduce((s, r) => s + parseFloat(r.score_completude || 0), 0) / deptRows.length);

// Échantillon représentatif : uniquement fiches avec au moins 1 contact exploitable
const contactRows = deptRows.filter(r => {
  const hasEmail = !!(r.email_generique || r.email_dirigeant);
  const hasTel = !!(r.telephone || r.dirigeant_telephone);
  return hasEmail || hasTel;
});
console.log(`Fiches avec contact: ${contactRows.length}/${deptRows.length} (${Math.round(contactRows.length/deptRows.length*100)}%)`);

// Tri par score, puis 1 sur N parmi les fiches avec contact
contactRows.sort((a, b) => parseFloat(b.score_completude || 0) - parseFloat(a.score_completude || 0));
const pool = SAMPLE_SIZE >= contactRows.length ? contactRows : contactRows;
const step = Math.max(1, Math.floor(pool.length / SAMPLE_SIZE));
const sample = [];
for (let i = 0; i < pool.length && sample.length < SAMPLE_SIZE; i += step) {
  sample.push(pool[i]);
}
console.log(`Échantillon: ${sample.length} fiches (1 sur ${step})`);

// Scores de l'échantillon pour vérifier la représentativité
const sampleScores = sample.map(r => parseFloat(r.score_completude || 0));
console.log(`  Score min: ${Math.min(...sampleScores)}%, max: ${Math.max(...sampleScores)}%, moy: ${Math.round(sampleScores.reduce((a,b)=>a+b,0)/sampleScores.length)}%`);

// Construction Excel
const wb = XLSX.utils.book_new();
const colKeys = COLS.map(c => c[0]);
const colHeaders = COLS.map(c => c[1]);

// Onglet 1 : Résumé
const pct = (n) => Math.round(n / deptRows.length * 100) + '%';
const summaryData = [
  ['Base Artisans BTP — Département ' + DEPT],
  [],
  ['Statistiques du département (base complète)'],
  ['Total fiches', deptRows.length],
  ['Avec email', withEmail, pct(withEmail)],
  ['Avec mobile (06/07)', withMobile, pct(withMobile)],
  ['RGE confirmé', withRge, pct(withRge)],
  ['Assurance identifiée', withRc, pct(withRc)],
  ['Score complétude moyen', avgScore + '%'],
  [],
  [`Cet échantillon contient ${sample.length} fiches représentatives sur ${deptRows.length}.`],
  ['La base complète est disponible sur demande.'],
  [],
  ['Contact : paul@growth-factory.fr'],
];
const wsSummary = XLSX.utils.aoa_to_sheet(summaryData);
wsSummary['!cols'] = [{wch: 40}, {wch: 10}, {wch: 8}];
XLSX.utils.book_append_sheet(wb, wsSummary, 'Résumé');

// Onglet 2 : Échantillon
const sheetData = [colHeaders];
sample.forEach(r => {
  sheetData.push(colKeys.map(k => {
    // Champ virtuel : merge RC + DC en un seul "Assureur"
    if (k === '_assureur') {
      return (r.assurance_rc || r.assurance_dc || '').trim();
    }
    const v = (r[k] || '').trim();
    if (!v) return '';
    if (k === 'score_completude') return parseFloat(v) || 0;
    if (k === 'nb_sources') return parseInt(v) || 0;
    return v;
  }));
});
const ws = XLSX.utils.aoa_to_sheet(sheetData);
ws['!cols'] = colHeaders.map(h => ({ wch: Math.max(h.length + 4, 14) }));
XLSX.utils.book_append_sheet(wb, ws, `Échantillon ${DEPT}`);

// Export
const outFile = `export_dept_${DEPT}_${sample.length}fiches.xlsx`;
XLSX.writeFile(wb, outFile);
const size = Math.round(fs.statSync(outFile).size / 1024);
console.log(`\n=> ${outFile} (${size} KB)`);
