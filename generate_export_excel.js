const fs = require('fs');
const XLSX = require('xlsx');

const REGION_NAMES = {
  "84": "Auvergne-Rhône-Alpes", "27": "Bourgogne-Franche-Comté", "53": "Bretagne",
  "24": "Centre-Val de Loire", "94": "Corse", "44": "Grand Est", "32": "Hauts-de-France",
  "11": "Île-de-France", "28": "Normandie", "75": "Nouvelle-Aquitaine",
  "76": "Occitanie", "52": "Pays de la Loire", "93": "PACA",
  "01": "Guadeloupe", "02": "Martinique", "03": "Guyane", "04": "La Réunion", "06": "Mayotte",
};

// Colonnes export (ordre logique, headers français)
const COLS = [
  ['nom_entreprise',       'Entreprise'],
  ['siret',                'SIRET'],
  ['siren',                'SIREN'],
  ['forme_juridique',      'Forme juridique'],
  ['code_ape',             'Code APE'],
  ['date_creation',        'Date création'],
  ['telephone',            'Téléphone'],
  ['email_generique',      'Email entreprise'],
  ['email_dirigeant',      'Email dirigeant'],
  ['site_web',             'Site web'],
  ['adresse',              'Adresse'],
  ['code_postal',          'Code postal'],
  ['ville',                'Ville'],
  ['dirigeant_nom_complet','Dirigeant'],
  ['dirigeant_telephone',  'Mobile dirigeant'],
  ['specialite',           'Spécialité'],
  ['is_rge',               'RGE'],
  ['qualibat',             'Qualibat'],
  ['qualienr',             'QualiENR'],
  ['qualifelec',           'Qualifelec'],
  ['assurance_rc',         'Assurance RC'],
  ['assurance_dc',         'Assurance DC'],
  ['chiffre_affaires',     'Chiffre d\'affaires'],
  ['nb_salaries',          'Effectif'],
  ['solvabilite',          'Solvabilité'],
  ['linkedin',             'LinkedIn'],
  ['sources',              'Sources'],
  ['nb_sources',           'Nb sources'],
  ['score_completude',     'Complétude (%)'],
];

console.log('Lecture artisans_unified.csv...');
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

// Grouper par région
const byRegion = {};
let totalRows = 0;
for (let i = 1; i < lines.length; i++) {
  if (!lines[i].trim()) continue;
  const r = parseRow(lines[i]);
  const code = (r.api_region || '').trim();
  const regName = REGION_NAMES[code] || 'Non classé';
  if (!byRegion[regName]) byRegion[regName] = [];
  byRegion[regName].push(r);
  totalRows++;
  if (i % 10000 === 0) process.stdout.write('\r  ' + i + ' / ' + (lines.length-1));
}
console.log('\r  ' + totalRows + ' lignes lues.   ');

// Construire le workbook
const wb = XLSX.utils.book_new();

// === Onglet RÉSUMÉ ===
const summaryData = [
  ['Région', 'Total', 'Emails', 'Téléphones', 'Mobiles dirigeants', 'RGE', 'Assurance RC', 'Score moyen'],
];
const regionOrder = Object.keys(REGION_NAMES).map(k => REGION_NAMES[k]).filter(n => byRegion[n]);
if (byRegion['Non classé']) regionOrder.push('Non classé');

let grandTotal = 0, grandEmail = 0, grandTel = 0, grandMobile = 0, grandRge = 0, grandRc = 0, grandScore = 0;
regionOrder.forEach(reg => {
  const rows = byRegion[reg] || [];
  const emails = rows.filter(r => r.email_generique || r.email_dirigeant).length;
  const tels = rows.filter(r => r.telephone).length;
  const mobiles = rows.filter(r => r.dirigeant_telephone && /^0[67]/.test(r.dirigeant_telephone.replace(/[\s.]/g,''))).length;
  const rge = rows.filter(r => r.is_rge === 'oui').length;
  const rc = rows.filter(r => r.assurance_rc).length;
  const score = rows.length ? Math.round(rows.reduce((s,r) => s + parseFloat(r.score_completude||0), 0) / rows.length) : 0;
  summaryData.push([reg, rows.length, emails, tels, mobiles, rge, rc, score + '%']);
  grandTotal += rows.length; grandEmail += emails; grandTel += tels;
  grandMobile += mobiles; grandRge += rge; grandRc += rc; grandScore += score * rows.length;
});
summaryData.push(['TOTAL', grandTotal, grandEmail, grandTel, grandMobile, grandRge, grandRc,
  Math.round(grandScore / grandTotal) + '%']);

const wsSummary = XLSX.utils.aoa_to_sheet(summaryData);
wsSummary['!cols'] = [24,8,8,12,18,8,12,12].map(w => ({wch:w}));
XLSX.utils.book_append_sheet(wb, wsSummary, 'Résumé');

// === Un onglet par région ===
const colKeys = COLS.map(c => c[0]);
const colHeaders = COLS.map(c => c[1]);

regionOrder.forEach(reg => {
  const rows = byRegion[reg] || [];
  console.log('  Onglet:', reg, '(' + rows.length + ' lignes)');

  const sheetData = [colHeaders];
  rows.forEach(r => {
    sheetData.push(colKeys.map(k => {
      const v = (r[k] || '').trim();
      if (!v) return '';
      // Convertir score en nombre
      if (k === 'score_completude') return parseFloat(v) || 0;
      // Convertir nb_sources en nombre
      if (k === 'nb_sources') return parseInt(v) || 0;
      return v;
    }));
  });

  const ws = XLSX.utils.aoa_to_sheet(sheetData);
  // Largeurs colonnes
  const widths = [28,16,12,16,10,12,14,28,28,30,35,10,20,25,14,22,5,10,10,10,20,20,16,10,12,35,30,8,10];
  ws['!cols'] = widths.map(w => ({wch:w}));

  // Nom d'onglet max 31 chars (limite Excel)
  const sheetName = reg.substring(0, 31);
  XLSX.utils.book_append_sheet(wb, ws, sheetName);
});

// === Export ===
const outFile = 'artisans_unified.xlsx';
console.log('Écriture', outFile, '...');
XLSX.writeFile(wb, outFile);
const size = Math.round(fs.statSync(outFile).size / 1024 / 1024 * 10) / 10;
console.log('Done:', outFile, '(' + size + ' MB)');
console.log(regionOrder.length, 'onglets + 1 résumé');
