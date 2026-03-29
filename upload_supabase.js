/**
 * Upload artisans to Supabase for a specific department & client.
 *
 * Usage:
 *   node upload_supabase.js --dept 30 --client-name "Rémi Martin" --client-email "agence.mksnimes@axa.fr" --client-code MARTIN30
 */

const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

// ── Supabase config ─────────────────────────────────────────────────
require('dotenv').config();
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://gzqtlfnucxyklxawwbxo.supabase.co';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const BATCH_SIZE = 500;

// ── CLI args ────────────────────────────────────────────────────────
function parseArgs() {
  const args = process.argv.slice(2);
  const opts = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--dept' && args[i + 1]) opts.dept = args[++i];
    else if (args[i] === '--client-name' && args[i + 1]) opts.clientName = args[++i];
    else if (args[i] === '--client-email' && args[i + 1]) opts.clientEmail = args[++i];
    else if (args[i] === '--client-code' && args[i + 1]) opts.clientCode = args[++i];
  }
  if (!opts.dept || !opts.clientName || !opts.clientEmail) {
    console.error('Usage: node upload_supabase.js --dept 30 --client-name "Rémi Martin" --client-email "agence@example.fr" [--client-code optionnel]');
    process.exit(1);
  }
  if (!opts.clientCode) {
    opts.clientCode = require('crypto').randomBytes(5).toString('hex');
    console.log('Code auto-généré:', opts.clientCode);
  }
  return opts;
}

// ── Minimal RFC 4180 CSV parser (from audit_quality.js) ─────────────
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

// ── Helpers ──────────────────────────────────────────────────────────
function v(row, col, headers) {
  const idx = headers.indexOf(col);
  if (idx === -1) return '';
  return (row[idx] || '').trim();
}

function hasContact(row, headers) {
  const contactFields = [
    'telephone', 'email_generique', 'email_dirigeant',
    'dirigeant_telephone', 'website_emails', 'website_mobiles', 'website_fixes'
  ];
  return contactFields.some(f => v(row, f, headers) !== '');
}

function buildFiche(row, headers, clientId) {
  const g = (csvCol) => v(row, csvCol, headers) || null;

  return {
    client_id: clientId,
    nom_entreprise: g('nom_entreprise'),
    siret: g('siret'),
    forme_juridique: g('forme_juridique'),
    code_ape: g('code_ape'),
    activite_naf: g('api_libelle_naf') || g('api_naf'),
    date_creation: g('api_date_creation') || g('date_creation') || null,
    telephone: g('telephone'),
    email_generique: g('email_generique'),
    site_web: g('site_web'),
    website_emails: g('website_emails'),
    website_mobiles: g('website_mobiles'),
    website_fixes: g('website_fixes'),
    dirigeant_nom_complet: g('dirigeant_nom_complet'),
    dirigeant_prenom: g('dirigeant_prenom'),
    dirigeant_nom: g('dirigeant_nom'),
    dirigeant_telephone: g('dirigeant_telephone'),
    email_dirigeant: g('email_dirigeant'),
    linkedin_fonction: g('linkedin_job_title'),
    linkedin_profil: g('linkedin_profil'),
    adresse: g('adresse'),
    code_postal: g('code_postal'),
    ville: g('ville'),
    specialite: g('specialite'),
    activites_principales: g('activites_principales'),
    activites_secondaires: g('activites_secondaires'),
    is_rge: g('is_rge'),
    qualibat: g('qualibat'),
    qualibat_detail: g('qualibat_type'),
    qualienr: g('qualienr'),
    certifs_enr: g('qualienr_certifications'),
    qualifelec: g('qualifelec'),
    assurance_rc: g('assurance_rc'),
    assurance_dc: g('assurance_dc'),
    assureur: g('qualibat_assurance'),
    chiffre_affaires: g('chiffre_affaires'),
    effectif: g('nb_salaries'),
    tranche_effectif: g('api_tranche_effectif'),
    categorie_entreprise: g('api_categorie_entreprise'),
    solvabilite: g('solvabilite'),
    sources: g('sources'),
    nb_sources: g('nb_sources') ? parseInt(g('nb_sources'), 10) : null,
    score_completude: g('score_completude') ? parseFloat(g('score_completude')) : null,
    latitude: g('latitude') ? parseFloat(g('latitude')) : null,
    longitude: g('longitude') ? parseFloat(g('longitude')) : null,
    risque_impaye: g('risque_impaye'),
    nb_salaries: g('nb_salaries'),
    api_etat_administratif: g('api_etat_administratif'),
  };
}

// ── Main ─────────────────────────────────────────────────────────────
async function main() {
  const opts = parseArgs();
  console.log(`\n=== Upload Supabase — Dept ${opts.dept} — Client: ${opts.clientName} ===\n`);

  // 1. Load CSV
  console.log('Loading artisans_unified.csv...');
  let raw = fs.readFileSync(path.join(__dirname, 'artisans_unified.csv'), 'utf-8');
  if (raw.charCodeAt(0) === 0xFEFF) raw = raw.slice(1);

  const allRows = parseCSV(raw);
  const headers = allRows[0].map(h => h.trim());
  const data = allRows.slice(1);
  console.log(`Parsed ${data.length} rows, ${headers.length} columns`);

  // 2. Filter by department
  const deptPrefix = opts.dept.padStart(2, '0');
  const deptRows = data.filter(row => {
    const cp = v(row, 'code_postal', headers);
    return cp.startsWith(deptPrefix);
  });
  console.log(`Dept ${deptPrefix}: ${deptRows.length} fiches total`);

  // 3. Filter to fiches with at least 1 contact
  const contactRows = deptRows.filter(row => hasContact(row, headers));
  console.log(`With contact: ${contactRows.length} fiches`);

  if (contactRows.length === 0) {
    console.log('No fiches to upload. Exiting.');
    return;
  }

  // 4. Upsert client
  console.log(`\nCreating/finding client: ${opts.clientName} (${opts.clientEmail})...`);
  const { data: existingClients, error: findErr } = await supabase
    .from('clients')
    .select('id')
    .eq('email', opts.clientEmail)
    .limit(1);

  if (findErr) {
    console.error('Error finding client:', findErr.message);
    process.exit(1);
  }

  let clientId;
  if (existingClients && existingClients.length > 0) {
    clientId = existingClients[0].id;
    console.log(`Client already exists (id: ${clientId})`);

    // Update name/code if needed
    await supabase
      .from('clients')
      .update({ nom: opts.clientName, code_acces: opts.clientCode })
      .eq('id', clientId);
  } else {
    const { data: newClient, error: createErr } = await supabase
      .from('clients')
      .insert({
        nom: opts.clientName,
        email: opts.clientEmail,
        code_acces: opts.clientCode,
        departements: [deptPrefix],
      })
      .select('id')
      .single();

    if (createErr) {
      console.error('Error creating client:', createErr.message);
      process.exit(1);
    }
    clientId = newClient.id;
    console.log(`Client created (id: ${clientId})`);
  }

  // 5. Delete existing fiches for this client (idempotency)
  console.log('Deleting existing fiches for this client...');
  const { error: delErr, count: delCount } = await supabase
    .from('fiches')
    .delete({ count: 'exact' })
    .eq('client_id', clientId);

  if (delErr) {
    console.error('Error deleting existing fiches:', delErr.message);
    process.exit(1);
  }
  if (delCount > 0) {
    console.log(`Deleted ${delCount} existing fiches`);
  }

  // 6. Build fiches and upload in batches
  console.log(`\nUploading ${contactRows.length} fiches in batches of ${BATCH_SIZE}...`);
  let uploaded = 0;
  let errors = 0;

  for (let i = 0; i < contactRows.length; i += BATCH_SIZE) {
    const batch = contactRows.slice(i, i + BATCH_SIZE);
    const fiches = batch.map(row => buildFiche(row, headers, clientId));

    const { error: insertErr } = await supabase
      .from('fiches')
      .insert(fiches);

    if (insertErr) {
      console.error(`  Batch ${Math.floor(i / BATCH_SIZE) + 1} ERROR: ${insertErr.message}`);
      errors += batch.length;
    } else {
      uploaded += batch.length;
      const pct = ((i + batch.length) / contactRows.length * 100).toFixed(1);
      console.log(`  Batch ${Math.floor(i / BATCH_SIZE) + 1}: ${batch.length} fiches uploaded (${pct}%)`);
    }
  }

  // 7. Stats
  console.log('\n=== Upload Complete ===');
  console.log(`Total uploaded: ${uploaded}`);
  console.log(`Errors: ${errors}`);

  // Compute contact stats
  let withMobile = 0, withEmail = 0, withPhone = 0, withWebsiteEmail = 0, withDirigeant = 0;
  for (const row of contactRows) {
    if (v(row, 'website_mobiles', headers) || v(row, 'dirigeant_telephone', headers)) withMobile++;
    if (v(row, 'email_generique', headers) || v(row, 'email_dirigeant', headers)) withEmail++;
    if (v(row, 'telephone', headers)) withPhone++;
    if (v(row, 'website_emails', headers)) withWebsiteEmail++;
    if (v(row, 'dirigeant_nom_complet', headers)) withDirigeant++;
  }

  console.log(`\n--- Contact Stats ---`);
  console.log(`With mobile (website_mobiles or dirigeant_tel): ${withMobile} (${(withMobile / contactRows.length * 100).toFixed(1)}%)`);
  console.log(`With email (generique or dirigeant): ${withEmail} (${(withEmail / contactRows.length * 100).toFixed(1)}%)`);
  console.log(`With phone (telephone): ${withPhone} (${(withPhone / contactRows.length * 100).toFixed(1)}%)`);
  console.log(`With website email: ${withWebsiteEmail} (${(withWebsiteEmail / contactRows.length * 100).toFixed(1)}%)`);
  console.log(`With dirigeant: ${withDirigeant} (${(withDirigeant / contactRows.length * 100).toFixed(1)}%)`);
  console.log('');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
