const fs = require("fs");
const path = require("path");

const XLSX = require("xlsx");
const PROGRESS_FILE = path.join(__dirname, ".enrich_progress.json");

// Toutes les sources de SIRETs
const SOURCES = {
  capeb: path.join(__dirname, "artisans_capeb.csv"),
  qualibat: path.join(__dirname, "Base Qualibat.xlsx"),
  qualienr: path.join(__dirname, "Base QualiENR.xlsx"),
  qualifelec: path.join(__dirname, "Base Qualif'elec.xlsx"),
};

// API config
const API_BASE = "https://recherche-entreprises.api.gouv.fr/search";
const BATCH_SIZE = 5; // requêtes en parallèle (respecter l'API publique)
const DELAY_BETWEEN_BATCHES = 800; // ~5 req/s max
const SAVE_EVERY = 10; // sauvegarder tous les X batches
const MAX_CONSECUTIVE_ERRORS = 20; // stopper si l'API semble down

// Colonnes d'enrichissement qu'on va ajouter
const ENRICHMENT_COLS = [
  "api_nom_complet",
  "api_siren",
  "api_naf",
  "api_naf25",
  "api_libelle_naf",
  "api_categorie_entreprise",
  "api_nature_juridique",
  "api_date_creation",
  "api_etat_administratif",
  "api_tranche_effectif",
  "api_annee_effectif",
  "api_caractere_employeur",
  "api_date_fermeture",
  "api_adresse_complete",
  "api_code_postal",
  "api_commune",
  "api_departement",
  "api_region",
  "api_dirigeant_nom",
  "api_dirigeant_prenom",
  "api_dirigeant_qualite",
  "api_liste_idcc",
  "api_liste_rge",
  "api_section_activite",
];

// NAF Rev.2 section labels
const NAF_SECTIONS = {
  A: "Agriculture, sylviculture et pêche",
  B: "Industries extractives",
  C: "Industrie manufacturière",
  D: "Électricité, gaz, vapeur et air conditionné",
  E: "Eau, assainissement, gestion des déchets",
  F: "Construction",
  G: "Commerce, réparation d'automobiles",
  H: "Transports et entreposage",
  I: "Hébergement et restauration",
  J: "Information et communication",
  K: "Activités financières et d'assurance",
  L: "Activités immobilières",
  M: "Activités spécialisées, scientifiques et techniques",
  N: "Activités de services administratifs",
  O: "Administration publique",
  P: "Enseignement",
  Q: "Santé humaine et action sociale",
  R: "Arts, spectacles et activités récréatives",
  S: "Autres activités de services",
  T: "Activités des ménages",
  U: "Activités extra-territoriales",
};

// ---- CSV parsing (gère les guillemets) ----
function parseCSVLine(line) {
  const fields = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        current += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ",") {
        fields.push(current);
        current = "";
      } else {
        current += ch;
      }
    }
  }
  fields.push(current);
  return fields;
}

function escapeCSV(val) {
  if (val == null) return "";
  const s = String(val);
  if (s.includes(",") || s.includes('"') || s.includes("\n")) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

// ---- API call ----
async function fetchSiretData(siret, retries = 3) {
  try {
    const url = `${API_BASE}?q=${siret}&page=1&per_page=1`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    const resp = await fetch(url, { signal: controller.signal });
    clearTimeout(timeout);
    if (resp.status === 429) {
      if (retries > 0) {
        await sleep(3000 + Math.random() * 2000);
        return fetchSiretData(siret, retries - 1);
      }
      return { _error: "rate_limited" };
    }
    if (!resp.ok) return { _error: `http_${resp.status}` };
    const data = await resp.json();
    if (!data.results || data.results.length === 0) return { _not_found: true }; // vrai "pas trouvé"

    const r = data.results[0];
    const siege = r.siege || {};
    const dirigeant = (r.dirigeants && r.dirigeants[0]) || {};

    return {
      api_nom_complet: r.nom_complet || "",
      api_siren: r.siren || "",
      api_naf: r.activite_principale || "",
      api_naf25: r.activite_principale_naf25 || "",
      api_libelle_naf: NAF_SECTIONS[r.section_activite_principale] || "",
      api_categorie_entreprise: r.categorie_entreprise || "",
      api_nature_juridique: r.nature_juridique || "",
      api_date_creation: r.date_creation || "",
      api_etat_administratif: r.etat_administratif || "",
      api_tranche_effectif: r.tranche_effectif_salarie || "",
      api_annee_effectif: r.annee_tranche_effectif_salarie || "",
      api_caractere_employeur: siege.caractere_employeur || "",
      api_date_fermeture: r.date_fermeture || "",
      api_adresse_complete: siege.adresse || "",
      api_code_postal: siege.code_postal || "",
      api_commune: siege.libelle_commune || "",
      api_departement: siege.departement || "",
      api_region: siege.region || "",
      api_dirigeant_nom: dirigeant.nom || "",
      api_dirigeant_prenom: dirigeant.prenoms || "",
      api_dirigeant_qualite: dirigeant.qualite || "",
      api_liste_idcc: (siege.liste_idcc || []).join("|"),
      api_liste_rge: (siege.liste_rge || []).join("|"),
      api_section_activite: r.section_activite_principale || "",
    };
  } catch (err) {
    return { _error: err.name === "AbortError" ? "timeout" : "network" };
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---- Progress management ----
function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      return JSON.parse(fs.readFileSync(PROGRESS_FILE, "utf-8"));
    }
  } catch {}
  return {};
}

function saveProgress(progress) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress));
}

// ---- Collecte multi-source des SIRETs ----
function normalizeSiret(s) {
  const cleaned = String(s || "").replace(/[\s\-.]/g, "");
  return /^\d{14}$/.test(cleaned) ? cleaned : "";
}

function collectAllSirets() {
  const siretSet = new Set();

  // CAPEB (CSV)
  console.log("  CAPEB...");
  const raw = fs.readFileSync(SOURCES.capeb, "utf-8");
  const lines = raw.split("\n");
  const headers = parseCSVLine(lines[0]);
  const siretIdx = headers.indexOf("siret");
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const fields = parseCSVLine(lines[i]);
    const s = normalizeSiret(fields[siretIdx]);
    if (s) siretSet.add(s);
  }
  console.log(`    ${siretSet.size} SIRETs`);

  // XLSX sources
  for (const [name, filePath] of Object.entries(SOURCES)) {
    if (name === "capeb") continue;
    console.log(`  ${name}...`);
    const siretCol = name === "qualienr" ? "siret" : "Siret";
    const wb = XLSX.readFile(filePath);
    let added = 0;
    for (const sheetName of wb.SheetNames) {
      const rows = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], { defval: "" });
      for (const row of rows) {
        const s = normalizeSiret(String(row[siretCol] || ""));
        if (s && !siretSet.has(s)) { siretSet.add(s); added++; }
      }
    }
    console.log(`    +${added} nouveaux (total: ${siretSet.size})`);
  }

  return [...siretSet];
}

// ---- Main ----
async function main() {
  console.log("Collecte des SIRETs de toutes les sources...\n");
  const allSirets = collectAllSirets();
  const validSirets = allSirets.map(s => ({ siret: s }));
  console.log(`\nTotal SIRETs uniques: ${validSirets.length}`);

  // Charger la progression
  const progress = loadProgress();
  const alreadyDone = Object.keys(progress).length;
  console.log(`Déjà enrichis (reprise): ${alreadyDone}`);

  const toProcess = validSirets.filter((s) => !(s.siret in progress));
  console.log(`Restant à traiter: ${toProcess.length}`);
  console.log("");

  // Traitement par batch
  let processed = 0;
  let consecutiveErrors = 0;
  let totalFound = 0;
  let totalNotFound = 0;
  let totalErrors = 0;
  const startTime = Date.now();

  for (let i = 0; i < toProcess.length; i += BATCH_SIZE) {
    const batch = toProcess.slice(i, i + BATCH_SIZE);
    const batchResults = [];
    const promises = batch.map(async ({ siret }) => {
      const data = await fetchSiretData(siret);
      batchResults.push(data);
      if (data && !data._not_found && !data._error) {
        progress[siret] = data; // vrai résultat
      } else if (data && data._not_found) {
        progress[siret] = data; // vrai "pas trouvé"
      }
      // On ne sauvegarde PAS les erreurs réseau → sera retenté au prochain run
    });

    await Promise.all(promises);
    processed += batch.length;

    // Compter résultats du batch
    let batchErrors = 0;
    for (const r of batchResults) {
      if (!r || (r && r._error)) { batchErrors++; totalErrors++; }
      else if (r._not_found) { totalNotFound++; }
      else { totalFound++; }
    }

    if (batchErrors === batch.length) {
      consecutiveErrors++;
      if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
        console.log(`\n\nAPI semble down (${MAX_CONSECUTIVE_ERRORS} batches en erreur). Arrêt propre.`);
        console.log(`Progression sauvegardée. Relancez plus tard.`);
        saveProgress(progress);
        break;
      }
    } else {
      consecutiveErrors = 0;
    }

    // Sauvegarde de progression régulière
    if (Math.floor(processed / BATCH_SIZE) % SAVE_EVERY === 0 || i + BATCH_SIZE >= toProcess.length) {
      saveProgress(progress);
    }

    // Stats
    const elapsed = (Date.now() - startTime) / 1000;
    const rate = processed / elapsed;
    const remaining = toProcess.length - processed;
    const eta = remaining / rate;
    const etaMin = Math.floor(eta / 60);
    const etaSec = Math.floor(eta % 60);

    process.stdout.write(
      `\r  ${alreadyDone + processed}/${validSirets.length} | ${totalFound} trouvés | ${totalNotFound} absents | ${totalErrors} erreurs | ${rate.toFixed(1)}/s | ETA: ${etaMin}m${etaSec}s    `
    );

    if (i + BATCH_SIZE < toProcess.length) {
      await sleep(DELAY_BETWEEN_BATCHES);
    }
  }

  saveProgress(progress);

  // Stats finales
  let enriched = 0, notFoundTotal = 0;
  for (const k of Object.keys(progress)) {
    if (progress[k]._not_found) notFoundTotal++;
    else enriched++;
  }

  console.log(`\n\nTerminé !`);
  console.log(`  Progress file: ${PROGRESS_FILE}`);
  console.log(`  Total enrichis: ${enriched}`);
  console.log(`  Non trouvés: ${notFoundTotal}`);
  console.log(`  Total dans progress: ${Object.keys(progress).length}`);
  console.log(`\n  → Relancer 'node reconcile.js' pour intégrer les nouvelles données.`);
}

main().catch(console.error);
