const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");

// ============================================================
//  CONFIG
// ============================================================
const BASE_DIR = __dirname;
const OUTPUT_FILE = path.join(BASE_DIR, "artisans_unified.csv");
const STATS_FILE = path.join(BASE_DIR, "reconciliation_stats.json");
const UNMATCHED_FILE = path.join(BASE_DIR, "linkedin_unmatched.csv");
const ENRICH_FILE = path.join(BASE_DIR, ".enrich_progress.json");
const WEBSITE_FILE = path.join(BASE_DIR, ".website_progress.json");

const SOURCES = {
  capeb: path.join(BASE_DIR, "artisans_capeb.csv"),
  qualibat: path.join(BASE_DIR, "Base Qualibat.xlsx"),
  qualienr: path.join(BASE_DIR, "Base QualiENR.xlsx"),
  qualifelec: path.join(BASE_DIR, "Base Qualif'elec.xlsx"),
  linkedin: path.join(BASE_DIR, "Base Linkedin.xlsx"),
};

// ============================================================
//  UTILS
// ============================================================
function parseCSVLine(line) {
  const fields = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') { current += '"'; i++; }
      else if (ch === '"') { inQuotes = false; }
      else { current += ch; }
    } else {
      if (ch === '"') { inQuotes = true; }
      else if (ch === ",") { fields.push(current); current = ""; }
      else { current += ch; }
    }
  }
  fields.push(current);
  return fields;
}

function escapeCSV(val) {
  if (val == null) return "";
  // Strip newlines/carriage returns to prevent multi-line CSV issues
  const s = String(val).replace(/[\r\n]+/g, " ").trim();
  if (s.includes(",") || s.includes('"') || s.includes(";")) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function normalizeSiret(s) {
  const cleaned = String(s || "").replace(/[\s\-.]/g, "");
  return /^\d{14}$/.test(cleaned) ? cleaned : "";
}

function extractSiren(s) {
  const siret = normalizeSiret(s);
  if (siret) return siret.substring(0, 9);
  const cleaned = String(s || "").replace(/[\s\-.]/g, "");
  return /^\d{9}$/.test(cleaned) ? cleaned : "";
}

// ============================================================
//  NOM NORMALISATION + FUZZY
// ============================================================
const STOP_WORDS = new Set([
  // Formes juridiques
  "sarl", "sas", "eurl", "sa", "ei", "eirl", "sasu", "sci", "snc", "scp", "sep",
  "ste", "societe", "société", "entreprise", "ent", "ets", "etablissements",
  // Mots de liaison
  "et", "de", "du", "des", "le", "la", "les", "l", "d", "a", "au", "aux",
  // Civilités
  "fils", "freres", "frères", "mr", "mme", "m", "me",
  // Mots métier artisanat — trop génériques, polluent le fuzzy matching
  "plomberie", "plombier", "electricite", "electricien", "electrique",
  "menuiserie", "menuisier", "serrurerie", "serrurier",
  "couverture", "couvreur", "zinguerie", "zingueur",
  "maconnerie", "macon", "peinture", "peintre",
  "chauffage", "climatisation", "ventilation", "isolation",
  "carrelage", "carreleur", "charpente", "charpentier",
  "plaquiste", "platrier", "platrerie",
  "renovation", "construction", "batiment", "artisan", "artisanal",
  "depannage", "installation", "maintenance", "travaux", "services",
]);

function normalizeCompanyName(name) {
  if (!name) return "";
  let s = String(name).toLowerCase();
  s = s.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  s = s.replace(/[^a-z0-9\s]/g, " ");
  const words = s.split(/\s+/).filter(w => w.length > 0 && !STOP_WORDS.has(w));
  return words.join(" ");
}

function trigrams(s) {
  const set = new Set();
  const padded = `  ${s} `;
  for (let i = 0; i < padded.length - 2; i++) set.add(padded.substring(i, i + 3));
  return set;
}

function trigramSimilarity(a, b) {
  const tA = trigrams(a), tB = trigrams(b);
  let intersection = 0;
  for (const t of tA) if (tB.has(t)) intersection++;
  const union = tA.size + tB.size - intersection;
  return union === 0 ? 0 : intersection / union;
}

function levenshtein(a, b) {
  if (a.length === 0) return b.length;
  if (b.length === 0) return a.length;
  const m = [];
  for (let i = 0; i <= b.length; i++) m[i] = [i];
  for (let j = 0; j <= a.length; j++) m[0][j] = j;
  for (let i = 1; i <= b.length; i++)
    for (let j = 1; j <= a.length; j++)
      m[i][j] = b[i-1] === a[j-1] ? m[i-1][j-1] : Math.min(m[i-1][j-1]+1, m[i][j-1]+1, m[i-1][j]+1);
  return m[b.length][a.length];
}

function nameSimilarity(nameA, nameB) {
  const a = normalizeCompanyName(nameA), b = normalizeCompanyName(nameB);
  if (!a || !b) return 0;
  if (a === b) return 1.0;
  const tSim = trigramSimilarity(a, b);
  const lSim = 1 - levenshtein(a, b) / Math.max(a.length, b.length);
  const containsBonus = (a.includes(b) || b.includes(a)) ? 0.15 : 0;
  return Math.min(1.0, tSim * 0.5 + lSim * 0.5 + containsBonus);
}

function normalizePersonName(name) {
  if (!name) return "";
  return String(name).toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z\s]/g, " ").split(/\s+/).filter(w => w.length > 1).sort().join(" ");
}

function personNameMatch(a, b) {
  const na = normalizePersonName(a), nb = normalizePersonName(b);
  if (!na || !nb) return 0;
  if (na === nb) return 1.0;
  const wa = na.split(" "), wb = nb.split(" ");
  if (wa[wa.length-1] === wb[wb.length-1]) return 0.7;
  return 0;
}

function extractCity(location) {
  if (!location) return "";
  return String(location).split(",")[0].trim().toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

function normalizeCity(city) {
  if (!city) return "";
  return String(city).toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z\s]/g, " ").trim();
}

// Extraire le code postal depuis une adresse HQ LinkedIn (ex: "78, Rue de Rome, 75008, Paris")
function extractPostalFromHQ(hq) {
  if (!hq) return "";
  const match = String(hq).match(/\b(\d{5})\b/);
  return match ? match[1] : "";
}

// Extraire la ville depuis une adresse HQ LinkedIn
function extractCityFromHQ(hq) {
  if (!hq) return "";
  const parts = String(hq).split(",").map(p => p.trim());
  // Format typique: "adresse, code_postal, ville, region, France"
  for (let i = 0; i < parts.length; i++) {
    if (/^\d{5}$/.test(parts[i]) && parts[i + 1]) {
      return normalizeCity(parts[i + 1]);
    }
  }
  return "";
}

// Normaliser un domaine web pour comparaison
function normalizeDomain(url) {
  if (!url) return "";
  let s = String(url).toLowerCase().trim();
  s = s.replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/.*$/, "");
  return s;
}

// Extraire le domaine d'un email
function emailDomain(email) {
  if (!email) return "";
  const parts = String(email).split("@");
  return parts.length === 2 ? parts[1].toLowerCase().trim() : "";
}

// ============================================================
//  ENTITÉ UNIFIÉE — le modèle de données cible
// ============================================================
function createEntity() {
  return {
    // === IDENTITÉ ===
    siret: "",
    siren: "",
    nom_entreprise: "",
    nom_commercial: "",
    forme_juridique: "",
    code_ape: "",
    date_creation: "",

    // === CONTACT ===
    email_generique: "",
    email_dirigeant: "",
    telephone: "",
    fax: "",
    site_web: "",

    // === LOCALISATION ===
    adresse: "",
    adresse2: "",
    code_postal: "",
    ville: "",
    latitude: "",
    longitude: "",

    // === DIRIGEANT ===
    dirigeant_nom_complet: "",
    dirigeant_prenom: "",
    dirigeant_nom: "",
    dirigeant_telephone: "",

    // === FINANCIER ===
    chiffre_affaires: "",
    nb_salaries: "",
    solvabilite: "",
    risque_impaye: "",

    // === CERTIFICATIONS ===
    is_rge: "",
    specialite: "",
    activites_principales: "",
    activites_secondaires: "",

    // Qualibat
    qualibat: "",  // oui/non
    qualibat_type: "",
    qualibat_assurance: "",

    // QualiENR
    qualienr: "",  // oui/non
    qualienr_certifications: "",
    qualienr_photovoltaique: "",
    qualienr_validite: "",

    // Qualifelec
    qualifelec: "",  // oui/non
    qualifelec_rge: "",
    qualifelec_spv: "",
    qualifelec_date_debut: "",
    qualifelec_date_fin: "",

    // === ASSURANCES ===
    assurance_rc: "",
    assurance_dc: "",

    // === LINKEDIN ===
    linkedin_profil: "",
    linkedin_page_entreprise: "",
    linkedin_job_title: "",
    linkedin_headline: "",
    linkedin_location: "",
    linkedin_hq: "",
    linkedin_description: "",
    linkedin_category: "",
    linkedin_ca_estime: "",
    linkedin_employee: "",
    linkedin_creation: "",
    linkedin_source_metier: "",
    linkedin_match_method: "",       // website|email|hq_name|fuzzy
    linkedin_match_confidence: "",   // high|medium|low
    linkedin_match_score: "",

    // === API SIRENE (données officielles INSEE) ===
    api_etat_administratif: "",   // A=actif, F=fermé
    api_date_fermeture: "",
    api_nature_juridique: "",     // code juridique INSEE
    api_categorie_entreprise: "", // PME, ETI, GE, TPE
    api_section_activite: "",     // lettre section NAF (F=Construction, etc.)
    api_naf: "",                  // code NAF officiel (ex: 43.32A)
    api_naf25: "",                // code NAF 2025 (nouvelle nomenclature)
    api_libelle_naf: "",          // libellé activité
    api_tranche_effectif: "",     // code tranche (00, 01, 02... NN=non renseigné)
    api_caractere_employeur: "",  // O/N
    api_dirigeant_qualite: "",    // Gérant, Président, etc.
    api_liste_idcc: "",           // conventions collectives
    api_liste_rge: "",            // labels RGE officiels
    api_departement: "",
    api_region: "",

    // === WEBSITE CRAWL ===
    website_emails: "",          // emails trouvés sur le site (pipe-separated)
    website_mobiles: "",         // mobiles trouvés (pipe-separated)
    website_fixes: "",           // fixes trouvés (pipe-separated)

    // === FLAGS ===
    source_only_linkedin: "",    // "oui" si entité créée uniquement depuis LinkedIn (pas de SIRET)

    // === METADATA ===
    sources: [],
    nb_sources: 0,
    score_completude: 0,
  };
}

// Prendre la première valeur non vide
function firstNonEmpty(...vals) {
  for (const v of vals) {
    const s = String(v || "").trim();
    if (s && s !== "undefined" && s !== "null" && s !== "[object Object]") return s;
  }
  return "";
}

// ============================================================
//  LOADERS
// ============================================================
function loadCAPEB() {
  console.log("  Chargement CAPEB...");
  const raw = fs.readFileSync(SOURCES.capeb, "utf-8");
  const lines = raw.split("\n");
  const headers = parseCSVLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const fields = parseCSVLine(lines[i]);
    const row = {};
    for (let j = 0; j < headers.length; j++) row[headers[j]] = fields[j] || "";
    rows.push(row);
  }
  console.log(`    ${rows.length} lignes`);
  return rows;
}

function loadXLSXDedup(filePath, siretCol) {
  const wb = XLSX.readFile(filePath);
  const allRows = [];
  const seen = new Set();
  for (const sheetName of wb.SheetNames) {
    const rows = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], { defval: "" });
    for (const row of rows) {
      const siret = normalizeSiret(String(row[siretCol] || ""));
      if (siret && seen.has(siret)) continue;
      if (siret) seen.add(siret);
      allRows.push(row);
    }
  }
  console.log(`    ${allRows.length} lignes uniques`);
  return allRows;
}

function loadLinkedinDedup() {
  const wb = XLSX.readFile(SOURCES.linkedin);
  const rows = [];
  const seen = new Set();
  for (const sheetName of wb.SheetNames) {
    const sheetRows = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], { defval: "" });
    for (const row of sheetRows) {
      const url = row.linkedin_profile_url || "";
      if (url && seen.has(url)) continue;
      if (url) seen.add(url);
      rows.push(row);
    }
  }
  console.log(`    ${rows.length} profils uniques`);
  return rows;
}

// ============================================================
//  ÉTAPE 1 : Construire le dictionnaire d'entités (clé = SIRET)
//            Sources structurées : CAPEB + Qualibat + QualiENR + Qualifelec
// ============================================================
function buildEntityMap(capeb, qualibat, qualienr, qualifelec) {
  const entities = new Map(); // siret -> entity
  const sirenToSiret = new Map(); // siren -> premier siret vu (pour les dedup SIREN)

  function getOrCreate(siret) {
    if (!entities.has(siret)) {
      const e = createEntity();
      e.siret = siret;
      e.siren = siret.substring(0, 9);
      entities.set(siret, e);
      if (!sirenToSiret.has(e.siren)) sirenToSiret.set(e.siren, siret);
    }
    return entities.get(siret);
  }

  // --- CAPEB (base la plus large) ---
  let capebIngested = 0;
  for (const r of capeb) {
    const siret = normalizeSiret(r.siret);
    if (!siret) continue;
    const e = getOrCreate(siret);
    e.nom_entreprise = firstNonEmpty(e.nom_entreprise, r.name, r.fullName);
    e.nom_commercial = firstNonEmpty(e.nom_commercial, r.fullName);
    e.forme_juridique = firstNonEmpty(e.forme_juridique, r.legalCode);
    e.email_generique = firstNonEmpty(e.email_generique, r.email);
    e.telephone = firstNonEmpty(e.telephone, r.phone);
    e.fax = firstNonEmpty(e.fax, r.fax);
    e.site_web = firstNonEmpty(e.site_web, r.website);
    e.adresse = firstNonEmpty(e.adresse, r.address);
    e.adresse2 = firstNonEmpty(e.adresse2, r.address2);
    e.code_postal = firstNonEmpty(e.code_postal, r.zipCode);
    e.ville = firstNonEmpty(e.ville, r.city);
    e.latitude = firstNonEmpty(e.latitude, r.latitude);
    e.longitude = firstNonEmpty(e.longitude, r.longitude);
    e.is_rge = firstNonEmpty(e.is_rge, r.isRGE === "true" ? "oui" : (r.isRGE === "false" ? "non" : ""));
    e.specialite = firstNonEmpty(e.specialite, r.speciality);
    e.activites_principales = firstNonEmpty(e.activites_principales, r.mainActivities);
    e.activites_secondaires = firstNonEmpty(e.activites_secondaires, r.secondaryActivities);
    e.dirigeant_nom_complet = firstNonEmpty(e.dirigeant_nom_complet, r.fullName);
    if (!e.sources.includes("capeb")) e.sources.push("capeb");
    capebIngested++;
  }
  console.log(`    CAPEB: ${capebIngested} entités`);

  // --- QUALIBAT ---
  let qbNew = 0, qbMerged = 0;
  for (const r of qualibat) {
    const siret = normalizeSiret(r.Siret);
    if (!siret) continue;
    const e = getOrCreate(siret);
    const isNew = !e.sources.includes("capeb") && e.sources.length === 0;
    e.nom_entreprise = firstNonEmpty(e.nom_entreprise, r.Entreprise);
    e.code_ape = firstNonEmpty(e.code_ape, r.APE);
    e.chiffre_affaires = firstNonEmpty(e.chiffre_affaires, r["chiffre d'affaire"]);
    e.solvabilite = firstNonEmpty(e.solvabilite, r["solvabilité"]);
    e.date_creation = firstNonEmpty(e.date_creation, r["date de création"]);
    e.nb_salaries = firstNonEmpty(e.nb_salaries, r["Salariés"]);
    e.site_web = firstNonEmpty(e.site_web, r["site web"], r["site web_1"]);
    e.email_generique = firstNonEmpty(e.email_generique, r["e-mail générique"]);
    e.email_dirigeant = firstNonEmpty(e.email_dirigeant, r["e-mail enrichis"], r["e-mail direct"], r["email certificat signataire"], r["email certificat"]);
    e.dirigeant_nom_complet = firstNonEmpty(e.dirigeant_nom_complet, r["Dirigeant signataire"], r["Décideurs"], r["dirigeant infonet"]);
    e.dirigeant_prenom = firstNonEmpty(e.dirigeant_prenom, r["Prénom"], r["Prénom_1"]);
    e.dirigeant_nom = firstNonEmpty(e.dirigeant_nom, r["nom"], r["Nom"]);
    e.dirigeant_telephone = firstNonEmpty(e.dirigeant_telephone, r["num signataire"]);
    e.qualibat = "oui";
    e.qualibat_type = firstNonEmpty(e.qualibat_type, r.Type);
    e.qualibat_assurance = firstNonEmpty(e.qualibat_assurance, r["Assurance Certificat"]);
    e.assurance_rc = firstNonEmpty(e.assurance_rc, r["Assurance Certificat"]);
    if (!e.sources.includes("qualibat")) e.sources.push("qualibat");
    if (isNew) qbNew++; else qbMerged++;
  }
  console.log(`    Qualibat: ${qbMerged} fusionnés, ${qbNew} nouveaux`);

  // --- QUALIENR ---
  let qeNew = 0, qeMerged = 0;
  for (const r of qualienr) {
    const siret = normalizeSiret(r.siret);
    if (!siret) continue;
    const e = getOrCreate(siret);
    const isNew = e.sources.length === 0;
    e.nom_entreprise = firstNonEmpty(e.nom_entreprise, r["nom entreprise"], r["Entreprise"]);
    e.code_ape = firstNonEmpty(e.code_ape, r["Code APE"]);
    e.site_web = firstNonEmpty(e.site_web, r.Website);
    e.email_generique = firstNonEmpty(e.email_generique, r["email (scrappé)"], r.email);
    e.telephone = firstNonEmpty(e.telephone, r["Tel "], r.Tel);
    e.dirigeant_nom_complet = firstNonEmpty(e.dirigeant_nom_complet, r["dirigeant infonet"], r.Full_name, r["Nom responsable légale"]);
    e.dirigeant_prenom = firstNonEmpty(e.dirigeant_prenom, r.first_name);
    e.dirigeant_nom = firstNonEmpty(e.dirigeant_nom, r.last_name);
    e.email_dirigeant = firstNonEmpty(e.email_dirigeant, r["e-mail direct (Enrichis)"], r["e-mail direct"]);
    e.chiffre_affaires = firstNonEmpty(e.chiffre_affaires, r.CA);
    e.risque_impaye = firstNonEmpty(e.risque_impaye, r["Impayé"]);
    // Certifications ENR
    const certifs = [r.Certif1, r.Certif2, r.Certif3].filter(Boolean).map(s => String(s).replace(/\n/g, " | ").trim()).filter(s => s);
    e.qualienr = "oui";
    e.qualienr_certifications = firstNonEmpty(e.qualienr_certifications, certifs.join(" | "));
    e.qualienr_photovoltaique = firstNonEmpty(e.qualienr_photovoltaique, r["Photovolt ?"], r["Photovolt ?_1"]);
    e.qualienr_validite = firstNonEmpty(e.qualienr_validite, r["Validité certificat"]);
    e.assurance_rc = firstNonEmpty(e.assurance_rc, r["Assurance RC"]);
    e.assurance_dc = firstNonEmpty(e.assurance_dc, r["Aassurance DC"]);
    e.is_rge = firstNonEmpty(e.is_rge, "oui"); // QualiENR = RGE par définition
    if (!e.sources.includes("qualienr")) e.sources.push("qualienr");
    if (isNew) qeNew++; else qeMerged++;
  }
  console.log(`    QualiENR: ${qeMerged} fusionnés, ${qeNew} nouveaux`);

  // --- QUALIFELEC ---
  let qfNew = 0, qfMerged = 0;
  for (const r of qualifelec) {
    let siret = normalizeSiret(r.Siret || r.siret);
    // Qualifelec a parfois SIREN seul — essayer de retrouver un SIRET existant
    if (!siret) {
      const siren = extractSiren(r.Siren || r.siren || r.Siret || r.siret);
      if (siren && sirenToSiret.has(siren)) {
        siret = sirenToSiret.get(siren);
      } else if (siren) {
        // Pas de SIRET connu → créer avec SIREN padded (on ajoutera NIC plus tard)
        continue; // on skip, pas assez fiable sans SIRET
      }
    }
    if (!siret) continue;
    const e = getOrCreate(siret);
    const isNew = e.sources.length === 0;
    e.nom_entreprise = firstNonEmpty(e.nom_entreprise, r["Nom entreprise"], r.Entreprise);
    e.code_ape = firstNonEmpty(e.code_ape, r.APE, r["code APE"]);
    e.site_web = firstNonEmpty(e.site_web, r["site web"]);
    e.email_generique = firstNonEmpty(e.email_generique, r["E-mail (scrappé)"], r["E-mail (Qualifelec)"], r["email (scrappé)"]);
    e.telephone = firstNonEmpty(e.telephone, r.Tel, r["phone number (Qualifelec)"]);
    e.dirigeant_nom_complet = firstNonEmpty(e.dirigeant_nom_complet, r["Dirigeant (Scrappé)"], r.Full_name, r["Nom récomposé"], r["dirigeant (infonet)"]);
    e.dirigeant_prenom = firstNonEmpty(e.dirigeant_prenom, r["Prénom"], r["Prénom signataire"]);
    e.dirigeant_nom = firstNonEmpty(e.dirigeant_nom, r["Nom"], r["Nom Signataire"]);
    e.email_dirigeant = firstNonEmpty(e.email_dirigeant, r["e-mail direct (enrichis)"], r["e-mail direct"], r["E-mail (Enrichis)"], r["E-mail direct"]);
    e.linkedin_profil = firstNonEmpty(e.linkedin_profil, r.linkedin);
    e.chiffre_affaires = firstNonEmpty(e.chiffre_affaires, r.CA);
    e.risque_impaye = firstNonEmpty(e.risque_impaye, r["risque impayé"]);
    e.solvabilite = firstNonEmpty(e.solvabilite, r["score solvabilité"]);
    e.adresse = firstNonEmpty(e.adresse, r["Adresse 1"]);
    e.adresse2 = firstNonEmpty(e.adresse2, r["Adresse 2"]);
    e.code_postal = firstNonEmpty(e.code_postal, r.CP);
    e.qualifelec = "oui";
    e.qualifelec_rge = firstNonEmpty(e.qualifelec_rge, r.RGE, r["RGE ?"]);
    e.qualifelec_spv = firstNonEmpty(e.qualifelec_spv, r.SPV);
    e.qualifelec_date_debut = firstNonEmpty(e.qualifelec_date_debut, r["start date (certificat)"]);
    e.qualifelec_date_fin = firstNonEmpty(e.qualifelec_date_fin, r["end date (certificat)"]);
    e.assurance_rc = firstNonEmpty(e.assurance_rc, r["Assurance RC"]);
    e.assurance_dc = firstNonEmpty(e.assurance_dc, r["Assurance DC"], r["Assurance Décennale"]);
    if (e.qualifelec_rge === "Oui" || e.qualifelec_rge === "oui") e.is_rge = "oui";
    if (!e.sources.includes("qualifelec")) e.sources.push("qualifelec");
    if (isNew) qfNew++; else qfMerged++;
  }
  console.log(`    Qualifelec: ${qfMerged} fusionnés, ${qfNew} nouveaux`);

  return { entities, sirenToSiret };
}

// ============================================================
//  ÉTAPE 2 : Matching LinkedIn → entités
//  V3 — 4 passes de précision décroissante
//    Passe 1 : Match par domaine web (exact)
//    Passe 2 : Match par domaine email (exact)
//    Passe 3 : Match par code postal HQ + nom similaire
//    Passe 4 : Fuzzy trigram (nom + ville + personne)
// ============================================================

// Seuils fuzzy (passe 4)
const FUZZY_THRESHOLD = 0.65;
const FUZZY_SHORT_NAME_THRESHOLD = 0.82;

// Appliquer les données LinkedIn à une entité
function applyLinkedinData(e, row, method, confidence, score) {
  if (e.linkedin_profil) return false; // déjà rempli
  e.linkedin_profil = firstNonEmpty(e.linkedin_profil, row.linkedin_profile_url);
  e.linkedin_page_entreprise = firstNonEmpty(e.linkedin_page_entreprise, row["linkedin page"]);
  e.linkedin_job_title = firstNonEmpty(e.linkedin_job_title, row.job_title);
  e.linkedin_headline = firstNonEmpty(e.linkedin_headline, row.headline);
  e.linkedin_location = firstNonEmpty(e.linkedin_location, row.location);
  e.linkedin_hq = firstNonEmpty(e.linkedin_hq, row.HQ);
  e.linkedin_description = firstNonEmpty(e.linkedin_description, row.description);
  e.linkedin_category = firstNonEmpty(e.linkedin_category, row.Category);
  e.linkedin_ca_estime = firstNonEmpty(e.linkedin_ca_estime, row["CA estimé"]);
  e.linkedin_employee = firstNonEmpty(e.linkedin_employee, row.employee);
  e.linkedin_creation = firstNonEmpty(e.linkedin_creation, row["création"]);
  e.linkedin_source_metier = firstNonEmpty(e.linkedin_source_metier, row.Source);
  e.linkedin_match_method = method;
  e.linkedin_match_confidence = confidence;
  e.linkedin_match_score = String(score);
  // Enrichir les données de contact/dirigeant si manquantes
  e.email_dirigeant = firstNonEmpty(e.email_dirigeant, row["e-mail direct"]);
  e.dirigeant_nom_complet = firstNonEmpty(e.dirigeant_nom_complet, row.full_name);
  e.dirigeant_prenom = firstNonEmpty(e.dirigeant_prenom, row.first_name);
  e.dirigeant_nom = firstNonEmpty(e.dirigeant_nom, row.last_name);
  e.site_web = firstNonEmpty(e.site_web, row.website);
  e.chiffre_affaires = firstNonEmpty(e.chiffre_affaires, row["CA estimé"]);
  e.nb_salaries = firstNonEmpty(e.nb_salaries, row.employee);
  if (!e.sources.includes("linkedin")) e.sources.push("linkedin");
  return true;
}

function matchLinkedin(linkedinRows, entities) {
  const stats = { pass1_website: 0, pass2_email: 0, pass3_hq: 0, pass4_fuzzy: 0, unmatched: 0 };
  const confDist = { high: 0, medium: 0, low: 0 };
  const unmatchedRows = [];
  let remaining = [...linkedinRows]; // profiles restants à matcher

  // === INDEX: domaine web → siret ===
  const domainToSiret = new Map();
  for (const [siret, e] of entities) {
    const d = normalizeDomain(e.site_web);
    if (d && d.length > 3) domainToSiret.set(d, siret);
  }

  // === INDEX: domaine email → siret ===
  const emailDomToSiret = new Map();
  for (const [siret, e] of entities) {
    for (const emailField of [e.email_generique, e.email_dirigeant]) {
      const d = emailDomain(emailField);
      if (d && !["gmail.com","yahoo.fr","yahoo.com","hotmail.fr","hotmail.com","wanadoo.fr","orange.fr","free.fr","sfr.fr","laposte.net","outlook.fr","outlook.com","live.fr","bbox.fr"].includes(d)) {
        emailDomToSiret.set(d, siret);
      }
    }
  }

  // === INDEX: code postal → sirets ===
  const cpToSirets = new Map();
  for (const [siret, e] of entities) {
    const cp = String(e.code_postal || "").trim();
    if (/^\d{5}$/.test(cp)) {
      if (!cpToSirets.has(cp)) cpToSirets.set(cp, []);
      cpToSirets.get(cp).push(siret);
    }
  }

  console.log(`    Index: ${domainToSiret.size} domaines web, ${emailDomToSiret.size} domaines email, ${cpToSirets.size} codes postaux`);

  // ========== PASSE 1 : Match par domaine web ==========
  console.log("    Passe 1: Match par domaine web...");
  let nextRemaining = [];
  for (const row of remaining) {
    const liDomain = normalizeDomain(row.website);
    if (liDomain && domainToSiret.has(liDomain)) {
      const e = entities.get(domainToSiret.get(liDomain));
      if (applyLinkedinData(e, row, "website", "high", "1.000")) {
        stats.pass1_website++;
        confDist.high++;
      } else {
        nextRemaining.push(row); // entité déjà remplie par un autre profil
      }
    } else {
      nextRemaining.push(row);
    }
  }
  remaining = nextRemaining;
  console.log(`      ${stats.pass1_website} matchés`);

  // ========== PASSE 2 : Match par domaine email ==========
  console.log("    Passe 2: Match par domaine email...");
  nextRemaining = [];
  for (const row of remaining) {
    const liEmailDom = emailDomain(row["e-mail direct"]);
    if (liEmailDom && emailDomToSiret.has(liEmailDom)) {
      const e = entities.get(emailDomToSiret.get(liEmailDom));
      if (applyLinkedinData(e, row, "email", "high", "1.000")) {
        stats.pass2_email++;
        confDist.high++;
      } else {
        nextRemaining.push(row);
      }
    } else {
      nextRemaining.push(row);
    }
  }
  remaining = nextRemaining;
  console.log(`      ${stats.pass2_email} matchés`);

  // ========== PASSE 3 : Match par code postal HQ + nom similaire ==========
  console.log("    Passe 3: Match par HQ (code postal + nom)...");
  nextRemaining = [];
  for (const row of remaining) {
    const hqCP = extractPostalFromHQ(row.HQ);
    const hqCity = extractCityFromHQ(row.HQ);
    const normCompany = normalizeCompanyName(row.company_name || "");

    if (hqCP && normCompany && cpToSirets.has(hqCP)) {
      const candidates = cpToSirets.get(hqCP);
      let bestSiret = null, bestScore = 0;
      for (const siret of candidates) {
        const e = entities.get(siret);
        const nScore = nameSimilarity(row.company_name, normalizeCompanyName(e.nom_entreprise));
        if (nScore > bestScore) { bestScore = nScore; bestSiret = siret; }
      }
      // Code postal identique + nom similaire → très fiable
      if (bestScore >= 0.50 && bestSiret) {
        const e = entities.get(bestSiret);
        const conf = bestScore >= 0.80 ? "high" : "medium";
        if (applyLinkedinData(e, row, "hq_postal", conf, bestScore.toFixed(3))) {
          stats.pass3_hq++;
          confDist[conf]++;
        } else {
          nextRemaining.push(row);
        }
      } else {
        nextRemaining.push(row);
      }
    } else {
      nextRemaining.push(row);
    }
  }
  remaining = nextRemaining;
  console.log(`      ${stats.pass3_hq} matchés`);

  // ========== PASSE 4 : Fuzzy matching (trigrammes) ==========
  console.log("    Passe 4: Fuzzy matching (trigrammes)...");

  // Construire index trigrammes
  const nameIndex = [];
  const trigramIdx = new Map();
  for (const [siret, entity] of entities) {
    if (entity.linkedin_profil) continue; // déjà matché dans passes précédentes
    const norm = normalizeCompanyName(entity.nom_entreprise);
    if (!norm) continue;
    const idx = nameIndex.length;
    nameIndex.push({ siret, norm, city: normalizeCity(entity.ville), cp: entity.code_postal, dirigeant: entity.dirigeant_nom_complet });
    for (const tg of trigrams(norm)) {
      if (!trigramIdx.has(tg)) trigramIdx.set(tg, new Set());
      trigramIdx.get(tg).add(idx);
    }
  }
  console.log(`      Index: ${nameIndex.length} noms, ${trigramIdx.size} trigrammes`);

  let fuzzyMatched = 0, cityRejected = 0, shortRejected = 0;
  nextRemaining = [];
  for (let li = 0; li < remaining.length; li++) {
    const row = remaining[li];
    const companyName = row.company_name || "";
    const normCompany = normalizeCompanyName(companyName);
    const personName = row.full_name || `${row.first_name || ""} ${row.last_name || ""}`.trim();
    const liCity = extractCity(row.location || "");
    // Aussi essayer la ville du HQ si dispo
    const liHQCity = extractCityFromHQ(row.HQ);
    const effectiveCity = liCity || liHQCity;

    let bestSiret = null, bestScore = 0, bestNameScore = 0, bestCityMatch = null;

    if (normCompany) {
      const queryTg = trigrams(normCompany);
      const candidates = new Map();
      for (const tg of queryTg) {
        const hits = trigramIdx.get(tg);
        if (!hits) continue;
        for (const idx of hits) candidates.set(idx, (candidates.get(idx) || 0) + 1);
      }

      const minTg = Math.max(2, Math.floor(queryTg.size * 0.15));
      for (const [idx, count] of candidates) {
        if (count < minTg) continue;
        const entry = nameIndex[idx];
        const nScore = nameSimilarity(companyName, entry.norm);
        if (nScore < 0.45) continue;

        // Scoring ville : check location ET HQ city
        let cityMatch = "unknown";
        if (effectiveCity && entry.city) {
          if (effectiveCity === entry.city || effectiveCity.includes(entry.city) || entry.city.includes(effectiveCity)) {
            cityMatch = "match";
          } else {
            cityMatch = "mismatch";
          }
        }

        const cityW = cityMatch === "match" ? 1.0 : cityMatch === "mismatch" ? 0.0 : 0.5;
        const pScore = personNameMatch(personName, entry.dirigeant);
        const total = nScore * 0.70 + cityW * 0.18 + pScore * 0.12;

        if (total > bestScore) {
          bestScore = total;
          bestSiret = entry.siret;
          bestNameScore = nScore;
          bestCityMatch = cityMatch;
        }
      }
    }

    const isShortName = normCompany.length < 6;
    const threshold = isShortName ? FUZZY_SHORT_NAME_THRESHOLD : FUZZY_THRESHOLD;

    if (bestScore >= threshold && bestSiret) {
      // Rejet : ville clairement différente ET nom pas quasi-exact
      if (bestCityMatch === "mismatch" && bestNameScore < 0.92) {
        nextRemaining.push(row);
        cityRejected++;
      } else {
        const e = entities.get(bestSiret);
        const conf = bestScore >= 0.85 ? "high" : bestScore >= 0.75 ? "medium" : "low";
        if (applyLinkedinData(e, row, "fuzzy", conf, bestScore.toFixed(3))) {
          fuzzyMatched++;
          confDist[conf]++;
        } else {
          nextRemaining.push(row);
        }
      }
    } else {
      nextRemaining.push(row);
      if (isShortName && bestScore >= FUZZY_THRESHOLD) shortRejected++;
    }

    if ((li + 1) % 1000 === 0) process.stdout.write(`\r      ${li + 1}/${remaining.length}...`);
  }
  stats.pass4_fuzzy = fuzzyMatched;
  stats.unmatched = nextRemaining.length;
  console.log(`\r      ${remaining.length}/${remaining.length} traités`);
  console.log(`      ${fuzzyMatched} matchés | ${cityRejected} rejetés ville | ${shortRejected} rejetés nom court`);

  // Résumé final
  const totalMatched = stats.pass1_website + stats.pass2_email + stats.pass3_hq + stats.pass4_fuzzy;
  console.log(`\n    === Résumé matching LinkedIn ===`);
  console.log(`    Passe 1 (website):    ${stats.pass1_website}`);
  console.log(`    Passe 2 (email):      ${stats.pass2_email}`);
  console.log(`    Passe 3 (HQ+nom):     ${stats.pass3_hq}`);
  console.log(`    Passe 4 (fuzzy):      ${stats.pass4_fuzzy}`);
  console.log(`    TOTAL matchés:        ${totalMatched}`);
  console.log(`    Non matchés:          ${stats.unmatched}`);
  console.log(`    Confidence: high=${confDist.high} medium=${confDist.medium} low=${confDist.low}`);

  return { matched: totalMatched, unmatched: stats.unmatched, unmatchedRows: nextRemaining, stats };
}

// ============================================================
//  ÉTAPE 3 : Enrichissement API SIRENE (données officielles INSEE)
// ============================================================
function enrichFromAPI(entities) {
  if (!fs.existsSync(ENRICH_FILE)) {
    console.log("    Fichier .enrich_progress.json introuvable — skip");
    return { enriched: 0, notFound: 0, noMatch: 0 };
  }

  const apiData = JSON.parse(fs.readFileSync(ENRICH_FILE, "utf-8"));
  const apiKeys = Object.keys(apiData);
  let enriched = 0, notFound = 0, noMatch = 0;

  for (const siret of apiKeys) {
    const d = apiData[siret];
    if (d._not_found) { notFound++; continue; }

    const e = entities.get(siret);
    if (!e) { noMatch++; continue; }

    // --- Champs existants : l'API complète ce qui manque ---
    e.code_ape = firstNonEmpty(e.code_ape, d.api_naf);
    e.date_creation = firstNonEmpty(e.date_creation, d.api_date_creation);
    e.forme_juridique = firstNonEmpty(e.forme_juridique, d.api_nature_juridique);
    e.adresse = firstNonEmpty(e.adresse, d.api_adresse_complete);
    e.code_postal = firstNonEmpty(e.code_postal, d.api_code_postal);
    e.ville = firstNonEmpty(e.ville, d.api_commune);
    e.dirigeant_nom = firstNonEmpty(e.dirigeant_nom, d.api_dirigeant_nom);
    e.dirigeant_prenom = firstNonEmpty(e.dirigeant_prenom, d.api_dirigeant_prenom);
    if (!e.dirigeant_nom_complet && d.api_dirigeant_prenom && d.api_dirigeant_nom) {
      e.dirigeant_nom_complet = `${d.api_dirigeant_prenom} ${d.api_dirigeant_nom}`;
    }
    e.nom_entreprise = firstNonEmpty(e.nom_entreprise, d.api_nom_complet);

    // Effectif : mapper le code tranche vers un nombre lisible
    if (!e.nb_salaries && d.api_tranche_effectif && d.api_tranche_effectif !== "NN") {
      const trancheMap = {
        "00": "0", "01": "1-2", "02": "3-5", "03": "6-9",
        "11": "10-19", "12": "20-49", "21": "50-99", "22": "100-199",
        "31": "200-249", "32": "250-499", "41": "500-999", "42": "1000-1999",
        "51": "2000-4999", "52": "5000-9999", "53": "10000+",
      };
      e.nb_salaries = trancheMap[d.api_tranche_effectif] || d.api_tranche_effectif;
    }

    // Cross-validation RGE : si l'API dit RGE, on confirme
    if (d.api_liste_rge) {
      e.is_rge = "oui";
    }

    // --- Champs nouveaux (exclusifs API) ---
    e.api_etat_administratif = d.api_etat_administratif || "";
    e.api_date_fermeture = d.api_date_fermeture || "";
    e.api_nature_juridique = d.api_nature_juridique || "";
    e.api_categorie_entreprise = d.api_categorie_entreprise || "";
    e.api_section_activite = d.api_section_activite || "";
    e.api_naf = d.api_naf || "";
    e.api_naf25 = d.api_naf25 || "";
    e.api_libelle_naf = d.api_libelle_naf || "";
    e.api_tranche_effectif = d.api_tranche_effectif || "";
    e.api_caractere_employeur = d.api_caractere_employeur || "";
    e.api_dirigeant_qualite = d.api_dirigeant_qualite || "";
    e.api_liste_idcc = d.api_liste_idcc || "";
    e.api_liste_rge = d.api_liste_rge || "";
    e.api_departement = d.api_departement || "";
    e.api_region = d.api_region || "";

    if (!e.sources.includes("api_sirene")) e.sources.push("api_sirene");
    enriched++;
  }

  console.log(`    Enrichis:  ${enriched}`);
  console.log(`    Not found: ${notFound}`);
  console.log(`    No match:  ${noMatch} (SIRET dans API mais pas dans nos entités)`);

  return { enriched, notFound, noMatch };
}

// ============================================================
//  ÉTAPE 4 : Enrichissement depuis crawl des sites web
// ============================================================
function enrichFromWebsites(entities) {
  if (!fs.existsSync(WEBSITE_FILE)) {
    console.log("    Fichier .website_progress.json introuvable — skip");
    return { enriched: 0, newEmails: 0, newMobiles: 0, newTels: 0 };
  }

  const webData = JSON.parse(fs.readFileSync(WEBSITE_FILE, "utf-8"));
  let enriched = 0, newEmails = 0, newMobiles = 0, newTels = 0;

  for (const [siret, d] of Object.entries(webData)) {
    if (d._error || d._empty) continue;

    const e = entities.get(siret);
    if (!e) continue;

    const hasData = (d.emails && d.emails.length > 0) || (d.phones && d.phones.length > 0);
    if (!hasData) continue;

    // Store raw crawl data
    if (d.emails && d.emails.length > 0) e.website_emails = d.emails.join("|");
    if (d.mobiles && d.mobiles.length > 0) e.website_mobiles = d.mobiles.join("|");
    if (d.fixes && d.fixes.length > 0) e.website_fixes = d.fixes.join("|");

    // Fill missing email_generique with first crawled email
    if (!e.email_generique && d.emails && d.emails.length > 0) {
      e.email_generique = d.emails[0];
      newEmails++;
    }

    // Fill missing telephone: prefer mobile, fallback to fixe
    if (!e.telephone) {
      if (d.mobiles && d.mobiles.length > 0) {
        e.telephone = d.mobiles[0];
        newTels++;
        newMobiles++;
      } else if (d.fixes && d.fixes.length > 0) {
        e.telephone = d.fixes[0];
        newTels++;
      }
    } else {
      // Has a phone but it's a fixe — add mobile as dirigeant_telephone if empty
      const existingClean = e.telephone.replace(/[\s.\-]/g, "");
      if (!existingClean.match(/^0[67]/) && d.mobiles && d.mobiles.length > 0) {
        if (!e.dirigeant_telephone) {
          e.dirigeant_telephone = d.mobiles[0];
          newMobiles++;
        }
      }
    }

    if (!e.sources.includes("website")) e.sources.push("website");
    enriched++;
  }

  console.log(`    Enrichis:     ${enriched}`);
  console.log(`    Nvx emails:   ${newEmails}`);
  console.log(`    Nvx mobiles:  ${newMobiles}`);
  console.log(`    Nvx tels:     ${newTels}`);

  return { enriched, newEmails, newMobiles, newTels };
}

// ============================================================
//  ÉTAPE 5c : Nettoyage qualité
// ============================================================
function cleanupData(entities) {
  let fixedCP = 0, fixedEmail = 0, fixedTel = 0, fixedSiret = 0, fixedCA = 0, removed = 0;

  const toDelete = [];

  for (const [key, e] of entities) {
    // 1. Code postal pollué : "44140 LE BIGNON" → extraire les 5 chiffres
    if (e.code_postal && !/^\d{5}$/.test(e.code_postal)) {
      const match = e.code_postal.match(/(\d{5})/);
      if (match) {
        if (!e.ville) {
          const rest = e.code_postal.replace(match[1], "").trim();
          if (rest) e.ville = rest;
        }
        e.code_postal = match[1];
        fixedCP++;
      } else {
        // Pas de CP valide du tout → vider ([NON-DIFFUSIBLE], #REF!, [ND])
        e.code_postal = "";
        fixedCP++;
      }
    }
    // CP avec espace : "31 270" → "31270"
    if (e.code_postal && /^\d{2}\s\d{3}$/.test(e.code_postal)) {
      e.code_postal = e.code_postal.replace(/\s/g, "");
      fixedCP++;
    }

    // 2. Emails malformés
    for (const f of ["email_generique", "email_dirigeant"]) {
      if (!e[f]) continue;
      const em = e[f].trim();
      if (/^\d+$/.test(em)) { e[f] = ""; fixedEmail++; }                    // numéros
      else if (/^["':\s]/.test(em)) { e[f] = em.replace(/^["':\s]+/, ""); fixedEmail++; } // préfixe ": "
      else if (/@@/.test(em)) { e[f] = em.replace(/@@/g, "@"); fixedEmail++; }  // double @
      else if (/\.\s*$/.test(em) || !/\.\w{2,}$/.test(em)) { e[f] = ""; fixedEmail++; } // domaine tronqué
    }

    // 3. Téléphones : normalisation +33, ajout 0 initial
    for (const f of ["telephone", "dirigeant_telephone"]) {
      if (!e[f]) continue;
      let clean = e[f].replace(/[\s.\-()]/g, "");
      // +33 → 0
      if (/^\+33/.test(clean)) {
        clean = "0" + clean.slice(3);
        e[f] = clean;
        fixedTel++;
      }
      // 0033 → 0
      else if (/^0033/.test(clean)) {
        clean = "0" + clean.slice(4);
        e[f] = clean;
        fixedTel++;
      }
      // 9 chiffres sans 0 initial
      else if (/^[1-9]\d{8}$/.test(clean)) {
        e[f] = "0" + clean;
        fixedTel++;
      }
    }

    // 4. Sites web invalides (réseaux sociaux, annuaires, domaines manifestement faux)
    if (e.site_web) {
      const sw = e.site_web.toLowerCase().replace(/^https?:\/\//, '').replace(/^www\./, '');
      const BAD_DOMAINS = [
        'facebook.com', 'm.facebook.com', 'fr-fr.facebook.com', 'l.facebook.com',
        'instagram.com', 'twitter.com', 'linkedin.com',
        'pagesjaunes.fr', 'pro.pagesjaunes.fr', 'solocal.com',
        'paramountpictures', 'netflix.com', 'amazon.',
        'google.com', 'youtube.com',
      ];
      const BAD_PATTERNS = [/^facebook/i, /^instagram/i, /^@/, /facebook/i];
      const isBad = BAD_DOMAINS.some(d => sw.startsWith(d) || sw.includes(d))
                 || BAD_PATTERNS.some(p => p.test(e.site_web.trim()));
      if (isBad) { e.site_web = ""; fixedEmail++; } // fixedEmail réutilisé comme compteur générique
    }

    // 5. SIRET invalides (parsing cassé)
    if (e.siret && !/^\d{14}$/.test(e.siret)) {
      e.siret = "";
      e.siren = "";
      fixedSiret++;
    }

    // 5. CA non fiables → vider
    if (e.chiffre_affaires) {
      const ca = String(e.chiffre_affaires).trim();
      const caNum = Number(ca);
      if (
        ca.includes("USD") ||                    // ranges LinkedIn en USD
        (ca === "1") ||                           // placeholder
        (/^\d+$/.test(ca) && caNum < 10000) ||    // valeurs absurdes < 10K€ pour un artisan
        (/^\d{9,}$/.test(ca) && caNum > 100000000) // SIREN collé par erreur (>100M)
      ) {
        e.chiffre_affaires = "";
        fixedCA++;
      }
    }

    // 6. Entités fantômes (pas de nom, ou SIRET cassé contenant des virgules)
    if (!e.nom_entreprise || (e.siret && e.siret.includes(","))) {
      toDelete.push(key);
      removed++;
    }
  }

  for (const key of toDelete) entities.delete(key);

  console.log(`  CP corrigés:         ${fixedCP}`);
  console.log(`  Emails nettoyés:     ${fixedEmail}`);
  console.log(`  Tels corrigés (+0):  ${fixedTel}`);
  console.log(`  SIRET invalides:     ${fixedSiret}`);
  console.log(`  CA non fiables vidés:${fixedCA}`);
  console.log(`  Entités supprimées:  ${removed}`);

  return { fixedCP, fixedEmail, fixedTel, fixedSiret, fixedCA, removed };
}

// ============================================================
//  ÉTAPE 5 : Score de complétude
// ============================================================
function computeCompleteness(entities) {
  const fields = [
    "siret", "nom_entreprise", "email_generique", "email_dirigeant", "telephone",
    "site_web", "adresse", "code_postal", "ville", "dirigeant_nom_complet",
    "chiffre_affaires", "nb_salaries", "code_ape", "date_creation",
    "assurance_rc", "assurance_dc", "linkedin_profil", "latitude", "longitude",
    "api_etat_administratif", "api_naf", "api_categorie_entreprise",
  ];

  for (const [, e] of entities) {
    let filled = 0;
    for (const f of fields) if (e[f]) filled++;
    e.score_completude = Math.round((filled / fields.length) * 100);
    e.nb_sources = e.sources.length;
    e.sources = e.sources.join("|");
  }
}

// ============================================================
//  ÉTAPE 5b : Ajouter les profils LinkedIn non matchés comme entités séparées
// ============================================================
function addUnmatchedLinkedin(unmatchedRows, entities) {
  let added = 0;
  let skipped = 0;

  for (const row of unmatchedRows) {
    // Helper: safely get string from any XLSX field (could be number)
    const s = (v) => String(v || "").trim();

    const url = s(row.linkedin_profile_url);
    if (!url) { skipped++; continue; }
    const companyName = s(row.company_name);
    if (!companyName) { skipped++; continue; }

    // Clé = LinkedIn URL (pas de SIRET)
    const key = "LI_" + url.replace(/[^a-zA-Z0-9]/g, "").slice(-20);
    if (entities.has(key)) { skipped++; continue; }

    const e = createEntity();
    e.nom_entreprise = companyName;
    e.source_only_linkedin = "oui";

    // Dirigeant
    e.dirigeant_prenom = s(row.first_name);
    e.dirigeant_nom = s(row.last_name);
    e.dirigeant_nom_complet = s(row.full_name);

    // Email
    const emailDirect = s(row["e-mail direct"]);
    if (emailDirect && emailDirect.includes("@")) {
      e.email_dirigeant = emailDirect;
    }

    // Site web
    e.site_web = firstNonEmpty(s(row.website), s(row.company_url));

    // LinkedIn
    e.linkedin_profil = url;
    e.linkedin_page_entreprise = s(row["linkedin page"]);
    e.linkedin_job_title = s(row.job_title);
    e.linkedin_headline = s(row.headline);
    e.linkedin_location = s(row.location);
    e.linkedin_hq = s(row.HQ);
    e.linkedin_description = s(row.description);
    e.linkedin_category = s(row.Category);
    e.linkedin_ca_estime = s(row["CA estimé"]);
    e.linkedin_employee = s(row.employee);
    e.linkedin_creation = s(row["création"]);
    e.linkedin_source_metier = s(row.Source);
    e.linkedin_match_method = "standalone";
    e.linkedin_match_confidence = "";
    e.linkedin_match_score = "";

    // Localisation depuis HQ ou location
    const hq = s(row.HQ);
    if (hq) {
      const cpMatch = hq.match(/(\d{5})\s+/);
      if (cpMatch) e.code_postal = cpMatch[1];
      e.adresse = hq;
    }
    const loc = s(row.location);
    if (loc && !e.ville) {
      // location format: "Paris, Île-de-France, France"
      const parts = loc.split(",");
      if (parts.length > 0) e.ville = parts[0].trim();
    }

    e.sources = ["linkedin"];
    e.nb_sources = 1;

    entities.set(key, e);
    added++;
  }

  console.log(`  ${added} profils ajoutés comme entités LinkedIn-only`);
  console.log(`  ${skipped} ignorés (pas d'URL ou pas de nom d'entreprise)`);
  return { added, skipped };
}

// ============================================================
//  EXPORT
// ============================================================
function exportCSV(entities) {
  // Trier : plus de sources d'abord, puis par score complétude
  const sorted = [...entities.values()].sort((a, b) => {
    if (b.nb_sources !== a.nb_sources) return b.nb_sources - a.nb_sources;
    return b.score_completude - a.score_completude;
  });

  const keys = Object.keys(createEntity());
  const header = keys.map(escapeCSV).join(",");
  const lines = [header];

  const linkedinOnly = [];
  for (const e of sorted) {
    // Exclure les entités LinkedIn-only de l'export principal (non vérifiées par SIRET)
    if (e.sources === "linkedin" || (Array.isArray(e.sources) && e.sources.join("|") === "linkedin")) {
      linkedinOnly.push(e);
      continue;
    }
    lines.push(keys.map(k => escapeCSV(e[k])).join(","));
  }

  fs.writeFileSync(OUTPUT_FILE, "\ufeff" + lines.join("\n"), "utf-8");
  console.log(`  LinkedIn-only exclus de l'export principal: ${linkedinOnly.length}`);
  return sorted.filter(e => !(e.sources === "linkedin" || (Array.isArray(e.sources) && e.sources.join("|") === "linkedin")));
}

function exportUnmatched(rows) {
  if (rows.length === 0) return;
  const keys = Object.keys(rows[0]);
  const lines = [keys.map(escapeCSV).join(",")];
  for (const r of rows) lines.push(keys.map(k => escapeCSV(r[k])).join(","));
  fs.writeFileSync(UNMATCHED_FILE, lines.join("\n"), "utf-8");
}

// ============================================================
//  MAIN
// ============================================================
async function main() {
  console.log("=== RÉCONCILIATION DATA ENGINEER ===\n");
  console.log("Phase 1: Chargement des sources\n");

  const capeb = loadCAPEB();
  console.log("  Chargement Qualibat...");
  const qualibat = loadXLSXDedup(SOURCES.qualibat, "Siret");
  console.log("  Chargement QualiENR...");
  const qualienr = loadXLSXDedup(SOURCES.qualienr, "siret");
  console.log("  Chargement Qualifelec...");
  const qualifelec = loadXLSXDedup(SOURCES.qualifelec, "Siret");
  console.log("  Chargement LinkedIn...");
  const linkedin = loadLinkedinDedup();

  console.log("\nPhase 2: Construction du dictionnaire d'entités (clé = SIRET)\n");
  const { entities, sirenToSiret } = buildEntityMap(capeb, qualibat, qualienr, qualifelec);
  console.log(`\n  TOTAL ENTITÉS: ${entities.size}`);

  // Stats cross-source
  let both_qb_qe = 0, both_qb_qf = 0, both_qe_qf = 0, all_three = 0;
  for (const [, e] of entities) {
    const has = { qb: e.qualibat === "oui", qe: e.qualienr === "oui", qf: e.qualifelec === "oui" };
    if (has.qb && has.qe) both_qb_qe++;
    if (has.qb && has.qf) both_qb_qf++;
    if (has.qe && has.qf) both_qe_qf++;
    if (has.qb && has.qe && has.qf) all_three++;
  }
  console.log(`  Doublons cross-source:`);
  console.log(`    Qualibat + QualiENR:           ${both_qb_qe}`);
  console.log(`    Qualibat + Qualifelec:         ${both_qb_qf}`);
  console.log(`    QualiENR + Qualifelec:         ${both_qe_qf}`);
  console.log(`    Les 3 certifs:                 ${all_three}`);

  console.log("\nPhase 3: Fuzzy matching LinkedIn\n");
  const liResult = matchLinkedin(linkedin, entities);
  console.log(`    Matchés: ${liResult.matched} | Non matchés: ${liResult.unmatched}`);

  console.log("\nPhase 4: Enrichissement API SIRENE\n");
  const apiResult = enrichFromAPI(entities);

  console.log("\nPhase 5: Enrichissement depuis crawl sites web\n");
  const webResult = enrichFromWebsites(entities);

  console.log("\nPhase 6: Ajout des profils LinkedIn non matchés\n");
  const liUnmatchedResult = addUnmatchedLinkedin(liResult.unmatchedRows, entities);

  console.log("\nPhase 7: Nettoyage qualité\n");
  const cleanResult = cleanupData(entities);

  console.log("\nPhase 8: Score de complétude\n");
  computeCompleteness(entities);

  // Stats finales
  const stats = { total_entities: entities.size };
  const sourceCounts = { capeb: 0, qualibat: 0, qualienr: 0, qualifelec: 0, linkedin: 0, api_sirene: 0, website: 0 };
  const nbSourcesDist = {};
  let totalScore = 0;
  for (const [, e] of entities) {
    for (const src of e.sources.split("|")) if (sourceCounts[src] !== undefined) sourceCounts[src]++;
    const n = e.nb_sources;
    nbSourcesDist[n] = (nbSourcesDist[n] || 0) + 1;
    totalScore += e.score_completude;
  }
  stats.sources = sourceCounts;
  stats.nb_sources_distribution = nbSourcesDist;
  stats.cross_source = { both_qb_qe, both_qb_qf, both_qe_qf, all_three };
  stats.linkedin = { matched: liResult.matched, unmatched: liResult.unmatched, standalone_added: liUnmatchedResult.added };
  stats.api_sirene = apiResult;
  stats.website_crawl = webResult;
  stats.cleanup = cleanResult;

  console.log("  === RÉSULTATS FINAUX ===\n");
  console.log(`  Entités uniques:       ${entities.size}`);
  console.log(`  Par source:`);
  for (const [src, count] of Object.entries(sourceCounts)) {
    console.log(`    ${src.padEnd(15)} ${count}`);
  }
  console.log(`  Par nb de sources:`);
  for (const [n, count] of Object.entries(nbSourcesDist).sort((a,b) => b[0]-a[0])) {
    console.log(`    ${n} source(s):       ${count}`);
  }
  stats.avg_completeness = Math.round(totalScore / entities.size);

  // Stats API spécifiques
  let fermees = 0, rgeApi = 0;
  for (const [, e] of entities) {
    if (e.api_etat_administratif === "F") fermees++;
    if (e.api_liste_rge) rgeApi++;
  }
  console.log(`  Score complétude moyen: ${stats.avg_completeness}%`);
  console.log(`  Entreprises fermées:   ${fermees} (état admin = F)`);
  console.log(`  RGE confirmés API:     ${rgeApi}`);

  console.log("\nPhase 9: Export\n");
  const sorted = exportCSV(entities);
  console.log(`  ${OUTPUT_FILE}`);
  console.log(`  ${sorted.length} lignes, ${Object.keys(createEntity()).length} colonnes`);

  // LinkedIn non matchés maintenant intégrés comme entités standalone (Phase 6)
  // exportUnmatched(liResult.unmatchedRows);
  console.log(`  LinkedIn standalone: ${liUnmatchedResult.added} entités ajoutées`);

  fs.writeFileSync(STATS_FILE, JSON.stringify(stats, null, 2));
  console.log(`  ${STATS_FILE}`);

  // Top 5 entités les plus complètes
  console.log("\n  TOP 5 entités les plus riches:");
  for (let i = 0; i < Math.min(5, sorted.length); i++) {
    const e = sorted[i];
    console.log(`    ${i+1}. ${e.nom_entreprise} | ${e.nb_sources} sources | ${e.score_completude}% | ${e.sources}`);
  }
}

main().catch(console.error);
