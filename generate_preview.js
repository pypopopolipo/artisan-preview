const fs = require("fs");
const lines = fs.readFileSync("artisans_unified.csv", "utf-8").split("\n");
const h = lines[0].split(",");

function parseCSV(line) {
  const f = []; let c = "", q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (q) { if (ch === '"' && line[i + 1] === '"') { c += '"'; i++; } else if (ch === '"') q = false; else c += ch; }
    else { if (ch === '"') q = true; else if (ch === ",") { f.push(c); c = ""; } else c += ch; }
  }
  f.push(c); return f;
}

const idx = {};
for (const f of h) idx[f] = h.indexOf(f);

// Collect stats by region
const regions = {};
const departments = {};
const certifCombos = {};
const rgeByRegion = {};
let totalRGE = 0;

// Sample: pick 30 rich entities (high score, multiple sources)
const samples = [];
const allRows = [];

for (let i = 1; i < lines.length; i++) {
  if (!lines[i].trim()) continue;
  const row = parseCSV(lines[i]);
  const obj = {};
  for (const f of h) obj[f] = (row[idx[f]] || "").trim();
  allRows.push(obj);

  // Region stats
  const region = obj.api_region || "Non renseigné";
  regions[region] = (regions[region] || 0) + 1;

  const dept = obj.api_departement || "";
  if (dept) departments[dept] = (departments[dept] || 0) + 1;

  // RGE by region
  if (obj.is_rge === "oui") {
    totalRGE++;
    rgeByRegion[region] = (rgeByRegion[region] || 0) + 1;
  }

  // Certif combos
  const certifs = [];
  if (obj.qualibat === "oui") certifs.push("Qualibat");
  if (obj.qualienr === "oui") certifs.push("QualiENR");
  if (obj.qualifelec === "oui") certifs.push("Qualifelec");
  if (certifs.length > 0) {
    const key = certifs.join(" + ");
    certifCombos[key] = (certifCombos[key] || 0) + 1;
  }
}

// Pick sample: top 30 by score + sources
allRows.sort((a, b) => {
  const sa = parseInt(b.nb_sources) - parseInt(a.nb_sources);
  if (sa !== 0) return sa;
  return parseInt(b.score_completude) - parseInt(a.score_completude);
});

const UNLOCKED_COUNT = 3; // First N fiches are fully visible
for (let i = 0; i < 30 && i < allRows.length; i++) {
  const r = allRows[i];
  const unlocked = i < UNLOCKED_COUNT;
  samples.push({
    nom: r.nom_entreprise,
    ville: r.ville,
    dept: r.api_departement,
    region: r.api_region,
    activite: r.api_libelle_naf || r.specialite || "",
    rge: r.is_rge,
    // Contact: full or masked
    email: unlocked ? (r.email_generique || r.email_dirigeant || "") : (r.email_generique ? "***@" + r.email_generique.split("@")[1] : ""),
    tel: unlocked ? (r.telephone || "") : (r.telephone ? r.telephone.substring(0, 6) + "XX XX" : ""),
    dirigeant: unlocked ? (r.dirigeant_prenom && r.dirigeant_nom ? r.dirigeant_prenom + " " + r.dirigeant_nom : r.dirigeant_nom_complet || "") : (r.dirigeant_prenom ? r.dirigeant_prenom.charAt(0) + ". " + r.dirigeant_nom : ""),
    sources: r.nb_sources,
    score: r.score_completude,
    qualibat: r.qualibat,
    qualienr: r.qualienr,
    qualifelec: r.qualifelec,
    ca: r.chiffre_affaires && parseInt(r.chiffre_affaires) >= 10000 ? r.chiffre_affaires : "",
    salaries: r.nb_salaries,
    site_web: unlocked ? (r.site_web || "") : (r.site_web ? "oui" : ""),
    linkedin: unlocked ? (r.linkedin_profil || "") : (r.linkedin_profil ? "oui" : ""),
    unlocked: unlocked,
    // Extra phones for unlocked
    website_mobiles: unlocked ? (r.website_mobiles || "") : "",
    website_fixes: unlocked ? (r.website_fixes || "") : "",
    website_emails: unlocked ? (r.website_emails || "") : "",
    // Extra fields for unlocked fiches
    siret: unlocked ? r.siret : "",
    siren: unlocked ? r.siren : "",
    code_ape: unlocked ? r.code_ape : "",
    forme_juridique: unlocked ? r.forme_juridique : "",
    date_creation: unlocked ? r.date_creation : "",
    adresse: unlocked ? r.adresse : "",
    code_postal: unlocked ? r.code_postal : "",
    email_dirigeant: unlocked ? (r.email_dirigeant || "") : "",
    dirigeant_telephone: unlocked ? (r.dirigeant_telephone || "") : "",
    fax: unlocked ? (r.fax || "") : "",
    assurance_rc: unlocked ? (r.assurance_rc || "") : "",
    assurance_dc: unlocked ? (r.assurance_dc || "") : "",
    solvabilite: unlocked ? (r.solvabilite || "") : "",
    qualibat_type: unlocked ? (r.qualibat_type || "") : "",
    qualienr_certifications: unlocked ? (r.qualienr_certifications || "") : "",
    qualifelec_rge: unlocked ? (r.qualifelec_rge || "") : "",
    api_categorie_entreprise: unlocked ? (r.api_categorie_entreprise || "") : "",
    api_libelle_naf: unlocked ? (r.api_libelle_naf || "") : "",
    api_liste_rge: unlocked ? (r.api_liste_rge || "") : "",
    linkedin_job_title: unlocked ? (r.linkedin_job_title || "") : "",
    linkedin_headline: unlocked ? (r.linkedin_headline || "") : "",
  });
}

// Top 15 regions
const topRegions = Object.entries(regions)
  .filter(([r]) => r !== "Non renseigné")
  .sort((a, b) => b[1] - a[1])
  .slice(0, 15)
  .map(([name, count]) => ({ name, count, rge: rgeByRegion[name] || 0 }));

// Top 20 departments
const topDepts = Object.entries(departments)
  .sort((a, b) => b[1] - a[1])
  .slice(0, 20)
  .map(([name, count]) => ({ name, count }));

// Certif combos
const certifList = Object.entries(certifCombos)
  .sort((a, b) => b[1] - a[1]);

const preview = {
  total: allRows.length,
  totalRGE,
  regions: topRegions,
  departments: topDepts,
  certifications: certifList,
  samples,
  stats: {
    emails: allRows.filter(r => r.email_generique || r.email_dirigeant).length,
    telephones: allRows.filter(r => r.telephone).length,
    mobiles: allRows.filter(r => r.telephone && r.telephone.replace(/[\s.\-]/g, "").match(/^0[67]/)).length,
    sites_web: allRows.filter(r => r.site_web).length,
    dirigeants: allRows.filter(r => r.dirigeant_nom).length,
    rge: totalRGE,
    qualibat: allRows.filter(r => r.qualibat === "oui").length,
    qualienr: allRows.filter(r => r.qualienr === "oui").length,
    qualifelec: allRows.filter(r => r.qualifelec === "oui").length,
    with_ca: allRows.filter(r => r.chiffre_affaires).length,
    with_salaries: allRows.filter(r => r.nb_salaries).length,
    with_assurance: allRows.filter(r => r.assurance_rc).length,
    linkedin: allRows.filter(r => r.linkedin_profil).length,
  },
};

fs.writeFileSync("preview_data.json", JSON.stringify(preview, null, 2));
console.log("preview_data.json generated");
console.log("Samples:", samples.length);
console.log("Regions:", topRegions.length);
