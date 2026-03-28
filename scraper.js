const fs = require("fs");
const path = require("path");

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// ─── CONFIG ─────────────────────────────────────────────────────────────────
const BASE_URL =
  "https://api.artisans-du-batiment-by-capeb.com/elastic-search";
const PER_PAGE = 24;
const MAX_PAGE = 416; // ES 10k limit: 24*416 = 9984
const CONCURRENCY = 10; // parallel requests
const RADIUS_KM = 55; // search radius per grid point
const GRID_STEP_KM = 75; // distance between grid points (~75km)
const OUTPUT_FILE = path.join(__dirname, "artisans_capeb.csv");
const PROGRESS_FILE = path.join(__dirname, "progress.json");

// ─── GEO GRID ───────────────────────────────────────────────────────────────
// Metropolitan France bounding box
const FRANCE_METRO = {
  latMin: 42.3,
  latMax: 51.2,
  lonMin: -5.2,
  lonMax: 9.7,
};

// DOM-TOM centers (single point + big radius each)
const DOM_TOM = [
  { name: "Guadeloupe", lat: 16.25, lon: -61.55, radius: 80 },
  { name: "Martinique", lat: 14.64, lon: -61.02, radius: 60 },
  { name: "Guyane", lat: 3.93, lon: -53.13, radius: 300 },
  { name: "Reunion", lat: -21.12, lon: 55.53, radius: 80 },
  { name: "Mayotte", lat: -12.78, lon: 45.15, radius: 50 },
  { name: "Corse", lat: 42.15, lon: 9.1, radius: 100 },
];

function kmToLatDeg(km) {
  return km / 111.32;
}
function kmToLonDeg(km, lat) {
  return km / (111.32 * Math.cos((lat * Math.PI) / 180));
}

function generateGrid() {
  const points = [];

  // Metropolitan France grid
  const latStep = kmToLatDeg(GRID_STEP_KM);
  for (let lat = FRANCE_METRO.latMin; lat <= FRANCE_METRO.latMax; lat += latStep) {
    const lonStep = kmToLonDeg(GRID_STEP_KM, lat);
    for (let lon = FRANCE_METRO.lonMin; lon <= FRANCE_METRO.lonMax; lon += lonStep) {
      points.push({
        lat: Math.round(lat * 1000) / 1000,
        lon: Math.round(lon * 1000) / 1000,
        radius: RADIUS_KM,
      });
    }
  }

  // DOM-TOM points
  for (const dt of DOM_TOM) {
    points.push({ lat: dt.lat, lon: dt.lon, radius: dt.radius });
  }

  return points;
}

// ─── FETCH WITH RETRY ───────────────────────────────────────────────────────
async function fetchPage(lat, lon, distance, page, retries = 3) {
  const url = `${BASE_URL}?q=&limit=${PER_PAGE}&page=${page}&lat=${lat}&lon=${lon}&distance=${distance}`;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const resp = await fetch(url);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      return data.items || [];
    } catch (err) {
      if (attempt === retries) {
        console.error(`  FAIL ${url}: ${err.message}`);
        return [];
      }
      await new Promise((r) => setTimeout(r, 1000 * attempt));
    }
  }
}

// ─── SCRAPE ONE GRID CELL ───────────────────────────────────────────────────
async function scrapeCell(lat, lon, radius, allIds) {
  let page = 1;
  let newCount = 0;
  let totalFetched = 0;

  while (page <= MAX_PAGE) {
    // Fetch batch of pages concurrently
    const batchSize = Math.min(5, MAX_PAGE - page + 1);
    const pages = Array.from({ length: batchSize }, (_, i) => page + i);
    const results = await Promise.all(
      pages.map((p) => fetchPage(lat, lon, radius, p))
    );

    let gotResults = false;
    for (const items of results) {
      if (!items || items.length === 0) continue;
      gotResults = true;
      totalFetched += items.length;
      for (const item of items) {
        if (!allIds.has(item.id)) {
          allIds.set(item.id, item);
          newCount++;
        }
      }
    }

    if (!gotResults) break;
    page += batchSize;
  }

  return { newCount, totalFetched };
}

// ─── PARALLEL EXECUTION WITH CONCURRENCY ────────────────────────────────────
async function runWithConcurrency(tasks, concurrency) {
  const results = [];
  let index = 0;

  async function worker() {
    while (index < tasks.length) {
      const i = index++;
      results[i] = await tasks[i]();
    }
  }

  const workers = Array.from({ length: concurrency }, () => worker());
  await Promise.all(workers);
  return results;
}

// ─── CSV EXPORT ─────────────────────────────────────────────────────────────
function escapeCSV(val) {
  if (val == null) return "";
  const str = String(val);
  if (str.includes(",") || str.includes('"') || str.includes("\n")) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function exportCSV(allIds) {
  const headers = [
    "id",
    "siret",
    "name",
    "fullName",
    "legalCode",
    "membership",
    "address",
    "address2",
    "address3",
    "zipCode",
    "city",
    "phone",
    "fax",
    "website",
    "email",
    "isRGE",
    "speciality",
    "mainActivities",
    "secondaryActivities",
    "latitude",
    "longitude",
    "slug",
    "labels",
    "createdAt",
    "updatedAt",
  ];

  const rows = [headers.join(",")];

  for (const [, item] of allIds) {
    const row = [
      item.id,
      item.siret,
      item.name,
      item.fullName,
      item.legalCode,
      item.membership,
      item.address,
      item.address2,
      item.address3,
      item.zipCode,
      item.city,
      item.phone,
      item.fax,
      item.website,
      item.email,
      item.isRGE,
      item.speciality,
      (item.mainActivities || []).map((a) => a.name).join(" | "),
      (item.secondaryActivities || []).map((a) => a.name).join(" | "),
      item.location?.lat,
      item.location?.lon,
      item.slug,
      (item.labelsForCompanies || []).map((l) => l.description || l.name || "").join(" | "),
      item.createdAt,
      item.updatedAt,
    ].map(escapeCSV);
    rows.push(row.join(","));
  }

  fs.writeFileSync(OUTPUT_FILE, rows.join("\n"), "utf-8");
}

// ─── SAVE/LOAD PROGRESS ────────────────────────────────────────────────────
function saveProgress(allIds, completedCells) {
  // Save artisans as JSON for resume capability
  const data = {
    completedCells,
    artisanCount: allIds.size,
    artisans: Object.fromEntries(allIds),
  };
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(data), "utf-8");
}

function loadProgress() {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      const raw = fs.readFileSync(PROGRESS_FILE, "utf-8");
      const data = JSON.parse(raw);
      const allIds = new Map(Object.entries(data.artisans).map(([k, v]) => [Number(k), v]));
      console.log(`Resuming: ${allIds.size} artisans from ${data.completedCells} cells`);
      return { allIds, completedCells: data.completedCells };
    }
  } catch (e) {
    console.log("No valid progress file, starting fresh");
  }
  return { allIds: new Map(), completedCells: 0 };
}

// ─── MAIN ───────────────────────────────────────────────────────────────────
async function main() {
  console.log("=== CAPEB Artisans Scraper ===\n");

  const grid = generateGrid();
  console.log(`Grid points: ${grid.length}`);
  console.log(`Radius: ${RADIUS_KM}km | Grid step: ${GRID_STEP_KM}km`);
  console.log(`Concurrency: ${CONCURRENCY} parallel cells\n`);

  let { allIds, completedCells } = loadProgress();
  const startTime = Date.now();
  const remainingGrid = grid.slice(completedCells);

  let cellsDone = completedCells;
  const totalCells = grid.length;

  const tasks = remainingGrid.map((point, idx) => {
    return async () => {
      const cellIdx = completedCells + idx + 1;
      const { newCount, totalFetched } = await scrapeCell(
        point.lat,
        point.lon,
        point.radius,
        allIds
      );
      cellsDone++;

      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      const pct = ((cellsDone / totalCells) * 100).toFixed(1);
      console.log(
        `[${cellsDone}/${totalCells}] (${pct}%) ` +
          `lat=${point.lat} lon=${point.lon} r=${point.radius}km | ` +
          `fetched=${totalFetched} new=${newCount} | ` +
          `TOTAL: ${allIds.size} unique | ${elapsed}s`
      );

      // Save progress every 20 cells
      if (cellsDone % 20 === 0) {
        saveProgress(allIds, cellsDone);
        exportCSV(allIds);
        console.log(`  >> Progress saved: ${allIds.size} artisans to CSV`);
      }
    };
  });

  await runWithConcurrency(tasks, CONCURRENCY);

  // Final save
  exportCSV(allIds);
  console.log(`\n=== DONE ===`);
  console.log(`Total unique artisans: ${allIds.size}`);
  console.log(`Output: ${OUTPUT_FILE}`);
  console.log(`Duration: ${((Date.now() - startTime) / 1000 / 60).toFixed(1)} minutes`);

  // Cleanup progress file
  if (fs.existsSync(PROGRESS_FILE)) {
    fs.unlinkSync(PROGRESS_FILE);
  }
}

main().catch(console.error);
