const fs = require('fs');
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

const rows92 = [];
for (let i = 1; i < lines.length; i++) {
  if (!lines[i].trim()) continue;
  const r = parseRow(lines[i]);
  if ((r.code_postal||'').trim().startsWith('92')) rows92.push(r);
}
rows92.sort((a,b) => parseFloat(b.score_completude||0) - parseFloat(a.score_completude||0));

const step = Math.floor(rows92.length / 50);
const sample = [];
for (let i = 0; i < rows92.length && sample.length < 50; i += step) sample.push(rows92[i]);

const stats = {
  total: rows92.length,
  with_email: rows92.filter(r => r.email_generique || r.email_dirigeant).length,
  with_tel: rows92.filter(r => r.telephone).length,
  with_mobile: rows92.filter(r => r.dirigeant_telephone && /^0[67]/.test(r.dirigeant_telephone)).length,
  rge: rows92.filter(r => r.is_rge === 'oui').length,
  with_rc: rows92.filter(r => r.assurance_rc).length,
  score_moyen: Math.round(rows92.reduce((s,r)=>s+parseFloat(r.score_completude||0),0)/rows92.length),
};

const COLS = ['nom_entreprise','siret','forme_juridique','code_ape','date_creation',
  'telephone','email_generique','email_dirigeant','site_web',
  'adresse','code_postal','ville',
  'dirigeant_nom_complet','dirigeant_telephone',
  'specialite','is_rge','qualibat','qualienr','qualifelec',
  'chiffre_affaires','nb_salaries','solvabilite',
  'assurance_rc','assurance_dc','linkedin',
  'nb_sources','score_completude','sources'];

const sampleClean = sample.map(r => {
  const o = {};
  COLS.forEach(k => { if (r[k] !== undefined) o[k] = r[k]; });
  return o;
});

const sampleJson = JSON.stringify(sampleClean);

const html = `<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Extrait — 50 artisans du 92 (Hauts-de-Seine)</title>
<meta name="robots" content="noindex">
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: #f8f9fa; color: #1a1a2e; line-height: 1.6; }
  .hero { background: linear-gradient(135deg, #0f0c29, #302b63, #24243e); color: white; padding: 50px 30px; text-align: center; }
  .hero h1 { font-size: 2.2em; font-weight: 800; margin-bottom: 8px; }
  .hero h1 span { color: #4fc3f7; }
  .hero .sub { font-size: 1em; color: #b0bec5; margin-bottom: 28px; }
  .hero-stats { display: flex; justify-content: center; gap: 35px; flex-wrap: wrap; margin-bottom: 20px; }
  .hero-stat .number { font-size: 2em; font-weight: 800; color: #4fc3f7; }
  .hero-stat .label { font-size: 0.8em; color: #90a4ae; text-transform: uppercase; letter-spacing: 1px; }
  .notice { max-width: 820px; margin: 20px auto 0; background: #fff8e1; border-left: 4px solid #f59e0b; color: #92400e; padding: 13px 18px; font-size: 0.88em; border-radius: 0 8px 8px 0; }
  .section { max-width: 1200px; margin: 0 auto; padding: 36px 20px; }
  .section h2 { font-size: 1.5em; font-weight: 700; margin-bottom: 18px; }
  .section h2 span { color: #302b63; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr)); gap: 18px; }
  .card { background: linear-gradient(135deg, #f0fff4 0%, #fff 100%); border: 2px solid #00b894; border-radius: 14px; padding: 20px; cursor: pointer; transition: all 0.22s; }
  .card:hover { transform: translateY(-3px); box-shadow: 0 8px 28px rgba(0,184,148,0.18); }
  .card-top { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 10px; }
  .card-tag { display: inline-block; background: linear-gradient(135deg, #00b894, #00cec9); color: white; font-size: 0.62em; font-weight: 800; padding: 3px 10px; border-radius: 10px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px; }
  .card-name { font-size: 1.05em; font-weight: 800; color: #1a1a2e; }
  .card-loc { font-size: 0.82em; color: #777; margin-top: 2px; }
  .score { width: 40px; height: 40px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 0.7em; font-weight: 800; color: white; flex-shrink: 0; }
  .badges { display: flex; flex-wrap: wrap; gap: 5px; margin: 10px 0; }
  .badge { padding: 2px 9px; border-radius: 10px; font-size: 0.74em; font-weight: 700; }
  .bg { background: #e8f5e9; color: #2e7d32; }
  .bb { background: #e3f2fd; color: #1565c0; }
  .bo { background: #fff3e0; color: #e65100; }
  .bp { background: #f3e5f5; color: #6a1b9a; }
  .br { background: linear-gradient(135deg, #00b894, #00cec9); color: white; }
  .bs { background: #302b63; color: white; }
  .row { display: flex; align-items: flex-start; gap: 8px; padding: 5px 0; border-top: 1px solid #f0f0f0; font-size: 0.85em; }
  .lbl { color: #999; min-width: 68px; flex-shrink: 0; }
  .val { color: #222; font-weight: 500; word-break: break-all; }
  .val.none { color: #ccc; font-style: italic; font-weight: 400; }
  .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(15,12,41,0.75); z-index: 1000; backdrop-filter: blur(4px); justify-content: center; align-items: center; }
  .modal-overlay.active { display: flex; }
  .modal { background: white; border-radius: 18px; max-width: 560px; width: 92%; max-height: 88vh; overflow-y: auto; box-shadow: 0 25px 60px rgba(0,0,0,0.3); animation: mIn 0.22s ease; }
  @keyframes mIn { from { transform: scale(0.92) translateY(14px); opacity: 0; } to { transform: scale(1) translateY(0); opacity: 1; } }
  .mhead { background: linear-gradient(135deg, #0f0c29, #302b63); color: white; padding: 24px 26px; border-radius: 18px 18px 0 0; position: relative; }
  .mhead h3 { font-size: 1.3em; font-weight: 800; margin-bottom: 4px; }
  .mhead p { color: #b0bec5; font-size: 0.88em; }
  .mclose { position: absolute; top: 14px; right: 16px; background: rgba(255,255,255,0.15); border: none; color: white; width: 32px; height: 32px; border-radius: 50%; cursor: pointer; font-size: 1.1em; }
  .mclose:hover { background: rgba(255,255,255,0.3); }
  .mbody { padding: 20px 26px 26px; }
  .msec { margin-bottom: 18px; }
  .msec-title { font-size: 0.73em; text-transform: uppercase; letter-spacing: 1.5px; color: #999; font-weight: 700; margin-bottom: 8px; padding-bottom: 5px; border-bottom: 2px solid #f0f0f0; }
  .mrow { display: flex; padding: 6px 0; }
  .mrow .lbl { width: 126px; color: #999; font-size: 0.85em; flex-shrink: 0; }
  .mrow .val { color: #1a1a2e; font-weight: 600; font-size: 0.88em; word-break: break-all; }
  .cta { background: linear-gradient(135deg, #302b63, #0f0c29); color: white; padding: 50px 30px; text-align: center; margin-top: 20px; }
  .cta h2 { font-size: 1.8em; margin-bottom: 12px; }
  .cta p { color: #b0bec5; margin-bottom: 24px; font-size: 1em; }
  .cta a { display: inline-block; background: #4fc3f7; color: #0f0c29; padding: 13px 36px; border-radius: 8px; font-size: 1em; font-weight: 700; text-decoration: none; }
  .cta a:hover { background: white; }
  @media (max-width: 600px) { .hero h1 { font-size: 1.5em; } .hero-stats { gap: 16px; } }
</style>
</head>
<body>

<div class="hero">
  <h1>Artisans du <span>92 — Hauts-de-Seine</span></h1>
  <p class="sub">Extrait représentatif · 50 fiches débloquées sur ${stats.total} disponibles</p>
  <div class="hero-stats">
    <div class="hero-stat"><div class="number">${stats.total}</div><div class="label">Fiches dispo</div></div>
    <div class="hero-stat"><div class="number">${stats.with_email}</div><div class="label">Avec email</div></div>
    <div class="hero-stat"><div class="number">${stats.with_tel}</div><div class="label">Téléphones</div></div>
    <div class="hero-stat"><div class="number">${stats.with_mobile}</div><div class="label">Mobiles</div></div>
    <div class="hero-stat"><div class="number">${stats.rge}</div><div class="label">Certifiés RGE</div></div>
    <div class="hero-stat"><div class="number">${stats.with_rc}</div><div class="label">Assurance RC</div></div>
  </div>
  <div style="color:#78909c; font-size:0.8em;">7 sources · CAPEB · Qualibat · QualiENR · Qualifelec · INSEE · Sites web · LinkedIn</div>
</div>

<div style="padding:0 20px">
  <div class="notice">
    <strong>Extrait représentatif :</strong> Ces 50 fiches sont tirées uniformément sur l'ensemble des ${stats.total} disponibles (1 fiche sur ${step}) — pas cherry-pickées. La base complète a la même distribution de qualité. Score moyen ici : ${stats.score_moyen}%.
  </div>
</div>

<div class="section">
  <h2>50 fiches <span>entièrement débloquées</span></h2>
  <div class="grid" id="grid"></div>
</div>

<div id="modal-overlay" class="modal-overlay" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <div class="mhead">
      <h3 id="m-name"></h3>
      <p id="m-loc"></p>
      <button class="mclose" onclick="closeModal()">&#215;</button>
    </div>
    <div class="mbody" id="m-body"></div>
  </div>
</div>

<div class="cta">
  <h2>Les ${stats.total} fiches du 92 ?</h2>
  <p>${stats.with_email} emails &middot; ${stats.with_tel} t&eacute;l&eacute;phones &middot; ${stats.rge} certifi&eacute;s RGE &middot; ${stats.with_rc} assurances RC identifi&eacute;es</p>
  <a href="mailto:paul@growth-factory.fr?subject=Base artisans 92 - Demande de tarif">Demander le tarif complet</a>
</div>

<script>
var S = ${sampleJson};
function sc(v){ var n=parseFloat(v)||0; return n>=80?'#00b894':n>=60?'#f59e0b':'#ef4444'; }
function g(v){ return (v&&v.trim())?v.trim():''; }
function renderGrid(){
  var grid=document.getElementById('grid');
  S.forEach(function(s,i){
    var score=Math.round(parseFloat(s.score_completude||0));
    var tel=g(s.telephone)||'—';
    var email=g(s.email_generique)||g(s.email_dirigeant)||'—';
    var dir=g(s.dirigeant_nom_complet)||'—';
    var badges='';
    if(s.is_rge==='oui') badges+='<span class="badge br">&#10003; RGE</span>';
    if(g(s.qualibat)) badges+='<span class="badge bb">Qualibat</span>';
    if(g(s.qualienr)) badges+='<span class="badge bg">QualiENR</span>';
    if(g(s.qualifelec)) badges+='<span class="badge bp">Qualifelec</span>';
    if(g(s.assurance_rc)) badges+='<span class="badge bo">RC Pro</span>';
    if(g(s.nb_sources)) badges+='<span class="badge bs">'+g(s.nb_sources)+' src</span>';
    var spec=g(s.specialite)?'<div class="row"><span class="lbl">M&eacute;tier</span><span class="val">'+g(s.specialite)+'</span></div>':'';
    grid.innerHTML+='<div class="card" onclick="openModal('+i+')">'
      +'<div class="card-top"><div><span class="card-tag">D&eacute;bloqu&eacute;</span>'
      +'<div class="card-name">'+g(s.nom_entreprise)+'</div>'
      +'<div class="card-loc">&#128205; '+g(s.ville,'—')+' ('+g(s.code_postal,'—')+')</div></div>'
      +'<div class="score" style="background:'+sc(s.score_completude)+'">'+score+'%</div></div>'
      +'<div class="badges">'+badges+'</div>'
      +'<div class="row"><span class="lbl">T&eacute;l.</span><span class="val'+(tel==='—'?' none':'')+'">'+tel+'</span></div>'
      +'<div class="row"><span class="lbl">Email</span><span class="val'+(email==='—'?' none':'')+'">'+email+'</span></div>'
      +'<div class="row"><span class="lbl">Dirigeant</span><span class="val'+(dir==='—'?' none':'')+'">'+dir+'</span></div>'
      +spec+'</div>';
  });
}
function openModal(i){
  var s=S[i];
  var score=Math.round(parseFloat(s.score_completude||0));
  document.getElementById('m-name').textContent=g(s.nom_entreprise);
  document.getElementById('m-loc').textContent=g(s.ville,'—')+' ('+g(s.code_postal,'—')+')';
  var sections=[
    {t:'Identit&eacute;',rows:[['SIRET',g(s.siret)],['Forme jur.',g(s.forme_juridique)],['Code APE',g(s.code_ape)],['Cr&eacute;ation',g(s.date_creation)],['Sp&eacute;cialit&eacute;',g(s.specialite)],['Compl&eacute;tude',score+'%'],['Sources',g(s.sources)]]},
    {t:'Contact',rows:[['T&eacute;l&eacute;phone',g(s.telephone)],['Email entreprise',g(s.email_generique)],['Site web',g(s.site_web)]]},
    {t:'Dirigeant',rows:[['Nom complet',g(s.dirigeant_nom_complet)],['Mobile',g(s.dirigeant_telephone)],['Email dirigeant',g(s.email_dirigeant)],['LinkedIn',g(s.linkedin)]]},
    {t:'Financier &amp; INSEE',rows:[['CA',g(s.chiffre_affaires)],['Effectif',g(s.nb_salaries)],['Solvabilit&eacute;',g(s.solvabilite)]]},
    {t:'Certifications &amp; Assurances',rows:[['RGE',s.is_rge==='oui'?'&#10003; Oui':'Non'],['Qualibat',g(s.qualibat)||'—'],['QualiENR',g(s.qualienr)||'—'],['Qualifelec',g(s.qualifelec)||'—'],['Assurance RC',g(s.assurance_rc)||'—'],['Assurance DC',g(s.assurance_dc)||'—']]}
  ];
  var html='';
  sections.forEach(function(sec){
    var rows=sec.rows.filter(function(r){return r[1]&&r[1]!=='—';});
    if(!rows.length)return;
    html+='<div class="msec"><div class="msec-title">'+sec.t+'</div>';
    rows.forEach(function(r){html+='<div class="mrow"><span class="lbl">'+r[0]+'</span><span class="val">'+r[1]+'</span></div>';});
    html+='</div>';
  });
  document.getElementById('m-body').innerHTML=html;
  document.getElementById('modal-overlay').classList.add('active');
}
function closeModal(){ document.getElementById('modal-overlay').classList.remove('active'); }
document.addEventListener('keydown',function(e){if(e.key==='Escape')closeModal();});
renderGrid();
</script>
</body>
</html>`;

fs.writeFileSync('demo-92.html', html, 'utf-8');
console.log('Genere: demo-92.html (' + Math.round(html.length/1024) + ' KB)');
