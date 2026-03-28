# Artisan Data Platform

## Objectif

Construire la **meilleure base de données artisans du bâtiment en France** — super clean, enrichie, avec un maximum de data points bien organisés. Exploitable en CSV pour un client, intégrable en SaaS, ou utilisable comme data asset standalone.

## Sources de données

| Source | Type | Volume | Données clés |
|---|---|---|---|
| **CAPEB** | Scraping API | 56 005 artisans | SIRET, nom, adresse, GPS, spécialité, RGE, activités |
| **Qualibat** | XLSX (4 sheets) | 15 650 certifiés | CA, assurance, solvabilité, emails dirigeant, date création |
| **QualiENR** | XLSX (6 sheets) | 17 813 certifiés | Certifs ENR (PAC, solaire, bois), photovoltaïque, assurances RC/DC |
| **Qualifelec** | XLSX (4 sheets) | 5 384 certifiés | RGE, SPV photovoltaïque, assurances, adresses, LinkedIn |
| **LinkedIn** | XLSX (6 sheets) | 9 701 profils | Profils dirigeants, job titles, headlines, localisation |
| **API SIRENE** | API gouv (en cours) | 78K SIRET à enrichir | NAF officiel, effectifs, statut actif/fermé, dirigeants INSEE |

## Architecture de réconciliation

### Modèle d'entité unique (clé = SIRET)

Chaque entreprise = 1 ligne avec 51 colonnes organisées en blocs :

- **Identité** : siret, siren, nom_entreprise, forme_juridique, code_ape, date_creation
- **Contact** : email_generique, email_dirigeant, telephone, fax, site_web
- **Localisation** : adresse, code_postal, ville, latitude, longitude
- **Dirigeant** : nom_complet, prenom, nom, email, telephone
- **Financier** : chiffre_affaires, nb_salaries, solvabilite, risque_impaye
- **Certifications** : is_rge, qualibat (oui/non), qualienr (oui/non + détail), qualifelec (oui/non + RGE/SPV)
- **Assurances** : assurance_rc, assurance_dc
- **LinkedIn** : profil, job_title, headline, source_metier, match_type
- **Metadata** : sources (pipe-separated), nb_sources, score_completude (0-100%)

### Pipeline de réconciliation (reconcile.js)

1. **CAPEB comme base pivot** → 54 459 entités initiales
2. **Qualibat/QualiENR/Qualifelec** → match SIRET exact + SIREN (9 digits) → fusionne ou crée nouvelle entité
3. **LinkedIn** → fuzzy matching (trigrammes + Levenshtein + bonus ville/dirigeant) → enrichit les entités existantes
4. **Score de complétude** calculé sur 19 champs clés
5. **Export trié** par richesse (nb_sources desc, score_completude desc)

### Résultats actuels

- **78 608 entités uniques**
- 15 378 enrichies depuis 2+ sources
- Score complétude moyen : 50%
- 1 entreprise avec 5 sources (ADVANCE INGENIERIE SERVICES)

### Qualité du fuzzy matching LinkedIn

Audit sur 4 052 matchs fuzzy :

| Tranche score | Volume | Qualité |
|---|---|---|
| 1.00+ | 1 268 (31%) | Excellent — nom exact + bonus ville/dirigeant |
| 0.90-1.00 | 273 (7%) | Très bon — quasi exact |
| 0.80-0.90 | 377 (9%) | Bon — noms très proches |
| 0.70-0.80 | 736 (18%) | Correct — à vérifier manuellement sur un sample |
| 0.60-0.70 | 960 (24%) | Risqué — faux positifs possibles (villes différentes) |
| 0.55-0.60 | 438 (11%) | Douteux — beaucoup de faux positifs |

**Action requise** : remonter le seuil à 0.65 ou ajouter une colonne `linkedin_confidence` (high/medium/low) pour que le client puisse filtrer.

## Scripts

| Script | Rôle | Commande |
|---|---|---|
| `scraper.js` | Scraping CAPEB API → artisans_capeb.csv | `node scraper.js` |
| `enrich_siret.js` | Enrichissement SIRET via API SIRENE gouv | `node enrich_siret.js` |
| `reconcile.js` | Réconciliation multi-sources → artisans_unified.csv | `node reconcile.js` |

## Fichiers de sortie

| Fichier | Contenu |
|---|---|
| `artisans_unified.csv` | Base unifiée principale (78K lignes, 51 cols) |
| `linkedin_unmatched.csv` | Profils LinkedIn non matchés (2 762) |
| `reconciliation_stats.json` | Stats de réconciliation |
| `.enrich_progress.json` | Progression enrichissement API (reprise auto) |

## TODO

- [ ] **Enrichissement API SIRENE** — relancer `node enrich_siret.js` quand l'API sera débloquée (5 req/s, stop auto si erreurs). Ajoutera NAF officiel, effectifs, statut actif/fermé, dirigeants INSEE aux 78K entités.
- [ ] **Améliorer fuzzy LinkedIn** — ajouter colonne confidence (high >= 0.80, medium 0.65-0.80, low 0.55-0.65), ou remonter seuil
- [ ] **Merger enrichissement API dans le unified** — quand enrich_siret.js sera fini, intégrer les data SIRENE dans reconcile.js
- [ ] **LinkedIn non matchés** — tenter enrichissement par nom via API SIRENE (chercher SIRET par nom d'entreprise)
- [ ] **Nettoyage données** — uniformiser formats tel, normaliser villes, détecter emails invalides
- [ ] **Export client-ready** — version CSV propre avec headers français, filtres métier prêts

## Notes techniques

- L'API `recherche-entreprises.api.gouv.fr` rate-limit agressivement au-delà de ~5 req/s. Ne pas dépasser. Le script a un stop auto après 20 batches en erreur consécutifs.
- Les XLSX Qualibat/QualiENR/Qualifelec ont plusieurs sheets (segments) avec des colonnes légèrement différentes. Le loader déduplique par SIRET cross-sheets.
- La colonne `dédoublonnage` dans QualiENR indique "Qualibat" quand l'entreprise est aussi dans Qualibat — confirme le cross-dedup.
- Le fuzzy matching utilise un index inversé de trigrammes pour réduire les comparaisons de O(n*m) à O(n*k) où k << m.
