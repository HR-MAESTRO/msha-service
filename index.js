'use strict';
/**
 * MSHA transformer service.
 *
 * Apps Script's Utilities.unzip() is hard-capped at 100MB per zip member, and the
 * MSHA Accidents.txt is bigger than that. This service streams the MSHA Open-Gov
 * ZIPs line-by-line (no cap, low memory), filters to Metal/Non-Metal, aggregates,
 * and returns ~a few KB of JSON in EXACTLY the shapes the dashboard already expects:
 *
 *   { allFatalities:[{Year,Fatalities}],
 *     tenYrCauses:[{Cause,Fac,Surf,UG,Sum,Sum_1,Sum_2,Total}],
 *     injuryRate:[{Year,Injuries,Hours,Rate}],
 *     stateData:{ AL:{fatalities,injuries}, ... },
 *     lastUpdated:"ISO" }
 *
 * Deploy (Cloud Run, from this folder):
 *   gcloud run deploy msha-service --source . --region us-central1 \
 *     --allow-unauthenticated --set-env-vars SECRET=YOUR_SHARED_TOKEN --timeout 600
 * Then in Apps Script → Project Settings → Script Properties:
 *   MSHA_SERVICE_URL   = https://msha-service-xxxxx-uc.a.run.app
 *   MSHA_SERVICE_TOKEN = YOUR_SHARED_TOKEN   (must match SECRET)
 */

const express = require('express');
const https = require('https');
const fs = require('fs');
const unzipper = require('unzipper');

const BASE = 'https://arlweb.msha.gov/OpenGovernmentData/DataSets/';
const SECRET = process.env.SECRET || '';

// Optional coordinate corrections produced by the one-time geocode.js enrichment job
// (MINE_ID -> { lat, lng, source }). Absent on first deploy; applied to the roster once present.
let COORDS_OVERRIDE = {};
try {
  COORDS_OVERRIDE = JSON.parse(fs.readFileSync(__dirname + '/coords-override.json', 'utf8'));
  console.log('Loaded coords-override.json:', Object.keys(COORDS_OVERRIDE).length, 'entries');
} catch (e) { /* no override file yet — fine */ }

const FIPS_TO_STATE = {
  '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA', '08': 'CO', '09': 'CT', '10': 'DE',
  '11': 'DC', '12': 'FL', '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN', '19': 'IA',
  '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME', '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN',
  '28': 'MS', '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH', '34': 'NJ', '35': 'NM',
  '36': 'NY', '37': 'NC', '38': 'ND', '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
  '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT', '50': 'VT', '51': 'VA', '53': 'WA',
  '54': 'WV', '55': 'WI', '56': 'WY'
};
// Generous per-state bounding boxes [latMin, latMax, lngMin, lngMax] — used only to
// catch GROSS coordinate errors (wrong state, sign flip, null island), not precision.
const STATE_BBOX = {
  AL: [30.0, 35.1, -88.6, -84.8], AK: [51.0, 71.6, -179.9, -129.0], AZ: [31.2, 37.1, -114.9, -108.9],
  AR: [32.9, 36.6, -94.7, -89.6], CA: [32.4, 42.1, -124.6, -114.0], CO: [36.9, 41.1, -109.1, -101.9],
  CT: [40.9, 42.1, -73.8, -71.7], DE: [38.4, 39.9, -75.8, -74.9], DC: [38.7, 39.0, -77.2, -76.9],
  FL: [24.3, 31.1, -87.7, -79.9], GA: [30.3, 35.1, -85.7, -80.7], HI: [18.8, 22.3, -160.4, -154.7],
  ID: [41.9, 49.1, -117.3, -110.9], IL: [36.9, 42.6, -91.6, -87.0], IN: [37.7, 41.8, -88.1, -84.7],
  IA: [40.3, 43.6, -96.7, -90.1], KS: [36.9, 40.1, -102.1, -94.5], KY: [36.4, 39.2, -89.6, -81.9],
  LA: [28.9, 33.1, -94.1, -88.7], ME: [42.9, 47.5, -71.1, -66.9], MD: [37.8, 39.8, -79.5, -74.9],
  MA: [41.2, 42.9, -73.6, -69.8], MI: [41.6, 48.4, -90.5, -82.3], MN: [43.4, 49.5, -97.3, -89.4],
  MS: [30.1, 35.1, -91.7, -88.0], MO: [35.9, 40.7, -95.9, -88.9], MT: [44.3, 49.1, -116.1, -103.9],
  NE: [39.9, 43.1, -104.1, -95.2], NV: [34.9, 42.1, -120.1, -113.9], NH: [42.6, 45.4, -72.6, -70.5],
  NJ: [38.8, 41.4, -75.6, -73.8], NM: [31.2, 37.1, -109.1, -102.9], NY: [40.4, 45.1, -79.8, -71.8],
  NC: [33.8, 36.6, -84.4, -75.3], ND: [45.9, 49.1, -104.1, -96.5], OH: [38.3, 42.4, -84.9, -80.4],
  OK: [33.6, 37.1, -103.1, -94.4], OR: [41.9, 46.4, -124.6, -116.4], PA: [39.7, 42.4, -80.6, -74.6],
  RI: [41.1, 42.1, -71.9, -71.0], SC: [32.0, 35.3, -83.4, -78.4], SD: [42.4, 46.0, -104.1, -96.4],
  TN: [34.9, 36.7, -90.4, -81.6], TX: [25.8, 36.6, -106.7, -93.4], UT: [36.9, 42.1, -114.1, -108.9],
  VT: [42.7, 45.1, -73.5, -71.4], VA: [36.5, 39.5, -83.7, -75.1], WA: [45.5, 49.1, -124.9, -116.9],
  WV: [37.1, 40.7, -82.7, -77.6], WI: [42.4, 47.1, -92.9, -86.7], WY: [40.9, 45.1, -111.1, -103.9]
};
function inBbox(st, lat, lng) {
  const b = STATE_BBOX[st];
  if (!b) return true; // unknown state (e.g. PR) — can't validate, don't flag
  return lat >= b[0] && lat <= b[1] && lng >= b[2] && lng <= b[3];
}
// Which state's bbox contains this point (first match) — '' if none (ocean/foreign/null-island).
function guessState(lat, lng) {
  for (const s in STATE_BBOX) {
    const b = STATE_BBOX[s];
    if (lat >= b[0] && lat <= b[1] && lng >= b[2] && lng <= b[3]) return s;
  }
  return '';
}

const INJURY_CODES = { '01': true, '02': true, '03': true, '04': true, '05': true, '06': true };
const FATALITY_CODE = '01';

// ── Aggregate-industry product classification ──────────────────────────────
// Keyed on the MSHA PRIMARY_SIC text exactly as it appears in Mines.txt (these are
// the same strings the dashboard already used when the data was pulled manually).
// segment 'construction_aggregate' feeds the Safety "aggregates" filter; segment
// 'industrial_sand' rides along in the location roster but is EXCLUDED from safety.
const PRODUCT_SEGMENT = {
  'crushed, broken limestone nec': 'construction_aggregate',
  'crushed, broken marble': 'construction_aggregate',
  'construction sand and gravel': 'construction_aggregate',
  'crushed, broken granite': 'construction_aggregate',
  'crushed, broken sandstone': 'construction_aggregate',
  'crushed, broken slate': 'construction_aggregate',
  'crushed, broken stone nec': 'construction_aggregate',
  'crushed, broken quartzite': 'construction_aggregate',
  'sand, common': 'construction_aggregate',
  'crushed, broken traprock': 'construction_aggregate',
  'crushed, broken mica': 'construction_aggregate',
  'crushed, broken basalt': 'construction_aggregate',
  'sand, industrial nec': 'industrial_sand',
  'sand, industrial': 'industrial_sand'
};
function normKey(s) { return String(s || '').replace(/\s+/g, ' ').trim().toLowerCase(); }
function productSegment(s) { return PRODUCT_SEGMENT[normKey(s)] || null; }

// MSHA statuses kept in the location roster (the "all listed" set the user wants).
// Abandoned / Closed-By-MSHA are excluded. Matched on a punctuation/space-stripped
// lowercase key to tolerate spelling variants (e.g. "Non-Producing" vs "NonProducing").
const ROSTER_STATUS = {
  'active': 'Active',
  'intermittent': 'Intermittent',
  'temporarilyidled': 'Temporarily Idled',
  'nonproducing': 'NonProducing',
  'newmine': 'New Mine'
};
function statusKey(s) { return String(s || '').toLowerCase().replace(/[^a-z]/g, ''); }
function rosterStatus(s) { return ROSTER_STATUS[statusKey(s)] || null; }

// Return the column index for the first header name that exists (case-insensitive
// names already uppercased in the idx map). Lets us tolerate MSHA naming variants.
function col(idx, names) {
  for (let i = 0; i < names.length; i++) {
    if (idx[names[i]] !== undefined) return idx[names[i]];
  }
  return undefined;
}

// Degree-of-injury code → readable severity label (MSHA standard codes).
const DEGREE_LABELS = {
  '01': 'Fatality',
  '02': 'Permanent Disability',
  '03': 'Days Away From Work',
  '04': 'Days Away & Restricted',
  '05': 'Restricted Duty Only',
  '06': 'No Lost Time (Medical)',
  '07': 'Occupational Illness'
};

// Human labels for the most-cited 30 CFR Metal/Non-Metal standards; fallback to the raw section.
const CFR_DESCR = {
  '56.14107': 'Moving machine parts — guarding',
  '56.12028': 'Continuity & resistance of grounding',
  '56.18010': 'First aid availability',
  '56.14100': 'Safety defects — exam & correction',
  '56.20003': 'Housekeeping at workplaces',
  '56.14132': 'Backup alarms / horns',
  '56.4101': 'Warning signs — flammable/combustible',
  '56.14112': 'Construction & maintenance of guards',
  '56.11001': 'Safe access to working places',
  '56.12032': 'Inspection & cover plates (electrical)',
  '56.14130': 'ROPS & seat belts',
  '56.9300': 'Berms or guardrails on roadways',
  '56.3200': 'Correction of hazardous ground conditions',
  '56.15005': 'Safety belts and lines',
  '56.14101': 'Brakes — self-propelled equipment',
  '56.14200': 'Warning before equipment is moved',
  '47.41': 'HazCom — container labeling'
};

// Stream a MSHA .zip, calling onLine(headerIdxMap, parts) for each data row.
function streamZip(fileName, onHeader, onRow) {
  return new Promise((resolve, reject) => {
    const url = BASE + fileName;
    https.get(url, { headers: { 'User-Agent': 'msha-service' } }, (res) => {
      if (res.statusCode !== 200) { reject(new Error(fileName + ' HTTP ' + res.statusCode)); return; }
      let handled = false;
      res.pipe(unzipper.Parse())
        .on('entry', (entry) => {
          if (handled || !/\.txt$/i.test(entry.path)) { entry.autodrain(); return; }
          handled = true;
          let leftover = '';
          let idx = null;
          const handleLine = (line) => {
            if (!line) return;
            // MSHA pipe-delimited files wrap every field in double quotes ("M"|"2024"|...).
            // Strip the surrounding quotes from each value so filters compare against M, not "M".
            const parts = line.split('|').map((s) => s.replace(/^"|"$/g, ''));
            if (idx === null) {
              idx = {};
              parts.forEach((h, i) => { idx[h.trim().toUpperCase().replace(/^"|"$/g, '')] = i; });
              if (onHeader) onHeader(idx);
              return;
            }
            onRow(idx, parts);
          };
          entry.on('data', (chunk) => {
            const text = leftover + chunk.toString('utf8');
            const lines = text.split('\n');
            leftover = lines.pop();
            for (const l of lines) handleLine(l.replace(/\r$/, ''));
          });
          entry.on('end', () => { if (leftover) handleLine(leftover.replace(/\r$/, '')); resolve(); });
          entry.on('error', reject);
        })
        .on('error', reject)
        .on('close', () => { if (!handled) reject(new Error('No .txt member found in ' + fileName)); });
    }).on('error', reject);
  });
}

// ── Safety accumulators (run Metal/Non-Metal and aggregates-only in parallel) ──
function newSafetyAcc() {
  return {
    fatalByYear: {}, injuryByYear: {}, causeBySubunit: {}, stateData: {},
    injSeverity: {}, injCause: {}, injOp: { Facility: 0, Surface: 0, Underground: 0 }
  };
}
function addAccidentRec(acc, rec, tenYrStart) {
  const yr = rec.yr, deg = rec.deg, cls = rec.cls, st = rec.st;
  if (deg === FATALITY_CODE) {
    acc.fatalByYear[yr] = (acc.fatalByYear[yr] || 0) + 1;
    if (st) (acc.stateData[st] = acc.stateData[st] || { fatalities: 0, injuries: 0 }).fatalities++;
  }
  if (INJURY_CODES[deg]) {
    acc.injuryByYear[yr] = (acc.injuryByYear[yr] || 0) + 1;
    if (st) (acc.stateData[st] = acc.stateData[st] || { fatalities: 0, injuries: 0 }).injuries++;
  }
  if (yr >= tenYrStart) {
    if (deg === FATALITY_CODE) {
      const c = acc.causeBySubunit[cls] || (acc.causeBySubunit[cls] = { Cause: cls, Fac: 0, Surf: 0, UG: 0 });
      if (rec.isFacility) c.Fac++; else if (rec.isUG) c.UG++; else c.Surf++;
    }
    if (deg !== FATALITY_CODE && (INJURY_CODES[deg] || deg === '07')) {
      const sev = DEGREE_LABELS[deg] || 'Other';
      acc.injSeverity[sev] = (acc.injSeverity[sev] || 0) + 1;
      acc.injCause[cls] = (acc.injCause[cls] || 0) + 1;
      if (rec.isFacility) acc.injOp.Facility++; else if (rec.isUG) acc.injOp.Underground++; else acc.injOp.Surface++;
    }
  }
}
function finalizeSafety(acc, hoursByYear) {
  const allFatalities = Object.keys(acc.fatalByYear).map(Number).sort((a, b) => a - b)
    .map((yr) => ({ Year: yr, Fatalities: acc.fatalByYear[yr] }));
  const tenYrCauses = Object.values(acc.causeBySubunit)
    .map((c) => ({ Cause: c.Cause, Fac: c.Fac, Surf: c.Surf, UG: c.UG, Sum: c.Fac, Sum_1: c.Surf, Sum_2: c.UG, Total: c.Fac + c.Surf + c.UG }))
    .sort((a, b) => b.Total - a.Total).slice(0, 15);
  const injuryArr = Object.keys(acc.injuryByYear).map(Number).sort((a, b) => a - b)
    .map((yr) => ({ Year: yr, Injuries: acc.injuryByYear[yr] }));
  const injuryRate = injuryArr.map((row) => {
    const hrs = hoursByYear[row.Year] || 0;
    const rate = hrs > 0 ? Number(((row.Injuries / hrs) * 200000).toFixed(2)) : null;
    return { Year: row.Year, Injuries: row.Injuries, Hours: Math.round(hrs), Rate: rate };
  }).filter((r) => r.Rate !== null);
  const stateByAbbr = {};
  Object.keys(acc.stateData).forEach((fips) => {
    const abbr = FIPS_TO_STATE[fips];
    if (abbr) stateByAbbr[abbr] = acc.stateData[fips];
  });
  const injuries = {
    byYear: Object.keys(acc.injuryByYear).map(Number).sort((a, b) => a - b)
      .map((yr) => ({ Year: yr, Count: acc.injuryByYear[yr] })),
    bySeverity: Object.keys(acc.injSeverity).map((k) => ({ label: k, count: acc.injSeverity[k] }))
      .sort((a, b) => b.count - a.count),
    byCause: Object.keys(acc.injCause).map((k) => ({ label: k, count: acc.injCause[k] }))
      .sort((a, b) => b.count - a.count).slice(0, 10),
    byOperation: Object.keys(acc.injOp).map((k) => ({ label: k, count: acc.injOp[k] }))
      .filter((o) => o.count > 0)
  };
  return { allFatalities: allFatalities, tenYrCauses: tenYrCauses, injuryRate: injuryRate, stateData: stateByAbbr, injuries: injuries };
}

function newViolAcc() { return { cfrAgg: {}, citByStateYear: {}, citByDistrictYear: {}, violYears: {} }; }
function addViolationRec(acc, rec, threeYrStart) {
  const yr = rec.yr, info = rec.info;
  if (info) {
    if (info.st) {
      (acc.citByStateYear[info.st] = acc.citByStateYear[info.st] || {});
      acc.citByStateYear[info.st][yr] = (acc.citByStateYear[info.st][yr] || 0) + 1;
      acc.violYears[yr] = true;
    }
    if (info.dist) {
      (acc.citByDistrictYear[info.dist] = acc.citByDistrictYear[info.dist] || {});
      acc.citByDistrictYear[info.dist][yr] = (acc.citByDistrictYear[info.dist][yr] || 0) + 1;
    }
  }
  if (yr >= threeYrStart && rec.base) {
    const a = acc.cfrAgg[rec.base] || (acc.cfrAgg[rec.base] = { count: 0, ss: 0 });
    a.count++;
    if (rec.ss) a.ss++;
  }
}
function finalizeViol(acc, stateDistrict) {
  const topViolations = Object.keys(acc.cfrAgg).map((sec) => {
    const a = acc.cfrAgg[sec];
    return {
      section: sec, description: CFR_DESCR[sec] || ('30 CFR §' + sec),
      count: a.count, ssCount: a.ss, typicallySS: a.ss > a.count / 2
    };
  }).sort((x, y) => y.count - x.count).slice(0, 15).map((v, i) => ({ section: v.section, description: v.description, count: v.count, ssCount: v.ssCount, typicallySS: v.typicallySS, rank: i + 1 }));
  return {
    topViolations: topViolations,
    byStateYear: acc.citByStateYear,
    byDistrictYear: acc.citByDistrictYear,
    stateDistrict: stateDistrict,
    years: Object.keys(acc.violYears).map(Number).sort((a, b) => a - b)
  };
}

// Split a construction-aggregate product into the two USGS commodities the State_Data
// sheet tracks: 'crushedStone' ("Stone, crushed") vs 'sandGravel' ("Sand and gravel").
function commodityOf(product) {
  const k = normKey(product);
  return (k.indexOf('sand') !== -1 || k.indexOf('gravel') !== -1) ? 'sandGravel' : 'crushedStone';
}

async function buildData() {
  const currentYear = new Date().getFullYear();
  const tenYrStart = currentYear - 10;
  const threeYrStart = currentYear - 3;
  const prodRecentYr = currentYear - 4;   // keep only recent MinesProdYearly hours for the production rollup

  // ── 1) Mines.zip FIRST — builds the location roster, the MINE_ID→{state,district}
  //       join map (for citations), and the aggregate MINE_ID set (for safety). ──
  const mineInfo = {};            // MINE_ID -> { st, dist } (ALL mines, for citation join)
  const stateDistrictCount = {};  // st -> { dist -> count } (dominant district per state)
  const aggMineIds = new Set();   // MINE_IDs with a construction-aggregate primary product (drives safety filter; product-only, ALL statuses)
  const roster = [];              // location roster (status-filtered, aggregate + industrial sand)
  const productCounts = {};       // raw PRIMARY_SIC -> count over roster [diagnostic]
  const statusCounts = {};        // raw status -> count over all mines [diagnostic]
  let latPresent = 0, latMissing = 0, latBadState = 0, latBadNonPortable = 0, latNoUSState = 0;
  const badSamples = [];          // actionable bad coords (non-portable or off-continent) [diagnostic]
  const prodInfo = {};            // MINE_ID -> { ctrl, comm, st } for construction-aggregate mines (membership/production rollup)
  try {
    let mId, mCm, mName, mBiz, mCtrl, mType, mStatus, mSt, mCnty, mCd, mDist, mProd, mSicCd, mLat, mLng, mEmp, mPort;
    await streamZip('Mines.zip', (idx) => {
      mId = col(idx, ['MINE_ID']);
      mCm = col(idx, ['COAL_METAL_IND']);
      mPort = col(idx, ['PORTABLE_OPERATION']);
      mName = col(idx, ['CURRENT_MINE_NAME', 'MINE_NAME']);
      mBiz = col(idx, ['CURRENT_OPERATOR_NAME', 'CURRENT_CONTROLLER_NAME', 'OPERATOR_NAME']);
      mCtrl = col(idx, ['CURRENT_CONTROLLER_NAME', 'CONTROLLER_NAME', 'CURRENT_CONTROLLER']);
      mType = col(idx, ['CURRENT_MINE_TYPE', 'MINE_TYPE']);
      mStatus = col(idx, ['CURRENT_MINE_STATUS', 'MINE_STATUS']);
      mSt = col(idx, ['STATE', 'STATE_ABBR', 'STATE_CD', 'MINE_STATE']);
      mCnty = col(idx, ['FIPS_CNTY_NM', 'COUNTY', 'COUNTY_NM']);
      mCd = col(idx, ['CONG_DIST_CD', 'CONGRESS_DIST', 'CONG_DIST']);
      mDist = col(idx, ['DISTRICT', 'MSHA_DISTRICT', 'DIST']);
      mProd = col(idx, ['PRIMARY_SIC', 'PRIMARY_PRODUCT', 'PRIMARY_CANVASS']);
      mSicCd = col(idx, ['PRIMARY_SIC_CD']);
      mLat = col(idx, ['LATITUDE', 'LAT']);
      mLng = col(idx, ['LONGITUDE', 'LONG', 'LON']);
      mEmp = col(idx, ['NO_EMPLOYEES', 'CURRENT_NO_EMPLOYEES', 'EMPLOYEES']);
    }, (_idx, p) => {
      if (mId === undefined) return;
      const id = (p[mId] || '').trim();
      if (!id) return;
      const cm = mCm !== undefined ? (p[mCm] || '').trim() : '';
      let st = mSt !== undefined ? (p[mSt] || '').trim().toUpperCase() : '';
      if (/^\d+$/.test(st)) st = FIPS_TO_STATE[st.padStart(2, '0')] || '';
      const dist = mDist !== undefined ? (p[mDist] || '').trim() : '';
      // Citation join map — Metal/Non-Metal only (skip coal mines we never use; saves ~half the entries).
      if ((mCm === undefined || cm === 'M') && st.length === 2) {
        mineInfo[id] = { st: st, dist: dist };
        if (dist) {
          (stateDistrictCount[st] = stateDistrictCount[st] || {});
          stateDistrictCount[st][dist] = (stateDistrictCount[st][dist] || 0) + 1;
        }
      }

      const product = mProd !== undefined ? (p[mProd] || '').trim() : '';
      const seg = productSegment(product);
      if (seg === 'construction_aggregate') {
        aggMineIds.add(id);
        // Membership/production rollup: parent (controller) + commodity + state, all statuses.
        const ctrl = (mCtrl !== undefined ? (p[mCtrl] || '').trim() : '') ||
          (mBiz !== undefined ? (p[mBiz] || '').trim() : '') || '(Unknown controller)';
        if (st.length === 2) prodInfo[id] = { ctrl: ctrl, comm: commodityOf(product), st: st };
      }

      const status = mStatus !== undefined ? (p[mStatus] || '').trim() : '';
      statusCounts[status] = (statusCounts[status] || 0) + 1;
      const rs = rosterStatus(status);
      // Roster = aggregate OR industrial sand, in one of the kept statuses.
      if (rs && seg) {
        productCounts[product] = (productCounts[product] || 0) + 1;
        let lat = parseFloat((mLat !== undefined ? p[mLat] : '') || '');
        let lng = parseFloat((mLng !== undefined ? p[mLng] : '') || '');
        let hasLL = isFinite(lat) && isFinite(lng) && lat !== 0 && lng !== 0;
        let coordSource = hasLL ? 'msha' : 'none';
        const ov = COORDS_OVERRIDE[id];
        if (ov && isFinite(ov.lat) && isFinite(ov.lng)) { lat = ov.lat; lng = ov.lng; hasLL = true; coordSource = ov.source || 'override'; }
        const nm = mName !== undefined ? (p[mName] || '').trim() : '';
        const portable = mPort !== undefined ? /^(y|1|t)/i.test((p[mPort] || '').trim()) : false;
        const coordOk = hasLL && inBbox(st, lat, lng);
        if (!hasLL) latMissing++;
        else if (coordOk) latPresent++;
        else {
          // Out of its registered state. For portables this is usually fine (the rig moved);
          // the actionable errors are non-portable or off-continent (ocean/foreign).
          latBadState++;
          const actuallyIn = guessState(lat, lng);
          if (!actuallyIn) latNoUSState++;
          if (!portable) latBadNonPortable++;
          if ((!portable || !actuallyIn) && badSamples.length < 40) {
            badSamples.push({ id: id, name: nm, state: st, lat: lat, lng: lng, actuallyIn: actuallyIn, portable: portable });
          }
        }
        roster.push({
          id: id,
          name: nm,
          business: mBiz !== undefined ? (p[mBiz] || '').trim() : '',
          type: mType !== undefined ? (p[mType] || '').trim() : '',
          status: rs,
          product: product,
          segment: seg,
          sicCd: mSicCd !== undefined ? (p[mSicCd] || '').trim() : '',
          state: st,
          county: mCnty !== undefined ? (p[mCnty] || '').trim() : '',
          cd: mCd !== undefined ? (p[mCd] || '').trim() : '',
          district: dist,
          portable: portable,
          lat: hasLL ? lat : null,
          lng: hasLL ? lng : null,
          coordOk: coordOk,
          coordSource: coordSource,
          employees: mEmp !== undefined ? (parseInt((p[mEmp] || '0').trim(), 10) || 0) : 0
        });
      }
    });
  } catch (e) {
    console.warn('Mines failed:', e.message);
  }
  const stateDistrict = {};
  Object.keys(stateDistrictCount).forEach((st) => {
    const m = stateDistrictCount[st];
    stateDistrict[st] = Object.keys(m).sort((a, b) => m[b] - m[a])[0];
  });

  // ── 2) AddressOfRecord.zip → street address for roster mines (join on MINE_ID) ──
  try {
    const rosterIds = new Set(roster.map((r) => r.id));
    const addrById = {};
    let aId, aStreet, aPo, aCity, aState, aZip;
    await streamZip('AddressOfRecord.zip', (idx) => {
      aId = col(idx, ['MINE_ID']);
      aStreet = col(idx, ['STREET', 'STREET_ADDRESS', 'ADDRESS']);
      aPo = col(idx, ['PO_BOX']);
      aCity = col(idx, ['CITY']);
      aState = col(idx, ['STATE_ABBR', 'STATE']);
      aZip = col(idx, ['ZIP_CD', 'ZIP', 'POSTAL_CD']);
    }, (_idx, p) => {
      if (aId === undefined) return;
      const id = (p[aId] || '').trim();
      if (!rosterIds.has(id)) return;
      addrById[id] = {
        street: aStreet !== undefined ? (p[aStreet] || '').trim() : '',
        po: aPo !== undefined ? (p[aPo] || '').trim() : '',
        city: aCity !== undefined ? (p[aCity] || '').trim() : '',
        state: aState !== undefined ? (p[aState] || '').trim() : '',
        zip: aZip !== undefined ? (p[aZip] || '').trim() : ''
      };
    });
    roster.forEach((r) => {
      const a = addrById[r.id];
      if (a) {
        const line1 = a.street || a.po;
        const cityState = [a.city, a.state].filter(Boolean).join(', ');
        r.address = [line1, cityState, a.zip].filter(Boolean).join(' ').trim();
        r.city = a.city; r.zip = a.zip;
      } else { r.address = ''; r.city = ''; r.zip = ''; }
    });
  } catch (e) {
    console.warn('AddressOfRecord failed:', e.message);
    roster.forEach((r) => { r.address = ''; r.city = ''; r.zip = ''; });
  }

  // Operations + employment by state (derived from roster)
  const operationsByState = {};
  const employmentByState = {};
  roster.forEach((r) => {
    const o = operationsByState[r.state] || (operationsByState[r.state] = { crushedStone: 0, sandGravel: 0, industrialSand: 0, total: 0 });
    const pk = normKey(r.product);
    if (r.segment === 'industrial_sand') o.industrialSand++;
    else if (pk.indexOf('sand') !== -1 || pk.indexOf('gravel') !== -1) o.sandGravel++;
    else o.crushedStone++;
    o.total++;
    employmentByState[r.state] = (employmentByState[r.state] || 0) + (r.employees || 0);
  });

  // ── 3) Accidents.zip → Metal/Non-Metal AND aggregates-only safety ──
  const mnm = newSafetyAcc(), agg = newSafetyAcc();
  await streamZip('Accidents.zip', null, (idx, p) => {
    const cm = (p[idx['COAL_METAL_IND']] || '').trim();
    const id = (p[idx['MINE_ID']] || '').trim();
    const isAgg = id && aggMineIds.has(id);
    if (cm !== 'M' && !isAgg) return;
    const yr = parseInt((p[idx['CAL_YR']] || '0').trim(), 10);
    if (!yr) return;
    const sub = (p[idx['SUBUNIT']] || '').trim().toLowerCase();
    const rec = {
      yr: yr,
      deg: (p[idx['DEGREE_INJURY_CD']] || '').trim(),
      cls: (p[idx['CLASSIFICATION']] || 'Unknown').trim() || 'Unknown',
      st: (p[idx['FIPS_STATE_CD']] || '').trim(),
      isUG: sub.indexOf('underground') !== -1,
      isFacility: sub.indexOf('facility') !== -1 || sub.indexOf('mill') !== -1 || sub.indexOf('shop') !== -1 || sub.indexOf('office') !== -1
    };
    if (cm === 'M') addAccidentRec(mnm, rec, tenYrStart);
    if (isAgg) addAccidentRec(agg, rec, tenYrStart);
  });

  // ── 4) MinesProdYearly.zip → employee-hours by year (M/NM + aggregates, joined on MINE_ID) ──
  const mnmHours = {}, aggHours = {};
  const prodHoursById = {};   // MINE_ID -> { year: hours } (recent years, aggregate mines only)
  try {
    let hCm, hYr, hHrs, hId;
    await streamZip('MinesProdYearly.zip', (idx) => {
      hCm = col(idx, ['C_M_IND', 'COAL_METAL_IND']);
      hYr = col(idx, ['CALENDAR_YR', 'CAL_YR', 'PROD_CAL_YR']);
      hHrs = col(idx, ['ANNUAL_HRS', 'ANNUAL_HOURS', 'HRS_WORKED', 'EMPLOYEE_HRS', 'EMPLOYEE_HOURS', 'HOURS_WORKED']);
      hId = col(idx, ['MINE_ID']);
    }, (_idx, p) => {
      if (hYr === undefined || hHrs === undefined) return;
      const yr = parseInt((p[hYr] || '0').trim(), 10);
      const hours = parseFloat((p[hHrs] || '0').trim());
      if (!yr || !hours) return;
      const cm = hCm !== undefined ? (p[hCm] || '').trim() : '';
      if (cm === 'M') mnmHours[yr] = (mnmHours[yr] || 0) + hours;
      const id = hId !== undefined ? (p[hId] || '').trim() : '';
      if (id && aggMineIds.has(id)) {
        aggHours[yr] = (aggHours[yr] || 0) + hours;
        if (prodInfo[id] && yr >= prodRecentYr) {
          (prodHoursById[id] = prodHoursById[id] || {});
          prodHoursById[id][yr] = (prodHoursById[id][yr] || 0) + hours;
        }
      }
    });
  } catch (e) {
    console.warn('MinesProdYearly failed (injury rate unavailable):', e.message);
  }

  // ── Production/membership rollup: parent (controller) employee-hours by state × commodity. ──
  // Allocator only — the dashboard combines these shares with USGS State_Data $ to estimate sales.
  // Each mine contributes its most-recent available year's hours (within the recent window).
  const controllerAgg = {};   // ctrlKey -> { name, mineCount, states:{}, hbsc:{ st:{crushedStone,sandGravel} } }
  const stateCommHours = {};  // st -> { crushedStone, sandGravel } (denominators for share allocation)
  let prodYearMax = 0;
  Object.keys(prodInfo).forEach((id) => {
    const info = prodInfo[id];
    const hrsObj = prodHoursById[id];
    let hrs = 0;
    if (hrsObj) {
      const yrs = Object.keys(hrsObj).map(Number).sort((a, b) => b - a);
      if (yrs.length) { hrs = hrsObj[yrs[0]]; if (yrs[0] > prodYearMax) prodYearMax = yrs[0]; }
    }
    const key = info.ctrl.toUpperCase();
    const c = controllerAgg[key] || (controllerAgg[key] = { name: info.ctrl, mineCount: 0, states: {}, hbsc: {} });
    c.mineCount++;
    c.states[info.st] = true;
    if (hrs > 0) {
      (c.hbsc[info.st] = c.hbsc[info.st] || { crushedStone: 0, sandGravel: 0 });
      c.hbsc[info.st][info.comm] += hrs;
      (stateCommHours[info.st] = stateCommHours[info.st] || { crushedStone: 0, sandGravel: 0 });
      stateCommHours[info.st][info.comm] += hrs;
    }
  });
  const production = {
    year: prodYearMax,
    stateCommHours: stateCommHours,
    controllers: Object.keys(controllerAgg).map((k) => {
      const c = controllerAgg[k];
      return { controller: c.name, mineCount: c.mineCount, states: Object.keys(c.states), hoursByStateComm: c.hbsc };
    })
  };

  // ── 5) Violations.zip → top CFR + citations, M/NM AND aggregates-only ──
  const mnmViol = newViolAcc(), aggViol = newViolAcc();
  try {
    let vCm, vYr, vCfr, vSS, vMine;
    await streamZip('Violations.zip', (idx) => {
      vCm = col(idx, ['COAL_METAL_IND']);
      vYr = col(idx, ['CAL_YR', 'VIOLATION_ISSUE_YR']);
      // PART_SECTION holds the 30 CFR standard (e.g. "56.14132(a)"); SECTION_OF_ACT is the Mine Act section (often blank).
      vCfr = col(idx, ['PART_SECTION', 'SECTION_OF_ACT', 'CFR_STANDARD']);
      vSS = col(idx, ['SIG_SUB', 'SIG_AND_SUB', 'S_AND_S']);
      vMine = col(idx, ['MINE_ID']);
    }, (_idx, p) => {
      if (vYr === undefined) return;
      const yr = parseInt((p[vYr] || '0').trim(), 10);
      if (!yr) return;
      const cm = vCm !== undefined ? (p[vCm] || '').trim() : '';
      const id = vMine !== undefined ? (p[vMine] || '').trim() : '';
      const isAgg = id && aggMineIds.has(id);
      if (cm !== 'M' && !isAgg) return;
      let base = null;
      if (yr >= threeYrStart && vCfr !== undefined) {
        const m = (p[vCfr] || '').trim().match(/^\d+\.\d+/);
        if (m) base = m[0];
      }
      const rec = {
        yr: yr,
        base: base,
        ss: vSS !== undefined && (p[vSS] || '').trim().toUpperCase() === 'Y',
        info: mineInfo[id] || null
      };
      if (cm === 'M') addViolationRec(mnmViol, rec, threeYrStart);
      if (isAgg) addViolationRec(aggViol, rec, threeYrStart);
    });
  } catch (e) {
    console.warn('Violations failed (citations unavailable):', e.message);
  }

  const mnmFinal = finalizeSafety(mnm, mnmHours);
  const aggFinal = finalizeSafety(agg, aggHours);

  return {
    // Top-level = Metal/Non-Metal (UNCHANGED shape — the deployed dashboard keeps working).
    allFatalities: mnmFinal.allFatalities,
    tenYrCauses: mnmFinal.tenYrCauses,
    injuryRate: mnmFinal.injuryRate,
    stateData: mnmFinal.stateData,
    injuries: mnmFinal.injuries,
    violations: finalizeViol(mnmViol, stateDistrict),
    // Aggregates-only parallel view (construction aggregates; industrial sand excluded).
    aggregates: {
      allFatalities: aggFinal.allFatalities,
      tenYrCauses: aggFinal.tenYrCauses,
      injuryRate: aggFinal.injuryRate,
      stateData: aggFinal.stateData,
      injuries: aggFinal.injuries,
      violations: finalizeViol(aggViol, stateDistrict)
    },
    // Location roster + derived counts (served via /mines, not /).
    roster: roster,
    operationsByState: operationsByState,
    employmentByState: employmentByState,
    // Parent-company employee-hours by state×commodity (served via /production).
    production: production,
    diagnostics: {
      rosterCount: roster.length,
      aggMineIdCount: aggMineIds.size,
      latPresent: latPresent,
      latMissing: latMissing,
      latBadState: latBadState,
      latBadNonPortable: latBadNonPortable,
      latNoUSState: latNoUSState,
      geocodeResidual: latMissing + latBadNonPortable + latNoUSState,
      badSamples: badSamples,
      productCounts: productCounts,
      statusCounts: statusCounts
    },
    lastUpdated: new Date().toISOString()
  };
}

// Cache results in memory (warm instance) to avoid re-downloading on every call.
let CACHE = null;
let CACHE_TS = 0;
const CACHE_MS = 6 * 60 * 60 * 1000;

async function getData(fresh) {
  if (!fresh && CACHE && (Date.now() - CACHE_TS) < CACHE_MS) return CACHE;
  const data = await buildData();
  CACHE = data; CACHE_TS = Date.now();
  return data;
}

// Safety payload (small) — the Safety tab reads this. Excludes the big roster.
function safetySlice(d) {
  return {
    allFatalities: d.allFatalities, tenYrCauses: d.tenYrCauses, injuryRate: d.injuryRate,
    stateData: d.stateData, injuries: d.injuries, violations: d.violations,
    aggregates: d.aggregates, lastUpdated: d.lastUpdated
  };
}
// Location payload (large) — the roster sheet writer reads this. Excludes safety detail.
function minesSlice(d) {
  return {
    roster: d.roster, operationsByState: d.operationsByState, employmentByState: d.employmentByState,
    diagnostics: d.diagnostics, lastUpdated: d.lastUpdated
  };
}
// Production payload — parent-company hours by state×commodity (membership targeting).
function productionSlice(d) {
  return { production: d.production, lastUpdated: d.lastUpdated };
}

const app = express();

app.get('/', async (req, res) => {
  if (SECRET && req.query.token !== SECRET) {
    res.status(401).json({ error: 'Invalid or missing token' });
    return;
  }
  try {
    const d = await getData(req.query.fresh === '1');
    res.json(safetySlice(d));
  } catch (e) {
    console.error('build error:', e);
    res.status(500).json({ error: String(e && e.message || e) });
  }
});

// Location roster + operations/employment + diagnostics.
app.get('/mines', async (req, res) => {
  if (SECRET && req.query.token !== SECRET) {
    res.status(401).json({ error: 'Invalid or missing token' });
    return;
  }
  try {
    const d = await getData(req.query.fresh === '1');
    res.json(minesSlice(d));
  } catch (e) {
    console.error('build error:', e);
    res.status(500).json({ error: String(e && e.message || e) });
  }
});

// Parent-company production rollup (hours by state×commodity) for membership targeting.
app.get('/production', async (req, res) => {
  if (SECRET && req.query.token !== SECRET) { res.status(401).json({ error: 'Invalid or missing token' }); return; }
  try {
    const d = await getData(req.query.fresh === '1');
    res.json(productionSlice(d));
  } catch (e) {
    console.error('build error:', e);
    res.status(500).json({ error: String(e && e.message || e) });
  }
});

// Diagnostics only — small, fast to eyeball after a build (does NOT force a rebuild).
app.get('/diag', async (req, res) => {
  if (SECRET && req.query.token !== SECRET) { res.status(401).json({ error: 'Invalid or missing token' }); return; }
  try {
    const d = await getData(req.query.fresh === '1');
    res.json({ diagnostics: d.diagnostics, lastUpdated: d.lastUpdated });
  } catch (e) {
    res.status(500).json({ error: String(e && e.message || e) });
  }
});

// Fast diagnostic: stream just the header line of each ZIP, then abort.
// Lets us see the real MSHA column names without a full (slow) rebuild.
function streamHeader(fileName) {
  return new Promise((resolve, reject) => {
    const req = https.get(BASE + fileName, { headers: { 'User-Agent': 'msha-service' } }, (res) => {
      if (res.statusCode !== 200) { reject(new Error(fileName + ' HTTP ' + res.statusCode)); return; }
      let done = false;
      res.pipe(unzipper.Parse())
        .on('entry', (entry) => {
          if (done || !/\.txt$/i.test(entry.path)) { entry.autodrain(); return; }
          let buf = '';
          entry.on('data', (chunk) => {
            if (done) return;
            buf += chunk.toString('utf8');
            const nl = buf.indexOf('\n');
            if (nl !== -1) {
              done = true;
              const header = buf.slice(0, nl).replace(/\r$/, '').split('|').map((s) => s.replace(/^"|"$/g, ''));
              try { req.destroy(); } catch (e) { /* ignore */ }
              resolve(header);
            }
          });
          entry.on('end', () => { if (!done) { done = true; resolve(buf.replace(/\r$/, '').split('|').map((s) => s.replace(/^"|"$/g, ''))); } });
          entry.on('error', reject);
        })
        .on('error', reject);
    });
    req.on('error', reject);
  });
}

app.get('/headers', async (req, res) => {
  if (SECRET && req.query.token !== SECRET) { res.status(401).json({ error: 'Invalid or missing token' }); return; }
  try {
    const out = {};
    const files = ['Accidents.zip', 'MinesProdYearly.zip', 'Violations.zip', 'Mines.zip', 'AddressOfRecord.zip'];
    for (const f of files) {
      try { out[f.replace('.zip', '')] = await streamHeader(f); }
      catch (e) { out[f.replace('.zip', '')] = 'ERROR: ' + ((e && e.message) || e); }
    }
    res.json(out);
  } catch (e) {
    res.status(500).json({ error: String((e && e.message) || e) });
  }
});

// Diagnostic: return the first N data rows of a file as {column: value} objects, so
// we can see the REAL status/product/lat-long/address spellings before wiring parsing.
// Usage: /sample?token=...&file=Mines.zip&n=3
function streamSample(fileName, n) {
  return new Promise((resolve, reject) => {
    const rows = [];
    let idx = null, header = null, count = 0, done = false;
    const req = https.get(BASE + fileName, { headers: { 'User-Agent': 'msha-service' } }, (res) => {
      if (res.statusCode !== 200) { reject(new Error(fileName + ' HTTP ' + res.statusCode)); return; }
      res.pipe(unzipper.Parse())
        .on('entry', (entry) => {
          if (done || !/\.txt$/i.test(entry.path)) { entry.autodrain(); return; }
          let leftover = '';
          const handle = (line) => {
            if (done || !line) return;
            const parts = line.split('|').map((s) => s.replace(/^"|"$/g, ''));
            if (idx === null) { idx = parts.map((h) => h.trim()); header = idx; return; }
            const obj = {};
            header.forEach((h, i) => { obj[h] = parts[i]; });
            rows.push(obj);
            if (++count >= n) { done = true; try { req.destroy(); } catch (e) {} resolve({ columns: header, rows: rows }); }
          };
          entry.on('data', (chunk) => {
            if (done) return;
            const text = leftover + chunk.toString('utf8');
            const ls = text.split('\n'); leftover = ls.pop();
            for (const l of ls) handle(l.replace(/\r$/, ''));
          });
          entry.on('end', () => { if (!done) { resolve({ columns: header, rows: rows }); } });
          entry.on('error', reject);
        })
        .on('error', reject);
    });
    req.on('error', reject);
  });
}

app.get('/sample', async (req, res) => {
  if (SECRET && req.query.token !== SECRET) { res.status(401).json({ error: 'Invalid or missing token' }); return; }
  const file = (req.query.file || 'Mines.zip').replace(/[^A-Za-z0-9._-]/g, '');
  const n = Math.min(parseInt(req.query.n, 10) || 3, 20);
  try {
    res.json(await streamSample(file, n));
  } catch (e) {
    res.status(500).json({ error: String((e && e.message) || e) });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log('msha-service listening on ' + PORT));
