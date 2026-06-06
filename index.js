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
const unzipper = require('unzipper');

const BASE = 'https://arlweb.msha.gov/OpenGovernmentData/DataSets/';
const SECRET = process.env.SECRET || '';

const FIPS_TO_STATE = {
  '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA', '08': 'CO', '09': 'CT', '10': 'DE',
  '11': 'DC', '12': 'FL', '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN', '19': 'IA',
  '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME', '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN',
  '28': 'MS', '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH', '34': 'NJ', '35': 'NM',
  '36': 'NY', '37': 'NC', '38': 'ND', '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
  '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT', '50': 'VT', '51': 'VA', '53': 'WA',
  '54': 'WV', '55': 'WI', '56': 'WY'
};
const INJURY_CODES = { '01': true, '02': true, '03': true, '04': true, '05': true, '06': true };
const FATALITY_CODE = '01';

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

async function buildData() {
  const currentYear = new Date().getFullYear();
  const tenYrStart = currentYear - 10;

  const fatalByYear = {};
  const injuryByYear = {};
  const causeBySubunit = {};
  const stateData = {};
  const hoursByYear = {};
  // Injuries-page breakdowns (non-fatal reportable injuries, last 10 yrs)
  const injSeverity = {};
  const injCause = {};
  const injOp = { Facility: 0, Surface: 0, Underground: 0 };

  // 1) Accidents.zip → fatalities, injuries, causes, state breakdown
  await streamZip('Accidents.zip', null, (idx, p) => {
    const cm = (p[idx['COAL_METAL_IND']] || '').trim();
    if (cm !== 'M') return;
    const yr = parseInt((p[idx['CAL_YR']] || '0').trim(), 10);
    if (!yr) return;
    const deg = (p[idx['DEGREE_INJURY_CD']] || '').trim();
    const cls = (p[idx['CLASSIFICATION']] || 'Unknown').trim() || 'Unknown';
    const sub = (p[idx['SUBUNIT']] || '').trim().toLowerCase();
    const st = (p[idx['FIPS_STATE_CD']] || '').trim();

    const isUG = sub.indexOf('underground') !== -1;
    const isFacility = sub.indexOf('facility') !== -1 || sub.indexOf('mill') !== -1 || sub.indexOf('shop') !== -1 || sub.indexOf('office') !== -1;

    if (deg === FATALITY_CODE) {
      fatalByYear[yr] = (fatalByYear[yr] || 0) + 1;
      if (st) { (stateData[st] = stateData[st] || { fatalities: 0, injuries: 0 }).fatalities++; }
    }
    if (INJURY_CODES[deg]) {
      injuryByYear[yr] = (injuryByYear[yr] || 0) + 1;
      if (st) { (stateData[st] = stateData[st] || { fatalities: 0, injuries: 0 }).injuries++; }
    }

    if (yr >= tenYrStart) {
      // Fatalities-by-cause pies (fatalities only — these feed the Fatalities page)
      if (deg === FATALITY_CODE) {
        const c = causeBySubunit[cls] || (causeBySubunit[cls] = { Cause: cls, Fac: 0, Surf: 0, UG: 0 });
        if (isFacility) c.Fac++; else if (isUG) c.UG++; else c.Surf++;
      }
      // Injury breakdowns (non-fatal reportable injuries — these feed the Injuries page)
      if (deg !== FATALITY_CODE && (INJURY_CODES[deg] || deg === '07')) {
        const sev = DEGREE_LABELS[deg] || 'Other';
        injSeverity[sev] = (injSeverity[sev] || 0) + 1;
        injCause[cls] = (injCause[cls] || 0) + 1;
        if (isFacility) injOp.Facility++; else if (isUG) injOp.Underground++; else injOp.Surface++;
      }
    }
  });

  // 2) MinesProdYearly.zip → employee-hours by year (for injury rate)
  try {
    let hCm, hYr, hHrs;
    await streamZip('MinesProdYearly.zip', (idx) => {
      hCm = col(idx, ['COAL_METAL_IND']);
      hYr = col(idx, ['CAL_YR', 'CALENDAR_YR', 'PROD_CAL_YR']);
      hHrs = col(idx, ['ANNUAL_HRS', 'ANNUAL_HOURS', 'HRS_WORKED', 'EMPLOYEE_HRS', 'EMPLOYEE_HOURS', 'HOURS_WORKED']);
    }, (_idx, p) => {
      // Filter to Metal/Non-Metal only if that column exists in this file.
      if (hCm !== undefined && (p[hCm] || '').trim() !== 'M') return;
      if (hYr === undefined || hHrs === undefined) return;
      const yr = parseInt((p[hYr] || '0').trim(), 10);
      const hours = parseFloat((p[hHrs] || '0').trim());
      if (!yr || !hours) return;
      hoursByYear[yr] = (hoursByYear[yr] || 0) + hours;
    });
  } catch (e) {
    console.warn('MinesProdYearly failed (injury rate unavailable):', e.message);
  }

  // 3) Violations.zip → top CFR sections (last 3 yrs) + citations by state & year
  const citByStateYear = {};   // { fips: { year: count } }
  const cfrAgg = {};           // { section: { count, ss } } — last 3 yrs
  const violYears = {};        // set of years seen
  try {
    let vCm, vYr, vCfr, vSS, vSt;
    const threeYrStart = currentYear - 3;
    await streamZip('Violations.zip', (idx) => {
      vCm = col(idx, ['COAL_METAL_IND']);
      vYr = col(idx, ['CAL_YR', 'VIOLATION_ISSUE_YR']);
      vCfr = col(idx, ['CFR_STANDARD', 'SECTION_OF_ACT', 'STANDARD', 'CFR_STANDARD_CD']);
      vSS = col(idx, ['SIG_SUB', 'SIG_AND_SUB', 'S_AND_S', 'SS_IND']);
      vSt = col(idx, ['FIPS_STATE_CD', 'STATE_FIPS_CD']);
    }, (_idx, p) => {
      if (vCm !== undefined && (p[vCm] || '').trim() !== 'M') return;
      if (vYr === undefined) return;
      const yr = parseInt((p[vYr] || '0').trim(), 10);
      if (!yr) return;
      // citations by state & year (full slider range)
      if (vSt !== undefined) {
        const st = (p[vSt] || '').trim();
        if (st) {
          (citByStateYear[st] = citByStateYear[st] || {});
          citByStateYear[st][yr] = (citByStateYear[st][yr] || 0) + 1;
          violYears[yr] = true;
        }
      }
      // top CFR sections — last 3 yrs only
      if (yr >= threeYrStart && vCfr !== undefined) {
        const sec = (p[vCfr] || '').trim();
        if (sec) {
          const a = cfrAgg[sec] || (cfrAgg[sec] = { count: 0, ss: 0 });
          a.count++;
          if (vSS !== undefined && (p[vSS] || '').trim().toUpperCase() === 'Y') a.ss++;
        }
      }
    });
  } catch (e) {
    console.warn('Violations failed (citations unavailable):', e.message);
  }

  const allFatalities = Object.keys(fatalByYear).map(Number).sort((a, b) => a - b)
    .map((yr) => ({ Year: yr, Fatalities: fatalByYear[yr] }));

  const injuryArr = Object.keys(injuryByYear).map(Number).sort((a, b) => a - b)
    .map((yr) => ({ Year: yr, Injuries: injuryByYear[yr] }));

  const tenYrCauses = Object.values(causeBySubunit)
    .map((c) => ({ ...c, Sum: c.Fac, Sum_1: c.Surf, Sum_2: c.UG, Total: c.Fac + c.Surf + c.UG }))
    .sort((a, b) => b.Total - a.Total)
    .slice(0, 15);

  const injuryRate = injuryArr.map((row) => {
    const hrs = hoursByYear[row.Year] || 0;
    const rate = hrs > 0 ? Number(((row.Injuries / hrs) * 200000).toFixed(2)) : null;
    return { Year: row.Year, Injuries: row.Injuries, Hours: Math.round(hrs), Rate: rate };
  }).filter((r) => r.Rate !== null);

  const stateByAbbr = {};
  Object.keys(stateData).forEach((fips) => {
    const abbr = FIPS_TO_STATE[fips];
    if (abbr) stateByAbbr[abbr] = stateData[fips];
  });

  // Injuries-page payload
  const injuries = {
    byYear: Object.keys(injuryByYear).map(Number).sort((a, b) => a - b)
      .map((yr) => ({ Year: yr, Count: injuryByYear[yr] })),
    bySeverity: Object.keys(injSeverity).map((k) => ({ label: k, count: injSeverity[k] }))
      .sort((a, b) => b.count - a.count),
    byCause: Object.keys(injCause).map((k) => ({ label: k, count: injCause[k] }))
      .sort((a, b) => b.count - a.count).slice(0, 10),
    byOperation: Object.keys(injOp).map((k) => ({ label: k, count: injOp[k] }))
      .filter((o) => o.count > 0)
  };

  // Citations-page payload
  const topViolations = Object.keys(cfrAgg).map((sec) => {
    const a = cfrAgg[sec];
    const base = sec.split(/[()]/)[0];
    return {
      section: sec,
      description: CFR_DESCR[sec] || CFR_DESCR[base] || ('30 CFR §' + sec),
      count: a.count,
      ssCount: a.ss,
      typicallySS: a.ss > a.count / 2
    };
  }).sort((x, y) => y.count - x.count).slice(0, 15).map((v, i) => ({ ...v, rank: i + 1 }));

  const citByAbbr = {};
  Object.keys(citByStateYear).forEach((fips) => {
    const abbr = FIPS_TO_STATE[fips];
    if (abbr) citByAbbr[abbr] = citByStateYear[fips];
  });
  const violations = {
    topViolations,
    byStateYear: citByAbbr,
    years: Object.keys(violYears).map(Number).sort((a, b) => a - b)
  };

  return {
    allFatalities, tenYrCauses, injuryRate,
    stateData: stateByAbbr,
    injuries, violations,
    lastUpdated: new Date().toISOString()
  };
}

// Cache results in memory (warm instance) to avoid re-downloading on every call.
let CACHE = null;
let CACHE_TS = 0;
const CACHE_MS = 6 * 60 * 60 * 1000;

const app = express();

app.get('/', async (req, res) => {
  if (SECRET && req.query.token !== SECRET) {
    res.status(401).json({ error: 'Invalid or missing token' });
    return;
  }
  try {
    const fresh = req.query.fresh === '1';
    if (!fresh && CACHE && (Date.now() - CACHE_TS) < CACHE_MS) {
      res.json({ ...CACHE, cached: true });
      return;
    }
    const data = await buildData();
    CACHE = data; CACHE_TS = Date.now();
    res.json(data);
  } catch (e) {
    console.error('build error:', e);
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
    const accidents = await streamHeader('Accidents.zip');
    const prod = await streamHeader('MinesProdYearly.zip');
    const violations = await streamHeader('Violations.zip');
    res.json({ Accidents: accidents, MinesProdYearly: prod, Violations: violations });
  } catch (e) {
    res.status(500).json({ error: String((e && e.message) || e) });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log('msha-service listening on ' + PORT));
