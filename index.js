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
            const parts = line.split('|');
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

    if (deg === FATALITY_CODE) {
      fatalByYear[yr] = (fatalByYear[yr] || 0) + 1;
      if (st) { (stateData[st] = stateData[st] || { fatalities: 0, injuries: 0 }).fatalities++; }
    }
    if (INJURY_CODES[deg]) {
      injuryByYear[yr] = (injuryByYear[yr] || 0) + 1;
      if (st) { (stateData[st] = stateData[st] || { fatalities: 0, injuries: 0 }).injuries++; }
    }
    if (yr >= tenYrStart) {
      const c = causeBySubunit[cls] || (causeBySubunit[cls] = { Cause: cls, Fac: 0, Surf: 0, UG: 0 });
      if (sub.indexOf('facility') !== -1 || sub.indexOf('mill') !== -1 || sub.indexOf('shop') !== -1 || sub.indexOf('office') !== -1) c.Fac++;
      else if (sub.indexOf('underground') !== -1) c.UG++;
      else c.Surf++;
    }
  });

  // 2) MinesProdYearly.zip → employee-hours by year (for injury rate)
  try {
    await streamZip('MinesProdYearly.zip', null, (idx, p) => {
      const cm = (p[idx['COAL_METAL_IND']] || '').trim();
      if (cm !== 'M') return;
      const yr = parseInt((p[idx['CAL_YR']] || '0').trim(), 10);
      const hours = parseFloat((p[idx['ANNUAL_HOURS']] || '0').trim());
      if (!yr || !hours) return;
      hoursByYear[yr] = (hoursByYear[yr] || 0) + hours;
    });
  } catch (e) {
    console.warn('MinesProdYearly failed (injury rate unavailable):', e.message);
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

  return {
    allFatalities, tenYrCauses, injuryRate,
    stateData: stateByAbbr,
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

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log('msha-service listening on ' + PORT));
