/**
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║        PIPELINE LOG AGGREGATOR  v3.0  —  SMART EDITION          ║
 * ║                                                                  ║
 * ║  ✅ Multiple IDs simultaneously                                  ║
 * ║  ✅ Smart tiered polling:                                        ║
 * ║       ACTIVE  (mobile ne last 30s mein request kiya) → 5s poll  ║
 * ║       IDLE    (30s se koi request nahi)              → 30s poll  ║
 * ║       Jaise hi mobile /status call kare → turant ACTIVE ho jaao ║
 * ║  ✅ Auto-delete: 1 ghante se Axiom mein koi naya log nahi → bye  ║
 * ║  ✅ Parser: original HTML se 1:1 match (parseStructured,         ║
 * ║             parseCurl, parseFFmpeg, parseUploadProgress,         ║
 * ║             parseMeta, updatePhase — sab exact)                  ║
 * ║                                                                  ║
 * ║  ENV: AXIOM_TOKEN, AXIOM_DATASET, PORT                          ║
 * ╚══════════════════════════════════════════════════════════════════╝
 */

const express = require('express');
const cors    = require('cors');
const app     = express();
app.use(cors());
app.use(express.json());

// ─────────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────────
const PORT          = process.env.PORT         || 3001;
const AXIOM_TOKEN   = process.env.AXIOM_TOKEN  || '';
const AXIOM_DATASET = process.env.AXIOM_DATASET|| 'wasmer_logs';
const AXIOM_URL     = 'https://api.axiom.co/v1/datasets/_apl?format=legacy';

const ACTIVE_INTERVAL   = 5_000;          // 5s  — mobile actively dekh raha hai
const IDLE_INTERVAL     = 30_000;         // 30s — mobile ne last 30s mein nahi dekha
const ACTIVE_WINDOW     = 30_000;         // 30s tak koi request → ACTIVE tier
const AXIOM_PAGE_SIZE   = 1000;           // har baar Axiom se itne logs
const MAX_KEPT_LOGS     = 200;            // mobile ke liye (ffmpeg-free)
const AUTO_DELETE_AFTER = 60 * 60_000;    // 1 ghanta — koi naya log nahi → delete
const TICKER_MS         = 1_000;          // master ticker — 1s

// ─────────────────────────────────────────────────────────────────
// STORE  —  Map<batchId, BatchObj>
// ─────────────────────────────────────────────────────────────────
const store = new Map();

function makeBatch(id) {
  return {
    id,
    createdAt:         Date.now(),
    lastClientRequest: Date.now(),   // jab mobile ne /status call kiya
    lastNewAxiomLog:   Date.now(),   // jab Axiom se koi naya log aaya
    lastPollTime:      0,            // actual polling time (epoch ms)
    tier:             'active',      // 'active' | 'idle'
    polling:          false,         // concurrent poll guard
    logCount:         0,
    recentLogs:       [],
    seenKeys:         new Set(),
    lastSeenTs:       null,          // Axiom time-cursor
    state:            makeFreshState(),
  };
}

function makeFreshState() {
  return {
    phase:           'waiting',
    phaseIdx:        -1,
    overallPct:      0,
    srcDur:          0,              // seconds — ffPct ke liye zaroori
    srcInfo:         null,
    done:            false,
    totalElapsed:    null,
    errorMsg:        null,
    accessDenied:    false,

    // downloadActive flag — HTML ki tarah, curl % sirf tab parse hoga
    downloadActive:  false,

    download: { pct: 0, done: false, sizeMb: null },

    ffmpeg: {
      mode:           'single',      // 'single' | 'parallel'
      current:        null,
      fps: null, speed: null, time: null, frame: null, q: null, pct: 0,
      parallel: {
        a: { quality: null, fps: null, speed: null, time: null, pct: 0 },
        b: { quality: null, fps: null, speed: null, time: null, pct: 0 },
      },
    },

    upload: {
      quality: null, file: null, sizeMb: null,
      pct: 0, speed: null, transferred: null, dest: null,
    },

    qualities: {
      orig:  { status: 'pending', sizeMb: null, elapsed: null, ffPct: 0 },
      '240': { status: 'pending', sizeMb: null, elapsed: null, ffPct: 0 },
      '360': { status: 'pending', sizeMb: null, elapsed: null, ffPct: 0 },
      '480': { status: 'pending', sizeMb: null, elapsed: null, ffPct: 0 },
      '720': { status: 'pending', sizeMb: null, elapsed: null, ffPct: 0 },
      '1080':{ status: 'pending', sizeMb: null, elapsed: null, ffPct: 0 },
    },

    // Parallel state — HTML ki Set/array ki jagah arrays
    parallelBatch:   [],    // ['240','360']
    parallelQuals:   [],    // ordered [A, B]  — HTML ke parallelQuals
    currentEncoding: null,
    currentUploading:null,
  };
}

// ─────────────────────────────────────────────────────────────────
//  PARSER — HTML se 1:1 port (sab 6 functions)
// ─────────────────────────────────────────────────────────────────

const PHASES = [
  { id:'security', pct:5   },
  { id:'download', pct:15  },
  { id:'analyze',  pct:22  },
  { id:'original', pct:35  },
  { id:'audio',    pct:45  },
  { id:'p240',     pct:60  },
  { id:'p480',     pct:75  },
  { id:'p1080',    pct:90  },
  { id:'done',     pct:100 },
];

// PHASE_RANGES[phaseIdx] = [base, next] — ring interpolation ke liye
const PHASE_RANGES = [
  [0,5],[5,15],[15,22],[22,35],[35,45],[45,60],[60,75],[75,90],[90,100]
];

function normalizeQ(raw) {
  const r = (raw || '').toLowerCase().trim();
  if (r === 'orig' || r === 'original') return 'orig';
  if (r === '240p' || r === '240')      return '240';
  if (r === '360p' || r === '360')      return '360';
  if (r === '480p' || r === '480')      return '480';
  if (r === '720p' || r === '720')      return '720';
  if (r === '1080p'|| r === '1080')     return '1080';
  if (r.startsWith('audio'))            return r;
  return r;
}

// ── 1. classify ─────────────────────────────────────────────────
function classify(msg) {
  if (/\[ENCODE:START\]|\[ENCODE:DONE\]|\[ENCODE:SKIP\]/.test(msg))     return 'sys';
  if (/\[UPLOAD:START\]|\[UPLOAD:DONE\]/.test(msg))                      return 'upload';
  if (/\[PARALLEL:START\]|\[PARALLEL:DONE\]/.test(msg))                  return 'sys';
  if (/\[SOURCE:DOWNLOADED\]|\[SOURCE:INFO\]|\[SIZE:EXACT\]/.test(msg))  return 'sys';
  if (/\[AUDIO:START\]|\[AUDIO:DONE\]/.test(msg))                        return 'sys';
  if (/\[TIMING:TOTAL\]/.test(msg))                                       return 'success';
  if (/frame=\s*\d+.*fps=/.test(msg))                                     return 'ffmpeg';
  if (/✅.*\[HF\]|✅.*Uploaded|STD FILE|HUGE FILE/.test(msg))             return 'upload';
  if (/Error|Failed|DENIED|❌/.test(msg))                                  return 'error';
  if (/WARNING|⚠️/.test(msg))                                              return 'warn';
  if (/COMPLETE|🎉|All Tasks Finished|✅/.test(msg))                       return 'success';
  if (/⏭️|SKIP/.test(msg))                                                 return 'skip';
  if (/INIT|SECURITY|TOOLS|BOOTUP|FFPROBE|STATS|PRIORITY|AUDIO|TRANSCODE|SYNC|SIZE|DOWNLOAD|ANALYZING/i.test(msg)) return 'sys';
  return 'default';
}

// ── 2. parseCurl ────────────────────────────────────────────────
// BUG FIX same as HTML: sirf downloadActive=true hone par run karo
// Warna libx264 stats jaise "ref B L1: 95.9%" galat bar update kar denge!
function parseCurl(msg, st) {
  if (!st.downloadActive) return;
  const m = msg.match(/(\d+\.?\d*)\s*%\s*$/);
  if (!m) return;
  const pct = parseFloat(m[1]);
  st.download.pct = pct;
  if (pct >= 100) { st.download.done = true; st.downloadActive = false; }
}

// ── 3. parseFFmpeg ──────────────────────────────────────────────
function parseFFmpeg(msg, st) {
  const tagM  = msg.match(/^\[FF:([^\]]+)\]\s*(.*)/s);
  const ffTag  = tagM ? tagM[1].trim() : null;
  const ffBody = tagM ? tagM[2] : msg;
  if (!/frame=\s*\d+/.test(ffBody)) return;

  const g    = re => (ffBody.match(re) || [])[1];
  const fps   = g(/fps=\s*([\d.]+)/);
  const speed = g(/speed=\s*(\S+)/);
  const frame = g(/frame=\s*(\d+)/);
  const q     = g(/q=([\d.-]+)/);
  const tstr  = g(/time=\s*([\d:.]+)/);
  const tagQ  = ffTag ? normalizeQ(ffTag) : null;

  // ffPct computation — same as HTML
  let ffPct = 0;
  if (tstr) {
    const p = tstr.split(':').map(parseFloat);
    const s = p[0] * 3600 + p[1] * 60 + p[2];
    ffPct   = st.srcDur > 0 ? Math.min(100, (s / st.srcDur) * 100) : 50;
  }

  const isParallel = st.parallelBatch.length >= 2 && st.parallelQuals.length >= 2;

  // ── PARALLEL MODE ──
  if (isParallel && tagQ) {
    let panel = null;
    if (tagQ === st.parallelQuals[0])      panel = 'a';
    else if (tagQ === st.parallelQuals[1]) panel = 'b';

    if (panel) {
      const p = st.ffmpeg.parallel[panel];
      if (fps)   p.fps   = fps;
      if (speed) p.speed = speed.replace('x', '') + 'x';
      if (tstr)  { p.time = tstr; p.pct = st.srcDur > 0 ? ffPct.toFixed(1) : '—'; }
      if (st.qualities[tagQ]) st.qualities[tagQ].ffPct = parseFloat(ffPct.toFixed(1));
    }
    return; // parallel mein single panel update nahi karna
  }

  // ── SINGLE MODE ──
  if (fps)   st.ffmpeg.fps   = fps;
  if (speed) st.ffmpeg.speed = speed.replace('x', '') + 'x';
  if (frame) st.ffmpeg.frame = frame;
  if (q)     st.ffmpeg.q     = q;
  if (tstr)  { st.ffmpeg.time = tstr; st.ffmpeg.pct = parseFloat(ffPct.toFixed(1)); }

  const displayQ = tagQ || st.currentEncoding;
  if (displayQ) st.ffmpeg.current = displayQ;

  // Quality card ffPct
  const targetQ = tagQ || st.currentEncoding;
  if (targetQ && st.qualities[targetQ])
    st.qualities[targetQ].ffPct = parseFloat(ffPct.toFixed(1));

  // Ring interpolation — HTML ki tarah
  if (st.phaseIdx >= 0 && st.srcDur > 0 && ffPct > 0) {
    const [base, next] = PHASE_RANGES[st.phaseIdx] || [st.overallPct, st.overallPct];
    st.overallPct = parseFloat((base + (next - base) * (ffPct / 100)).toFixed(1));
  }
}

// ── 4. parseUploadProgress ──────────────────────────────────────
// HuggingFace tqdm: "kernel_shard_ORG123.dat:  45%|████ | 360M/800M, 5.2MB/s"
function parseUploadProgress(msg, st) {
  const m = msg.match(/^(\S+\.\w+):\s+(\d+)%\|[^|]*\|\s*([\d.]+\w+)\s*\/\s*([\d.]+\w+)(?:,\s*([\d.]+\w+\/s))?/);
  if (!m) return;
  const [, fname, pct, xfr, total, speed] = m;
  if (!st.upload.quality) return; // koi upload chal nahi raha to ignore
  st.upload.file        = fname;
  st.upload.pct         = parseInt(pct);
  st.upload.transferred = `${xfr} / ${total}`;
  st.upload.speed       = speed || null;
}

// ── 5. parseMeta ────────────────────────────────────────────────
// Legacy [STATS] line se bhi srcDur aur srcInfo extract karo
function parseMeta(msg, st) {
  const h = (msg.match(/Source Height[:\s]*(\d+)/) || [])[1];
  const d = (msg.match(/Duration[:\s]*([\d.]+)s/)  || [])[1];
  const a = (msg.match(/Audio Tracks[:\s]*(\d+)/)  || [])[1];
  if (h || d || a) {
    if (!st.srcInfo) st.srcInfo = {};
    if (h) st.srcInfo.height     = h;
    if (d) { st.srcDur = parseFloat(d); st.srcInfo.duration = d; }
    if (a) st.srcInfo.audioTracks = a;
  }
  // [SIZE] Original File: 800 MB  (human-readable log)
  const sm = msg.match(/\[SIZE\]\s*(.+?):\s*(\d+)\s*MB/i);
  if (sm) {
    const rawQ = sm[1].toLowerCase().replace(/\s/g, '');
    const mb   = sm[2];
    const smap = {
      '240p':'240','360p':'360','480p':'480','720p':'720','1080p':'1080',
      'originalfile':'orig','original':'orig',
    };
    const q = smap[rawQ];
    if (q && st.qualities[q]) st.qualities[q].sizeMb = mb;
  }
}

// ── 6. parseStructured ──────────────────────────────────────────
function parseStructured(msg, st) {

  // [SOURCE:DOWNLOADED] bytes=N
  const sdl = msg.match(/\[SOURCE:DOWNLOADED\]\s+bytes=(\d+)/);
  if (sdl) {
    st.downloadActive   = false;
    st.download.done    = true;
    st.download.pct     = 100;
    st.download.sizeMb  = (parseInt(sdl[1]) / 1048576).toFixed(1);
  }

  // [SOURCE:INFO] height=N duration=N audio_tracks=N
  const si = msg.match(/\[SOURCE:INFO\]\s+height=(\d+)\s+duration=(\d+)\s+audio_tracks=(\d+)/);
  if (si) {
    st.srcDur  = parseInt(si[2]);
    st.srcInfo = { height: si[1], duration: si[2], audioTracks: si[3] };
  }

  // [ENCODE:START] quality=X
  const es = msg.match(/\[ENCODE:START\]\s+quality=(\S+)/);
  if (es) {
    const q = normalizeQ(es[1]);
    st.currentEncoding = q;
    const inParallel   = st.parallelBatch.includes(q);
    if (!inParallel && st.qualities[q]) st.qualities[q].status = 'running';
    // ffmpeg card label — HTML ki tarah
    if (st.parallelBatch.length > 0)
      st.ffmpeg.current = st.parallelBatch.join('+');
    else
      st.ffmpeg.current = q;
  }

  // [ENCODE:DONE] quality=X elapsed=Xs
  const ed = msg.match(/\[ENCODE:DONE\]\s+quality=(\S+)(?:\s+elapsed=(\S+))?/);
  if (ed) {
    const q = normalizeQ(ed[1]);
    if (st.qualities[q] && ed[2]) st.qualities[q].elapsed = ed[2];
    const inParallel = st.parallelBatch.includes(q);
    if (st.currentEncoding === q && !inParallel) {
      st.currentEncoding  = null;
      st.ffmpeg.current   = null;
    }
  }

  // [ENCODE:SKIP] quality=X
  const esk = msg.match(/\[ENCODE:SKIP\]\s+quality=(\S+)/);
  if (esk) {
    const q = normalizeQ(esk[1]);
    if (st.qualities[q]) st.qualities[q].status = 'skip';
  }

  // [SIZE:EXACT] quality=X bytes=N mb=N
  const se = msg.match(/\[SIZE:EXACT\]\s+quality=(\S+)\s+bytes=\d+\s+mb=(\d+)/);
  if (se) {
    const q = normalizeQ(se[1]);
    if (st.qualities[q]) st.qualities[q].sizeMb = se[2];
  }

  // [UPLOAD:START] quality=X file=X mb=N dest=X
  const us = msg.match(/\[UPLOAD:START\]\s+quality=(\S+)\s+file=(\S+)(?:\s+mb=(\d+))?(?:\s+dest=(\S+))?/);
  if (us) {
    const q = normalizeQ(us[1]);
    st.currentUploading = q;
    st.upload = {
      quality: q, file: us[2], sizeMb: us[3] || null,
      pct: 0, speed: null, transferred: `0 / ${us[3] || '?'}MB`, dest: us[4] || 'hf',
    };
  }

  // [UPLOAD:DONE] quality=X
  const ud = msg.match(/\[UPLOAD:DONE\]\s+quality=(\S+)/);
  if (ud) {
    const q = normalizeQ(ud[1]);
    if (!q.startsWith('audio') && st.qualities[q] && st.qualities[q].status !== 'skip')
      st.qualities[q].status = 'done';
    st.currentUploading = null;
    st.upload = { quality: null, file: null, sizeMb: null, pct: 0, speed: null, transferred: null, dest: null };
  }

  // [PARALLEL:START] batch=N qualities=X,Y
  const ps = msg.match(/\[PARALLEL:START\]\s+batch=(\d+)\s+qualities=(\S+)/i);
  if (ps) {
    const quals         = ps[2].split(',').map(normalizeQ);
    st.parallelBatch    = quals;
    st.parallelQuals    = quals;                    // ordered [A, B]
    st.ffmpeg.mode      = 'parallel';
    st.ffmpeg.current   = quals.join(' + ');
    st.ffmpeg.parallel.a = { quality: quals[0] || null, fps: null, speed: null, time: null, pct: 0 };
    st.ffmpeg.parallel.b = { quality: quals[1] || null, fps: null, speed: null, time: null, pct: 0 };
    quals.forEach(q => { if (st.qualities[q]) st.qualities[q].status = 'parallel'; });
  }

  // [PARALLEL:DONE]
  if (/\[PARALLEL:DONE\]/.test(msg)) {
    st.parallelBatch    = [];
    st.parallelQuals    = [];
    st.ffmpeg.mode      = 'single';
    st.currentEncoding  = null;
    st.ffmpeg.current   = null;
  }

  // [AUDIO:START] track=N lang=X
  const as_ = msg.match(/\[AUDIO:START\]\s+track=(\d+)\s+lang=(\S+)/);
  if (as_) {
    st.currentEncoding = `audio${as_[1]}`;
    st.ffmpeg.current  = `AUDIO ${as_[2]}`;
  }

  // [AUDIO:DONE]
  if (/\[AUDIO:DONE\]/.test(msg)) {
    st.currentEncoding = null;
    st.ffmpeg.current  = null;
  }

  // [TIMING:TOTAL] elapsed=Xs
  const tt = msg.match(/\[TIMING:TOTAL\]\s+elapsed=(\S+)/);
  if (tt) {
    st.totalElapsed = tt[1];
    st.done         = true;
    st.overallPct   = 100;
  }
}

// ── 7. updatePhase ──────────────────────────────────────────────
// HTML ke updatePhase se exact match
function updatePhase(msg, st) {
  const m   = msg.toUpperCase();
  let idx   = st.phaseIdx;

  if (/VERIFYING CREDENTIALS|SECURITY/.test(m))                 idx = 0;
  if (/DOWNLOADING SOURCE/.test(m)) {
    st.downloadActive = true;                                    // parseCurl unlock
    idx = 1;
  }
  if (/FFPROBE|ANALYZING SOURCE/.test(m))                       idx = 2;
  if (/\[SOURCE:DOWNLOADED\]/.test(m))                          idx = 1;  // HTML mein yehi hai
  if (/\[SOURCE:INFO\]/.test(m))                                idx = 2;
  if (/\[PRIORITY\].*ORIGINAL|\[ENCODE:START\].*ORIG/.test(m)) idx = 3;
  if (/\[AUDIO\]/.test(m) && !/\[AUDIO:/.test(m))              idx = 4;  // [AUDIO] Extracting... line
  if (/\[PARALLEL:START\].*BATCH=1/.test(m))                    idx = 5;
  if (/\[PARALLEL:START\].*BATCH=2/.test(m))                    idx = 6;
  if (/\[ENCODE:START\].*1080/.test(m))                         idx = 7;
  if (/COMPLETE|ALL TASKS FINISHED/.test(m))                    idx = 8;

  if (/ACCESS DENIED/.test(m)) st.accessDenied = true;

  if (idx !== st.phaseIdx) {
    st.phaseIdx    = idx;
    st.phase       = idx >= 0 ? PHASES[idx].id : 'waiting';
    // Sirf aage badhne par pct update karo (ffmpeg interpolation protect karo)
    if (idx >= 0 && st.overallPct < PHASES[idx].pct)
      st.overallPct = PHASES[idx].pct;
  }

  // COMPLETE — sab qualities done mark karo
  if (/COMPLETE|ALL TASKS FINISHED/.test(m)) {
    st.done             = true;
    st.downloadActive   = false;
    st.currentEncoding  = null;
    st.currentUploading = null;
    st.parallelBatch    = [];
    st.parallelQuals    = [];
    Object.entries(st.qualities).forEach(([, qv]) => {
      if (qv.status !== 'skip') qv.status = 'done';
    });
    st.overallPct = 100;
  }
}

// ── Master processLine — sab 6 functions exact order mein ───────
// HTML mein order: parseStructured → parseCurl → parseFFmpeg →
//                  parseUploadProgress → parseMeta → updatePhase
function processLine(msg, st) {
  parseStructured(msg, st);
  parseCurl(msg, st);
  parseFFmpeg(msg, st);
  parseUploadProgress(msg, st);
  parseMeta(msg, st);
  updatePhase(msg, st);
}

// ── Raw log se clean message nikaalo (HTML ki processLogs se same) ─
function extractMsg(log) {
  let raw = log.data || log;
  let msg = '';
  if (typeof raw === 'string')    msg = raw;
  else if (raw.message)           msg = raw.message;
  else if (raw.data?.message)     msg = raw.data.message;
  else                            msg = JSON.stringify(raw);
  msg = msg.replace(/^"|"$/g, '').replace(/\\"/g, '"').trim();
  // Empty placeholder skip karo
  if (/^\{"data":\{"batch_id":"[^"]+","message":""\}\}$/.test(msg)) return null;
  return msg || null;
}

// ─────────────────────────────────────────────────────────────────
//  AXIOM FETCH — paginated, cursor-based
// ─────────────────────────────────────────────────────────────────
async function fetchFromAxiom(batch) {
  if (!AXIOM_TOKEN) return;
  if (batch.polling) return;          // concurrent poll guard
  batch.polling   = true;
  batch.lastPollTime = Date.now();

  try {
    const end   = new Date();
    const start = batch.lastSeenTs
      ? new Date(batch.lastSeenTs)
      : new Date(end - 4 * 60 * 60_000); // pehli baar: 4 ghante back

    const apl = `['${AXIOM_DATASET}'] | where data.batch_id == "${batch.id}" | sort by _time asc | limit ${AXIOM_PAGE_SIZE}`;

    const res = await fetch(AXIOM_URL, {
      method:  'POST',
      headers: {
        'Authorization': `Bearer ${AXIOM_TOKEN}`,
        'Content-Type':  'application/json',
      },
      body:   JSON.stringify({ apl, startTime: start.toISOString(), endTime: end.toISOString() }),
      signal: AbortSignal.timeout(10_000),
    });

    if (res.status === 422) return;        // Axiom ka "no results" — normal
    if (!res.ok) { console.warn(`⚠️  [AGG] Axiom HTTP ${res.status} for ${batch.id}`); return; }

    const data    = await res.json();
    const matches = data.matches || data.data || data.results || [];
    if (!matches.length) return;

    let newCount = 0;
    let latestTs = batch.lastSeenTs ? new Date(batch.lastSeenTs).getTime() : 0;

    // Time-sorted process (Axiom already sorted karta hai, par ensure karte hain)
    matches.forEach(log => {
      const msg = extractMsg(log);
      if (!msg) return;

      const ts  = log._time ? new Date(log._time).getTime() : Date.now();
      const key = `${ts}|${msg.substring(0, 80)}`;
      if (batch.seenKeys.has(key)) return;
      batch.seenKeys.add(key);

      if (ts > latestTs) latestTs = ts;

      // Server pe parse karo
      processLine(msg, batch.state);
      batch.logCount++;
      newCount++;

      // Mobile ke liye: ffmpeg frames mat rakho (bahut zyada hain)
      const type = classify(msg);
      if (type !== 'ffmpeg') {
        batch.recentLogs.push({
          t:    new Date(ts).toLocaleTimeString('en-GB', { hour12: false }),
          ts,
          type,
          msg:  msg.substring(0, 300),
        });
      }
    });

    // Cursor aage badhao — +1ms taki same log dobara na aaye
    if (latestTs > 0) {
      batch.lastSeenTs    = new Date(latestTs + 1).toISOString();
      batch.lastNewAxiomLog = Date.now();   // auto-delete ka timer reset
    }

    // Mobile ke liye sirf last MAX_KEPT_LOGS rakho
    if (batch.recentLogs.length > MAX_KEPT_LOGS)
      batch.recentLogs = batch.recentLogs.slice(-MAX_KEPT_LOGS);

    if (newCount > 0)
      console.log(`📥 [${batch.id}] +${newCount} (total:${batch.logCount}) tier:${batch.tier} phase:${batch.state.phase}`);

  } catch (err) {
    console.error(`❌ [AGG] fetchFromAxiom ${batch.id}:`, err.message);
  } finally {
    batch.polling = false;
  }
}

// ─────────────────────────────────────────────────────────────────
//  MASTER TICKER — 1s interval, saari IDs smart tarike se handle
// ─────────────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();

  store.forEach((batch, id) => {

    // ── 1. AUTO-DELETE: 1 ghante se koi naya Axiom log nahi ──────
    if (now - batch.lastNewAxiomLog > AUTO_DELETE_AFTER) {
      console.log(`🗑️  [AGG] Auto-deleting idle batch: ${id} (no new logs for 1h)`);
      store.delete(id);
      return;
    }

    // ── 2. TIER UPDATE: 30s se mobile request nahi → IDLE ────────
    const clientAge = now - batch.lastClientRequest;
    if (batch.tier === 'active' && clientAge > ACTIVE_WINDOW) {
      batch.tier = 'idle';
      console.log(`😴 [${id}] Shifted to IDLE tier`);
    }

    // ── 3. DONE batches: IDLE tier pe force karo ─────────────────
    if (batch.state.done && batch.tier === 'active') {
      batch.tier = 'idle';
    }

    // ── 4. POLL DECISION ─────────────────────────────────────────
    const pollInterval = batch.tier === 'active' ? ACTIVE_INTERVAL : IDLE_INTERVAL;
    const timeSincePoll = now - batch.lastPollTime;

    if (timeSincePoll >= pollInterval) {
      fetchFromAxiom(batch); // async, guard ke saath
    }
  });

}, TICKER_MS);

// ─────────────────────────────────────────────────────────────────
//  ROUTES
// ─────────────────────────────────────────────────────────────────

/**
 * GET /start/:batch_id
 * logic.sh shuruaat mein call karta hai — batch create karo, ACTIVE karo
 * Frontend se bhi manually call kar sakte hain
 */
app.get('/start/:batch_id', (req, res) => {
  const id = req.params.batch_id.trim();
  if (!id) return res.status(400).json({ error: 'batch_id required' });

  let batch = store.get(id);
  const isNew = !batch;

  if (isNew) {
    batch = makeBatch(id);
    store.set(id, batch);
    console.log(`🚀 [AGG] Batch STARTED: ${id}`);
  } else {
    // Pehle se hai → ACTIVE tier pe wapas laao (agar IDLE tha)
    batch.lastClientRequest = Date.now();
    batch.tier = 'active';
    console.log(`🔄 [AGG] Batch RE-ACTIVATED: ${id}`);
  }

  // Turant pehla poll karo (wait mat karo)
  fetchFromAxiom(batch);

  res.json({
    ok:    true,
    id,
    isNew,
    tier:  batch.tier,
    msg:   isNew ? 'Batch created, Axiom polling started!' : 'Batch re-activated!',
  });
});

/**
 * GET /status/:batch_id
 * Mobile har 3s yahan se compact JSON leta hai (~1-3KB)
 * Request aate hi → ACTIVE tier mein shift (agar IDLE tha)
 */
app.get('/status/:batch_id', (req, res) => {
  const id    = req.params.batch_id.trim();
  let batch   = store.get(id);

  if (!batch) {
    // Auto-create karo — /start call nahi hua to bhi kaam kare
    batch = makeBatch(id);
    store.set(id, batch);
    fetchFromAxiom(batch);
    console.log(`🔄 [AGG] Auto-created batch on /status call: ${id}`);
  }

  // Mobile ne request kiya → ACTIVE tier!
  const wasIdle = batch.tier === 'idle';
  batch.lastClientRequest = Date.now();
  if (wasIdle) {
    batch.tier = 'active';
    console.log(`⚡ [${id}] IDLE → ACTIVE (mobile request aaya)`);
  }

  res.json({
    found:     true,
    batch_id:  batch.id,
    logCount:  batch.logCount,
    tier:      batch.tier,
    updatedAt: new Date(batch.lastNewAxiomLog).toISOString(),
    state:     batch.state,
    logs:      batch.recentLogs,
  });
});

/**
 * GET /health
 */
app.get('/health', (req, res) => {
  const batches = [];
  store.forEach((b, id) => batches.push({
    id,
    tier:       b.tier,
    logCount:   b.logCount,
    phase:      b.state.phase,
    done:       b.state.done,
    idleSince:  Math.round((Date.now() - b.lastClientRequest) / 1000) + 's',
    axiomSince: Math.round((Date.now() - b.lastNewAxiomLog)   / 1000) + 's',
  }));
  res.json({
    status:      'ok',
    uptime:      Math.round(process.uptime()) + 's',
    axiomReady:  !!AXIOM_TOKEN,
    dataset:     AXIOM_DATASET,
    activeBatches: batches.filter(b => b.tier === 'active').length,
    idleBatches:   batches.filter(b => b.tier === 'idle').length,
    batches,
  });
});

/**
 * DELETE /clear/:batch_id — manual cleanup
 */
app.delete('/clear/:batch_id', (req, res) => {
  store.delete(req.params.batch_id);
  res.json({ ok: true, deleted: req.params.batch_id });
});

app.listen(PORT, () => {
  console.log(`\n🚀 [AGG] Pipeline Log Aggregator v3.0`);
  console.log(`   Port:    ${PORT}`);
  console.log(`   Axiom:   ${AXIOM_TOKEN ? '✅ Token set' : '❌ AXIOM_TOKEN missing!'}`);
  console.log(`   Dataset: ${AXIOM_DATASET}`);
  console.log(`   Tiers:   ACTIVE=${ACTIVE_INTERVAL/1000}s | IDLE=${IDLE_INTERVAL/1000}s | AutoDelete=${AUTO_DELETE_AFTER/60000}min\n`);
});
