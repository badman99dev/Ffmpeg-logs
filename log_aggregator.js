/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║         PIPELINE LOG AGGREGATOR SERVER v2.0                  ║
 * ║                                                              ║
 * ║  Flow:  logic.sh → Axiom (unchanged ✅)                      ║
 * ║         Aggregator → Axiom se khud pull karta hai            ║
 * ║         3000 row limit? Aggregator time-cursor se aage       ║
 * ║         jaata hai — sab logs memory mein jama karta hai      ║
 * ║         Mobile → /status/:id → ~1KB compact JSON 🔋          ║
 * ╚══════════════════════════════════════════════════════════════╝
 *
 * ENV VARS:
 *   AXIOM_TOKEN    — Axiom Bearer token
 *   AXIOM_DATASET  — e.g. "wasmer_logs"
 *   PORT           — default 3001
 *
 * Deploy: Railway / Render / Fly.io
 * npm install express cors node-fetch
 * node log_aggregator.js
 */

const express = require('express');
const cors    = require('cors');
const app     = express();

app.use(cors());
app.use(express.json());

const PORT           = process.env.PORT || 3001;
const AXIOM_TOKEN    = process.env.AXIOM_TOKEN    || '';
const AXIOM_DATASET  = process.env.AXIOM_DATASET  || 'wasmer_logs';
const AXIOM_APL_URL  = 'https://api.axiom.co/v1/datasets/_apl?format=legacy';

const CLEANUP_AFTER  = 2 * 60 * 60 * 1000; // 2 hours
const POLL_INTERVAL  = 8 * 1000;            // Axiom ko har 8 seconds poll karo
const AXIOM_PAGE     = 1000;                // Har baar 1000 logs lo (safe under limit)
const MAX_KEPT_LOGS  = 200;                 // Mobile ko dikhane ke liye (ffmpeg-free)

// ─────────────────────────────────────────────────────────────────
// IN-MEMORY STORE
// ─────────────────────────────────────────────────────────────────
const store = new Map(); // batchId → batchData

function getOrCreate(batchId) {
  if (!store.has(batchId)) {
    console.log(`✅ [AGG] New batch watching started: ${batchId}`);
    const batch = {
      id:          batchId,
      startedAt:   new Date().toISOString(),
      updatedAt:   new Date().toISOString(),
      logCount:    0,
      recentLogs:  [],       // Mobile ke liye (no ffmpeg frames)
      seenKeys:    new Set(),// Duplicate avoid karne ke liye (msg+ts fingerprint)
      lastSeenTs:  null,     // Cursor — is timestamp ke baad ke logs laao
      cleanupTimer: null,
      pollTimer:   null,
      state:       createFreshState(),
    };
    store.set(batchId, batch);
    scheduleCleanup(batch);
    // Polling start karo turant
    startPolling(batch);
  }
  return store.get(batchId);
}

function scheduleCleanup(batch) {
  if (batch.cleanupTimer) clearTimeout(batch.cleanupTimer);
  batch.cleanupTimer = setTimeout(() => {
    stopPolling(batch);
    store.delete(batch.id);
    console.log(`🧹 [AGG] Auto-cleaned batch: ${batch.id}`);
  }, CLEANUP_AFTER);
}

function createFreshState() {
  return {
    phase:        'waiting',
    phaseIdx:     -1,
    overallPct:   0,
    srcDur:       0,
    srcInfo:      null,
    done:         false,
    totalElapsed: null,
    errorMsg:     null,
    download:     { active: false, done: false, pct: 0, sizeMb: null },
    ffmpeg: {
      mode:    'single',
      current: null,
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
    parallelBatch:   [],
    currentEncoding: null,
  };
}

// ─────────────────────────────────────────────────────────────────
// AXIOM POLLER — Time-cursor based pagination
// 3000 row limit ka problem solve: har 8s mein last seen time ke
// baad ke naye logs laata hai. Koi limit nahi!
// ─────────────────────────────────────────────────────────────────
function startPolling(batch) {
  if (batch.pollTimer) return;
  // Pehle immediately fetch karo, fir har 8s mein
  pollAxiom(batch);
  batch.pollTimer = setInterval(() => pollAxiom(batch), POLL_INTERVAL);
}

function stopPolling(batch) {
  if (batch.pollTimer) { clearInterval(batch.pollTimer); batch.pollTimer = null; }
}

async function pollAxiom(batch) {
  if (!AXIOM_TOKEN) {
    console.warn('⚠️ [AGG] AXIOM_TOKEN not set!');
    return;
  }

  try {
    // Time window: lastSeenTs se aaj tak
    // Pehli baar: last 4 ghante (job itna lamba nahi hoga)
    const endTime   = new Date();
    const startTime = batch.lastSeenTs
      ? new Date(batch.lastSeenTs)         // cursor — jahan se chhoda tha
      : new Date(endTime - 4 * 60 * 60 * 1000); // pehli baar: 4 hours back

    // Axiom APL — batch_id se filter, time sort, page size
    const apl = `['${AXIOM_DATASET}'] | where data.batch_id == "${batch.id}" | sort by _time asc | limit ${AXIOM_PAGE}`;

    const res = await fetch(AXIOM_APL_URL, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${AXIOM_TOKEN}`,
        'Content-Type':  'application/json',
      },
      body: JSON.stringify({
        apl,
        startTime: startTime.toISOString(),
        endTime:   endTime.toISOString(),
      }),
      signal: AbortSignal.timeout(10000),
    });

    if (res.status === 422) return; // Axiom ka "no data" response — normal hai
    if (!res.ok) {
      console.warn(`⚠️ [AGG] Axiom error: ${res.status}`);
      return;
    }

    const data    = await res.json();
    const matches = data.matches || data.data || data.results || [];

    if (matches.length === 0) return;

    // Naye logs process karo
    let newCount = 0;
    let latestTs = batch.lastSeenTs ? new Date(batch.lastSeenTs).getTime() : 0;

    matches.forEach(log => {
      // Raw message nikaalo (same as original HTML logic)
      let raw = log.data || log;
      let msg = '';
      if (typeof raw === 'string')      msg = raw;
      else if (raw.message)             msg = raw.message;
      else if (raw.data?.message)       msg = raw.data.message;
      else                              msg = JSON.stringify(raw);

      msg = msg.replace(/^"|"$/g, '').replace(/\\"/g, '"').trim();
      if (!msg) return;
      // Empty placeholder logs skip karo
      if (/^\{"data":\{"batch_id":"[^"]+","message":""\}\}$/.test(msg)) return;

      // Timestamp
      const ts = log._time ? new Date(log._time).getTime() : Date.now();

      // Duplicate check — same message same timestamp
      const key = `${ts}|${msg.substring(0, 80)}`;
      if (batch.seenKeys.has(key)) return;
      batch.seenKeys.add(key);

      // Cursor update karo
      if (ts > latestTs) latestTs = ts;

      // STATE PARSE — server pe, mobile pe nahi!
      parseLine(msg, batch.state);
      batch.logCount++;
      newCount++;

      // Sirf important logs rakho (ffmpeg frame lines nahi)
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

    // Cursor aage badhao — next poll yahan se shuru hoga
    // +1ms add karo taki same log dobara na aaye
    if (latestTs > 0) {
      batch.lastSeenTs = new Date(latestTs + 1).toISOString();
    }

    // Mobile ke liye sirf last 200 logs rakho
    if (batch.recentLogs.length > MAX_KEPT_LOGS) {
      batch.recentLogs = batch.recentLogs.slice(-MAX_KEPT_LOGS);
    }

    if (newCount > 0) {
      batch.updatedAt = new Date().toISOString();
      console.log(`📥 [AGG] Batch ${batch.id}: +${newCount} new logs (total: ${batch.logCount})`);
      // Deadline reset karo — activity aayi hai
      scheduleCleanup(batch);
    }

    // Job done? Polling band karo (5 min baad, agar koi last log aaye)
    if (batch.state.done && !batch.doneTimer) {
      batch.doneTimer = setTimeout(() => {
        stopPolling(batch);
        console.log(`✅ [AGG] Batch ${batch.id} complete — polling stopped.`);
      }, 5 * 60 * 1000);
    }

  } catch (err) {
    console.error(`❌ [AGG] pollAxiom error for ${batch.id}:`, err.message);
  }
}

// ─────────────────────────────────────────────────────────────────
// LOG PARSER (same logic jo pehle mobile HTML mein tha)
// ─────────────────────────────────────────────────────────────────

const PHASES = [
  { id: 'security', pct: 5   },
  { id: 'download', pct: 15  },
  { id: 'analyze',  pct: 22  },
  { id: 'original', pct: 35  },
  { id: 'audio',    pct: 45  },
  { id: 'p240',     pct: 60  },
  { id: 'p480',     pct: 75  },
  { id: 'p1080',    pct: 90  },
  { id: 'done',     pct: 100 },
];
const PHASE_RANGES = [
  [0,5],[5,15],[15,22],[22,35],[35,45],[45,60],[60,75],[75,90],[90,100]
];

function normalizeQ(raw) {
  const r = raw.toLowerCase().trim();
  if (r === 'orig' || r === 'original') return 'orig';
  if (r === '240p'  || r === '240')   return '240';
  if (r === '360p'  || r === '360')   return '360';
  if (r === '480p'  || r === '480')   return '480';
  if (r === '720p'  || r === '720')   return '720';
  if (r === '1080p' || r === '1080')  return '1080';
  if (r.startsWith('audio'))          return r;
  return r;
}

function classify(msg) {
  if (/\[ENCODE:START\]|\[ENCODE:DONE\]|\[ENCODE:SKIP\]/.test(msg))    return 'sys';
  if (/\[UPLOAD:START\]|\[UPLOAD:DONE\]/.test(msg))                     return 'upload';
  if (/\[PARALLEL:START\]|\[PARALLEL:DONE\]/.test(msg))                 return 'sys';
  if (/\[SOURCE:DOWNLOADED\]|\[SOURCE:INFO\]|\[SIZE:EXACT\]/.test(msg)) return 'sys';
  if (/\[AUDIO:START\]|\[AUDIO:DONE\]/.test(msg))                       return 'sys';
  if (/\[TIMING:TOTAL\]/.test(msg))                                      return 'success';
  if (/frame=\s*\d+.*fps=/.test(msg))                                    return 'ffmpeg';
  if (/✅.*\[HF\]|✅.*Uploaded|STD FILE|HUGE FILE/.test(msg))            return 'upload';
  if (/Error|Failed|DENIED|❌/.test(msg))                                 return 'error';
  if (/WARNING|⚠️/.test(msg))                                             return 'warn';
  if (/COMPLETE|🎉|All Tasks Finished|✅/.test(msg))                      return 'success';
  if (/⏭️|SKIP/.test(msg))                                                return 'skip';
  return 'default';
}

function parseLine(msg, state) {
  const mu = msg.toUpperCase();

  const setPhase = (idx) => {
    state.phaseIdx = idx;
    state.phase    = PHASES[idx].id;
    if (state.overallPct < PHASES[idx].pct) state.overallPct = PHASES[idx].pct;
  };

  if (/VERIFYING CREDENTIALS|SECURITY/.test(mu))                       setPhase(0);
  if (/DOWNLOADING SOURCE/.test(mu)) { setPhase(1); state.download.active = true; }
  if (/FFPROBE|ANALYZING SOURCE/.test(mu))                             setPhase(2);
  if (/\[PRIORITY\].*ORIGINAL/.test(mu))                               setPhase(3);
  if (/\[AUDIO\]\s/.test(mu) && !/AUDIO:/.test(mu))                   setPhase(4);
  if (/\[PARALLEL:START\].*BATCH=1/.test(mu))                          setPhase(5);
  if (/\[PARALLEL:START\].*BATCH=2/.test(mu))                          setPhase(6);
  if (/\[ENCODE:START\].*1080/.test(mu))                               setPhase(7);
  if (/COMPLETE|ALL TASKS FINISHED/.test(mu)) {
    setPhase(8); state.done = true;
    Object.keys(state.qualities).forEach(q => {
      if (state.qualities[q].status !== 'skip') state.qualities[q].status = 'done';
    });
  }

  // [SOURCE:DOWNLOADED]
  const sdl = msg.match(/\[SOURCE:DOWNLOADED\]\s+bytes=(\d+)/);
  if (sdl) {
    state.download.active = false; state.download.done = true;
    state.download.pct    = 100;
    state.download.sizeMb = (parseInt(sdl[1]) / 1048576).toFixed(1);
  }

  // [SOURCE:INFO]
  const si = msg.match(/\[SOURCE:INFO\]\s+height=(\d+)\s+duration=(\d+)\s+audio_tracks=(\d+)/);
  if (si) {
    state.srcDur = parseInt(si[2]);
    state.srcInfo = { height: si[1], duration: si[2], audioTracks: si[3] };
    setPhase(2);
  }

  // [ENCODE:START]
  const es = msg.match(/\[ENCODE:START\]\s+quality=(\S+)/);
  if (es) {
    const q = normalizeQ(es[1]);
    state.currentEncoding = q;
    if (!state.parallelBatch.includes(q) && state.qualities[q])
      state.qualities[q].status = 'running';
    if (/ORIG/.test(es[1].toUpperCase())) setPhase(3);
  }

  // [ENCODE:DONE]
  const ed = msg.match(/\[ENCODE:DONE\]\s+quality=(\S+)(?:\s+elapsed=(\S+))?/);
  if (ed) {
    const q = normalizeQ(ed[1]);
    if (state.qualities[q] && ed[2]) state.qualities[q].elapsed = ed[2];
    if (state.currentEncoding === q && !state.parallelBatch.includes(q))
      state.currentEncoding = null;
  }

  // [ENCODE:SKIP]
  const esk = msg.match(/\[ENCODE:SKIP\]\s+quality=(\S+)/);
  if (esk) {
    const q = normalizeQ(esk[1]);
    if (state.qualities[q]) state.qualities[q].status = 'skip';
  }

  // [SIZE:EXACT]
  const se = msg.match(/\[SIZE:EXACT\]\s+quality=(\S+)\s+bytes=\d+\s+mb=(\d+)/);
  if (se) {
    const q = normalizeQ(se[1]);
    if (state.qualities[q]) state.qualities[q].sizeMb = se[2];
  }

  // [UPLOAD:START]
  const us = msg.match(/\[UPLOAD:START\]\s+quality=(\S+)\s+file=(\S+)(?:\s+mb=(\d+))?(?:\s+dest=(\S+))?/);
  if (us) {
    state.upload = {
      quality: normalizeQ(us[1]), file: us[2],
      sizeMb: us[3] || null, pct: 0,
      speed: null, transferred: null, dest: us[4] || 'hf',
    };
  }

  // HuggingFace tqdm upload progress
  const hfu = msg.match(/^(\S+\.\w+):\s+(\d+)%\|[^|]*\|\s*([\d.]+\w+)\s*\/\s*([\d.]+\w+)(?:,\s*([\d.]+\w+\/s))?/);
  if (hfu && state.upload.quality) {
    state.upload.pct         = parseInt(hfu[2]);
    state.upload.transferred = `${hfu[3]} / ${hfu[4]}`;
    state.upload.speed       = hfu[5] || null;
  }

  // [UPLOAD:DONE]
  const ud = msg.match(/\[UPLOAD:DONE\]\s+quality=(\S+)/);
  if (ud) {
    const q = normalizeQ(ud[1]);
    if (!q.startsWith('audio') && state.qualities[q]) state.qualities[q].status = 'done';
    state.upload = { quality: null, file: null, sizeMb: null, pct: 0, speed: null, transferred: null, dest: null };
  }

  // [PARALLEL:START]
  const ps = msg.match(/\[PARALLEL:START\]\s+batch=(\d+)\s+qualities=(\S+)/i);
  if (ps) {
    const quals = ps[2].split(',').map(normalizeQ);
    state.parallelBatch = quals;
    state.ffmpeg.mode   = 'parallel';
    state.ffmpeg.parallel.a = { quality: quals[0] || null, fps: null, speed: null, time: null, pct: 0 };
    state.ffmpeg.parallel.b = { quality: quals[1] || null, fps: null, speed: null, time: null, pct: 0 };
    quals.forEach(q => { if (state.qualities[q]) state.qualities[q].status = 'parallel'; });
  }

  // [PARALLEL:DONE]
  if (/\[PARALLEL:DONE\]/.test(msg)) {
    state.parallelBatch = []; state.ffmpeg.mode = 'single'; state.currentEncoding = null;
  }

  // [AUDIO:START / DONE]
  const as_ = msg.match(/\[AUDIO:START\]\s+track=(\d+)\s+lang=(\S+)/);
  if (as_) state.currentEncoding = `audio${as_[1]}`;
  if (/\[AUDIO:DONE\]/.test(msg)) state.currentEncoding = null;

  // [TIMING:TOTAL]
  const tt = msg.match(/\[TIMING:TOTAL\]\s+elapsed=(\S+)/);
  if (tt) { state.totalElapsed = tt[1]; state.done = true; state.overallPct = 100; }

  // Curl download %
  if (state.download.active && !state.download.done) {
    const cm = msg.match(/(\d+\.?\d*)\s*%\s*$/);
    if (cm) state.download.pct = parseFloat(cm[1]);
  }

  // FFmpeg frame lines: "[FF:240] frame= 142 fps= 38 ..."
  const ffMatch = msg.match(/^\[FF:([^\]]+)\]\s*(.*)/s);
  const ffTag   = ffMatch ? ffMatch[1].trim() : null;
  const ffBody  = ffMatch ? ffMatch[2] : msg;

  if (/frame=\s*\d+/.test(ffBody)) {
    const g = re => (ffBody.match(re) || [])[1];
    const fps   = g(/fps=\s*([\d.]+)/);
    const speed = g(/speed=\s*(\S+)/);
    const frame = g(/frame=\s*(\d+)/);
    const q_val = g(/q=([\d.-]+)/);
    const tstr  = g(/time=\s*([\d:.]+)/);
    const tagQ  = ffTag ? normalizeQ(ffTag) : null;

    let ffPct = 0;
    if (tstr && state.srcDur > 0) {
      const p = tstr.split(':').map(parseFloat);
      ffPct   = Math.min(100, ((p[0]*3600 + p[1]*60 + p[2]) / state.srcDur) * 100);
    }

    if (state.ffmpeg.mode === 'parallel' && tagQ) {
      const side = tagQ === state.ffmpeg.parallel.a.quality ? 'a'
                 : tagQ === state.ffmpeg.parallel.b.quality ? 'b' : null;
      if (side) {
        const panel = state.ffmpeg.parallel[side];
        if (fps)   panel.fps   = fps;
        if (speed) panel.speed = speed;
        if (tstr)  { panel.time = tstr; panel.pct = ffPct.toFixed(1); }
        if (state.qualities[tagQ]) state.qualities[tagQ].ffPct = parseFloat(ffPct.toFixed(1));
      }
    } else {
      if (fps)   state.ffmpeg.fps   = fps;
      if (speed) state.ffmpeg.speed = speed;
      if (frame) state.ffmpeg.frame = frame;
      if (q_val) state.ffmpeg.q     = q_val;
      if (tstr)  { state.ffmpeg.time = tstr; state.ffmpeg.pct = parseFloat(ffPct.toFixed(1)); }
      state.ffmpeg.current = tagQ || state.currentEncoding;
      if (state.phaseIdx >= 0 && state.srcDur > 0 && ffPct > 0) {
        const [base, end] = PHASE_RANGES[state.phaseIdx] || [state.overallPct, state.overallPct];
        state.overallPct  = parseFloat((base + (end - base) * (ffPct / 100)).toFixed(1));
      }
      if (tagQ && state.qualities[tagQ])
        state.qualities[tagQ].ffPct = parseFloat(ffPct.toFixed(1));
    }
  }

  if (/ACCESS DENIED|❌/.test(msg) && !state.errorMsg)
    state.errorMsg = msg.substring(0, 250);
}

// ─────────────────────────────────────────────────────────────────
// ROUTES
// ─────────────────────────────────────────────────────────────────

/**
 * GET /status/:batch_id
 * Mobile yahan se compact JSON leta hai.
 * Agar pehli baar call ho raha hai → watch automatically start ho jaata hai
 */
app.get('/status/:batch_id', (req, res) => {
  const batch = getOrCreate(req.params.batch_id); // auto-start polling!
  res.json({
    found:     true,
    batch_id:  batch.id,
    logCount:  batch.logCount,
    startedAt: batch.startedAt,
    updatedAt: batch.updatedAt,
    state:     batch.state,
    logs:      batch.recentLogs,
  });
});

/** GET /health */
app.get('/health', (req, res) => {
  res.json({
    status:        'ok',
    activeBatches: store.size,
    uptime:        process.uptime(),
    axiomDataset:  AXIOM_DATASET,
    axiomReady:    !!AXIOM_TOKEN,
  });
});

/** GET /batches — active batches list */
app.get('/batches', (req, res) => {
  const list = [];
  store.forEach((b, id) => {
    list.push({ id, logCount: b.logCount, phase: b.state.phase, done: b.state.done, updatedAt: b.updatedAt });
  });
  res.json(list);
});

/** DELETE /clear/:batch_id — manual cleanup */
app.delete('/clear/:batch_id', (req, res) => {
  const batch = store.get(req.params.batch_id);
  if (batch) { stopPolling(batch); if (batch.cleanupTimer) clearTimeout(batch.cleanupTimer); }
  store.delete(req.params.batch_id);
  res.json({ ok: true, deleted: req.params.batch_id });
});

app.listen(PORT, () => {
  console.log(`🚀 [AGG] Running on port ${PORT}`);
  console.log(`📡 [AGG] Axiom dataset: ${AXIOM_DATASET} | Token: ${AXIOM_TOKEN ? 'SET ✅' : 'MISSING ❌'}`);
});
