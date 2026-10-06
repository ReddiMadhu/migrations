import { canonicalJson, snapshotJson } from './dist/src/core/json.js';
import { sha256Json } from './dist/src/core/hash.js';

const corpus = [
  { name: 'key-order', value: { z: 1, a: 2, m: 3 } },
  { name: 'non-ascii', value: { 'naïve': 'café', '日本': '語' } },
  { name: 'whole-float', value: { amount: 1.0, count: 2.0 } },
  { name: 'int', value: { amount: 1, count: 2 } },
  { name: 'negative-zero', value: { z: -0.0 } },
  { name: 'frac', value: { amount: 1.5 } },
  { name: 'nested-array', value: { rows: [[1, 2], [3, 4]], meta: { b: 1, a: 2 } } },
  { name: 'bool-null', value: { ok: true, none: null, no: false } },
];

const out = {};
for (const c of corpus) {
  out[c.name] = { canonical: canonicalJson(snapshotJson(c.value)), sha256: sha256Json(snapshotJson(c.value)) };
}
console.log(JSON.stringify(out, null, 2));