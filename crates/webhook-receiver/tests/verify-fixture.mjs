// Public fixture only. Uses the same Node crypto byte sequence as TS signing.ts.
import { createHmac } from 'node:crypto';
import { readFileSync } from 'node:fs';
import assert from 'node:assert/strict';
const fixture = JSON.parse(readFileSync(new URL('./fixture.json', import.meta.url)));
const mac = createHmac('sha256', Buffer.from('PUBLIC-TEST-FIXTURE-NOT-A-SECRET'))
  .update(`${fixture.timestamp}\nevt_1\n`).update(Buffer.from(fixture.body)).digest('hex');
assert.equal(fixture.signature, `v1;kid=fixture;mac=${mac}`);
console.log('Node crypto Lenso v1 fixture verified');
