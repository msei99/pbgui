/* Offline regression for Vast jobs polling when the browser tab is hidden. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'frontend', 'js', 'vast.js'), 'utf8');
const start = source.indexOf('  async function refreshJobs(preferred)');
const end = source.indexOf('  let validationGeneration', start);
const listenerStart = source.indexOf("  document.addEventListener('visibilitychange'");
const listenerEnd = source.indexOf("\n  window.addEventListener('pagehide'", listenerStart);
assert.ok(start > 0 && end > start && listenerStart > 0 && listenerEnd > listenerStart);
const pollCode = source.slice(start, end);
const visibilityCode = source.slice(listenerStart, listenerEnd);

const timers = [];
const listeners = {};
let requests = 0;
let deferNext = null;
const document = {
    hidden: false,
    addEventListener(name, listener) { listeners[name] = listener; },
};
const context = vm.createContext({
    document,
    window: {},
    jobTimer: null,
    jobGeneration: 0,
    disposed: false,
    jobRows: [],
    workers: [],
    worker: null,
    queueState: {},
    supervision: false,
    calibrationWorkerAvailable: false,
    selectedJobId: null,
    stoppingJobs: new Map(),
    clearSecrets() {},
    el() { return null; },
    message() {},
    renderJob() {},
    renderQueueOverview() {},
    refreshActiveGpuProfile() {},
    setTimeout(callback, delay) {
        const timer = {callback, delay, cleared: false};
        timers.push(timer);
        return timer;
    },
    clearTimeout(timer) { if (timer) timer.cleared = true; },
    request() {
        requests++;
        if (deferNext) {
            const deferred = deferNext;
            deferNext = null;
            return deferred.promise;
        }
        return Promise.resolve({jobs: [], workers: [], queue: {}, supervision_available: false});
    },
});
vm.runInContext(pollCode + '\n' + visibilityCode, context);
const activeTimers = () => timers.filter(timer => !timer.cleared);

(async () => {
    await vm.runInContext('pollJobs()', context);
    assert.equal(requests, 1);
    assert.equal(activeTimers().length, 1);
    assert.equal(activeTimers()[0].delay, 10000);

    document.hidden = true;
    listeners.visibilitychange();
    assert.equal(activeTimers().length, 0);
    await vm.runInContext('pollJobs()', context);
    await vm.runInContext('refreshJobs()', context);
    assert.equal(requests, 1);

    document.hidden = false;
    listeners.visibilitychange();
    await new Promise(setImmediate);
    assert.equal(requests, 2);
    assert.equal(activeTimers().length, 1);

    await vm.runInContext('refreshJobs()', context);
    assert.equal(requests, 3);
    assert.equal(activeTimers().length, 1);

    let resolve;
    deferNext = {promise: new Promise(done => { resolve = done; })};
    const pending = vm.runInContext('refreshJobs()', context);
    assert.equal(requests, 4);
    document.hidden = true;
    listeners.visibilitychange();
    resolve({jobs: [], workers: [], queue: {}, supervision_available: false});
    await pending;
    assert.equal(activeTimers().length, 0);

    document.hidden = false;
    listeners.visibilitychange();
    await new Promise(setImmediate);
    assert.equal(requests, 5);
    assert.equal(activeTimers().length, 1);
})().catch(error => { console.error(error); process.exitCode = 1; });
