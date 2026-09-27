/* Offline keyboard regression for the DB Tools confirmation dialog. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'frontend', 'db_tools.html'), 'utf8');
const start = source.indexOf('    function confirmModal(');
const end = source.indexOf('    function postJson(', start);
assert.ok(start > 0 && end > start);

function element() {
    const classes = new Set();
    return {
        textContent: '',
        onclick: null,
        attributes: {},
        classList: {
            add(name) { classes.add(name); },
            remove(name) { classes.delete(name); },
            contains(name) { return classes.has(name); },
        },
        setAttribute(name, value) { this.attributes[name] = value; },
        focus() { this.focused = true; },
    };
}
const nodes = Object.fromEntries(
    ['confirm-ovl', 'confirm-head', 'confirm-msg', 'confirm-detail', 'confirm-ok', 'confirm-cancel']
        .map(id => [id, element()])
);
const listeners = new Set();
const document = {
    addEventListener(name, listener) { assert.equal(name, 'keydown'); listeners.add(listener); },
    removeEventListener(name, listener) { assert.equal(name, 'keydown'); listeners.delete(listener); },
};
const context = vm.createContext({document, el: id => nodes[id], Promise});
vm.runInContext(source.slice(start, end), context);

(async () => {
    const cancelled = vm.runInContext("confirmModal('Delete', 'Delete?', '', true)", context);
    assert.equal(listeners.size, 1);
    assert.equal(nodes['confirm-ovl'].classList.contains('visible'), true);
    assert.equal(nodes['confirm-ovl'].onclick, null);
    let prevented = false;
    for (const listener of [...listeners]) {
        listener({key: 'Escape', preventDefault() { prevented = true; }});
    }
    assert.equal(await cancelled, false);
    assert.equal(prevented, true);
    assert.equal(listeners.size, 0);
    assert.equal(nodes['confirm-ovl'].classList.contains('visible'), false);

    const confirmed = vm.runInContext("confirmModal('Copy', 'Copy?', '', false)", context);
    for (const listener of [...listeners]) listener({key: 'Enter', preventDefault() {}});
    assert.equal(nodes['confirm-ovl'].classList.contains('visible'), true);
    nodes['confirm-ok'].onclick();
    assert.equal(await confirmed, true);
    assert.equal(listeners.size, 0);
})().catch(error => { console.error(error); process.exitCode = 1; });
