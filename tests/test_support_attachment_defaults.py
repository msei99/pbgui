"""Exercise real attachment-default and persistence code without runtime data or a browser."""
import json
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('area,saved,required,excluded,count', [
    (None, None, ['browser','services','ai','backtests','optimize','run','market','vps','vast','databases','other','coverage'], [], 12),
    ('unrecognized_screen', None, ['other','coverage','databases','ai'], [], 12),
    ('vps_manager', None, ['browser','services','vps','coverage'], ['ai','optimize','vast'], 4),
    ('v8_optimize_ai_loop', None, ['ai','optimize','backtests','vast','services','coverage'], ['vps','databases'], 7),
    ('v8_optimize', None, ['optimize','backtests','market','coverage'], ['ai','vast'], 6),
    (None, ['browser','services'], ['ai','vps','databases','coverage'], [], 12),
    (None, {'version':2,'groups':['browser']}, ['browser'], ['services','ai','coverage'], 1),
    ('vps_manager', {'version':2,'groups':[]}, [], ['browser','services','vps'], 0),
])
def test_defaults_and_user_choices(area, saved, required, excluded, count):
    """Manual/unknown scopes are complete; known errors include dependencies and choices survive."""
    script = r'''
      const fs = require('fs');
      const fixture = JSON.parse(fs.readFileSync(0, 'utf8'));
      global.window = {};
      const store = new Map();
      if (fixture.saved !== null) store.set('fixture', JSON.stringify(fixture.saved));
      global.sessionStorage = {getItem:key=>store.get(key)||null, setItem:(key,value)=>store.set(key,value)};
      eval(fs.readFileSync(process.argv[1], 'utf8'));
      const model = window.PBGuiSupportAttachments.create(()=>{}, {storageKey:'fixture'});
      const report = {format:'pbgui-support-debug-v1',system:{version:'fixture'},records:{},logs:{},coverage:{}};
      if (fixture.area) report.error_context = {area:fixture.area};
      model.setReport(report);
      const groups = model.getData().coverage.attachment_selection.groups;
      const restored = window.PBGuiSupportAttachments.create(()=>{}, {storageKey:'fixture'});
      restored.setReport(report);
      console.log(JSON.stringify({groups, restored:restored.getData().coverage.attachment_selection.groups}));
    '''
    result = subprocess.run(['node','-e',script,str(ROOT/'frontend/js/support_attachments.js')],
        input=json.dumps({'area':area,'saved':saved}), text=True, capture_output=True, timeout=10, check=True)
    value = json.loads(result.stdout)
    assert len(value['groups']) == count
    assert set(required).issubset(value['groups'])
    assert not set(excluded).intersection(value['groups'])
    assert value['restored'] == value['groups']
