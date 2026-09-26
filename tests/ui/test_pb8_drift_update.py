"""Executable editor regression for PB8's independent drift controls."""

from test_remaining_issue_regressions import execute, function


def test_drift_settings_round_trip_null_zero_validation_and_older_runtime():
    """New fields preserve inheritance and exact zero without leaking into old configs."""
    execute(''.join(function('v7_optimize.html', name) for name in (
        'collectOptimizeGpuSettings', 'populateOptimizeGpuSettings', 'resetOptimizeGpuSettings',
        'parseOptimizeGpuFractions',
    )) + r'''
const assert=require('node:assert/strict');
const optimizeEditorAdapter={isV8:true};
let defaults={drift_halt:0.6,drift_rank_halt:null,drift_objective_tolerance:0.000001};
const optimizeGpuDefaults=()=>defaults,deepClone=structuredClone;
const nodes=new Map();const el=id=>{if(!nodes.has(id))nodes.set(id,{value:'',checked:false});return nodes.get(id);};
const toNullableNumber=v=>v===''?null:Number(v),toNumberOr=(v,d)=>v===''?d:Number(v);
const scheduleStructuredEditorSync=()=>{};
const config={gpu:{...defaults,drift_rank_halt:0.8,drift_objective_tolerance:0,future:42}};
const updateOptimizeGpuHalvingFields=()=>{};
populateOptimizeGpuSettings(config);collectOptimizeGpuSettings(config,true);
assert.equal(config.gpu.drift_rank_halt,0.8);assert.equal(config.gpu.drift_objective_tolerance,0);assert.equal(config.gpu.future,42);
el('opted-gpu-drift-rank-halt').value='';collectOptimizeGpuSettings(config,true);assert.equal(config.gpu.drift_rank_halt,null);
for(const invalid of ['0','1.1','NaN','Infinity']) {el('opted-gpu-drift-rank-halt').value=invalid;assert.throws(()=>collectOptimizeGpuSettings(config,true),/drift_rank_halt/);}
el('opted-gpu-drift-rank-halt').value='';
for(const invalid of ['-1','NaN','Infinity']) {el('opted-gpu-drift-objective-tolerance').value=invalid;assert.throws(()=>collectOptimizeGpuSettings(config,true),/drift_objective_tolerance/);}
resetOptimizeGpuSettings();collectOptimizeGpuSettings(config,true);assert.equal(config.gpu.drift_objective_tolerance,0.000001);assert.equal(config.gpu.drift_rank_halt,null);
defaults={drift_halt:0.6};const old={gpu:{drift_halt:0.6}};collectOptimizeGpuSettings(old,true);
assert(!Object.hasOwn(old.gpu,'drift_rank_halt'));assert(!Object.hasOwn(old.gpu,'drift_objective_tolerance'));
''')
