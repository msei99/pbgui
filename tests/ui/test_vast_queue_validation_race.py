"""Queue validation must not be cancelled by a later background draft check."""
from pathlib import Path
import subprocess


def test_queue_validation_survives_background_generation_without_repainting():
    """A validated snapshot can queue; stale UI results stay suppressed and errors survive."""
    source = Path('frontend/js/vast.js').read_text()
    start = source.index('  async function validateEditorConfig(')
    end = source.index('\n  function scheduleValidation', start)
    code = r'''
const assert=require('assert');
let validationGeneration=0,validationTimer=null,cloudMetrics=null,disposed=false,resolve,reject,paint=0,sizingSubmitted=false;
const refreshGpuRecommendation=async()=>{};
const manualGpuErrors=()=>[];
const clearTimeout=()=>{},setQueueBlocked=()=>{},showValidation=()=>paint++;
const request=()=>new Promise((a,b)=>{resolve=a;reject=b});
(async()=>{
 const queued=validateEditorConfig({name:'captured'},{forQueue:true});
 validationGeneration++;
 resolve({valid:true,errors:[]});
 assert.equal(await queued,true);assert.equal(paint,1);
 const stale=validateEditorConfig({name:'background'});
 validationGeneration++;resolve({valid:true,errors:[]});assert.equal(await stale,false);
 const invalid=validateEditorConfig({},{forQueue:true});
 validationGeneration++;resolve({valid:false,errors:[]});assert.equal(await invalid,false);
 const failed=validateEditorConfig({},{forQueue:true});reject(Error('Service unavailable'));
 await assert.rejects(failed,/Service unavailable/);
})();
'''
    result = subprocess.run(['node', '-e', source[start:end] + code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
