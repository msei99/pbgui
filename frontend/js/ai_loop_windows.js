/* Inclusive UTC date arithmetic shared by the Loop Template and offline checks. */
(function (root) {
  'use strict';
  const DAY=86400000;
  function day(value){
    const stamp=Date.parse(value+'T00:00:00Z');
    if(!/^\d{4}-\d{2}-\d{2}$/.test(value)||!Number.isFinite(stamp)||new Date(stamp).toISOString().slice(0,10)!==value)throw new Error('Use valid UTC calendar dates.');
    return stamp/DAY;
  }
  const iso=value=>new Date(value*DAY).toISOString().slice(0,10);
  function validate(plan){
    const start=day(plan.start_date),end=day(plan.end_date),n=plan.training_windows;
    if(start>end||!Number.isInteger(n)||n<1||n>48||typeof plan.use_holdout!=='boolean')throw new Error('Use 1–48 training windows and ordered base dates.');
    const rows=plan.windows||[],training=rows.filter(w=>w.role==='training'),holdouts=rows.filter(w=>w.role==='holdout');
    if(training.length!==n||holdouts.length!==Number(plan.use_holdout)||rows.length!==n+Number(plan.use_holdout))throw new Error('Window roles and counts must match the holdout choice.');
    const ordered=rows.slice().sort((a,b)=>a.start_date.localeCompare(b.start_date));
    ordered.forEach((w,i)=>{
      const a=day(w.start_date),b=day(w.end_date);
      if(a<start||b>end||b-a<1)throw new Error('Each window needs at least two calendar days inside the selected period.');
      if(i&&ordered[i-1].end_date>=w.start_date)throw new Error('Windows must not overlap, including their boundary dates.');
    });
    if(plan.use_holdout&&ordered[ordered.length-1].role!=='holdout')throw new Error('Holdout must follow all training windows.');
    return plan;
  }
  function generate(start,end,n,enabled){
    const first=day(start),days=day(end)-first+1,k=n+Number(enabled);
    if(!Number.isInteger(n)||n<1||n>48||typeof enabled!=='boolean'||days<k*2)throw new Error('Each window needs at least two calendar days; reduce the count or extend the date range.');
    return validate({version:1,start_date:start,end_date:end,training_windows:n,use_holdout:enabled,windows:Array.from({length:k},(_,i)=>({
      id:'window_'+(i+1),label:i<n?'train_'+String(i+1).padStart(2,'0'):'holdout_01',role:i<n?'training':'holdout',
      start_date:iso(first+Math.floor(i*days/k)),end_date:iso(first+Math.floor((i+1)*days/k)-1),scenario:{}
    }))});
  }
  root.PBGuiAILoopWindows={generate,validate};
}(typeof window==='undefined'?globalThis:window));
