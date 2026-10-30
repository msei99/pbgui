/* Explicit deterministic timer fixture, executed solely inside the test browser. */
(() => {
  if(window.__pbguiClock)return;
  const RealDate=window.__pbguiNativeDate||Date,realNow=RealDate.now.bind(RealDate),realInterval=setInterval.bind(window),timers=new Map();
  let now=0,epoch=Date.now(),fixed=null,sequence=0,paused=false,busy=false,lastReal=performance.now();
  const schedule=(fn,delay,args,repeat)=>{const id=++sequence;
    timers.set(id,{id,fn,at:now+Math.max(1,Number(delay)||0),delay:Math.max(1,Number(delay)||0),args,repeat});return id;};
  window.setTimeout=(fn,delay,...args)=>schedule(fn,delay,args,false);
  window.setInterval=(fn,delay,...args)=>schedule(fn,delay,args,true);
  window.clearTimeout=window.clearInterval=id=>timers.delete(id);
  window.requestAnimationFrame=fn=>schedule(()=>fn(now),16,[],false);
  window.cancelAnimationFrame=id=>timers.delete(id);
  class TestDate extends RealDate {constructor(...args){super(...(args.length?args:[fixed??epoch+now]));}static now(){return fixed??epoch+now;}}
  window.Date=TestDate;
  window.__pbguiClock={
    setTime(value){fixed=null;epoch=(typeof value==='number'?value:RealDate.parse(value))-now;},
    setFixed(value){fixed=typeof value==='number'?value:RealDate.parse(value);},
    pause(value){paused=true;this.setTime(value);},
    async advance(ms,skip){
      const end=now+ms,seen=new Set();let iterations=0;
      while(true){const next=Array.from(timers.values()).filter(t=>t.at<=end&&(!skip||!seen.has(t.id)))
          .sort((a,b)=>a.at-b.at||a.id-b.id)[0];if(!next)break;
        if(++iterations>100000)throw Error('Synthetic timer queue did not converge');
        now=skip?end:next.at;seen.add(next.id);
        if(next.repeat)next.at=now+next.delay;else timers.delete(next.id);
        if(typeof next.fn==='function')next.fn(...next.args);else (0,eval)(next.fn);
        await Promise.resolve();await Promise.resolve();
      }now=end;lastReal=performance.now();await Promise.resolve();
    }
  };
  // Installation follows wall time until a test explicitly pauses the clock.
  realInterval(async()=>{if(paused||busy)return;busy=true;const elapsed=performance.now()-lastReal;
    try{await window.__pbguiClock.advance(elapsed,false);}finally{busy=false;}},10);
})();
