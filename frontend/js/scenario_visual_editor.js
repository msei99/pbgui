/* Shared-time-axis scenario editing. Only explicit Apply changes optimizer scenarios. */
(function () {
  'use strict';
  const DAY = 86400000, copy = value => JSON.parse(JSON.stringify(value));
  const day = value => Date.parse(value + 'T00:00:00Z') / DAY;
  const iso = value => new Date(Math.round(value) * DAY).toISOString().slice(0, 10);
  let owner = null, reference = null, magnetEnabled = true;
  const uid = () => 'window_' + Array.from(crypto.getRandomValues(new Uint8Array(16)),n=>n.toString(16).padStart(2,'0')).join('');
  function element(tag, text, parent) {
    const node = document.createElement(tag);
    if (text != null) node.textContent = text;
    if (parent) parent.appendChild(node);
    return node;
  }
  function mount(host, options) {
    if (owner) owner.dispose();
    if (!host) return;
    let windows = copy(options.windows || []), selected = windows[0]?.id, undo = [], redo = [];
    const referenceKey=JSON.stringify([options.context.exchanges||[],options.context.coins||[]]);
    let chart = null, sourceRows = [], chartController = null, generation = 0, disposed = false;
    let viewStart = day(options.context.start_date), viewEnd = day(options.context.end_date) + 1;
    const minimum = viewStart, maximum = viewEnd;
    if (!Number.isFinite(minimum) || !Number.isFinite(maximum) || minimum >= maximum) {
      element('p', 'Set valid base dates to open the visual editor.', host); return;
    }
    const events = new AbortController();
    const on = (node, event, fn) => node.addEventListener(event, fn, {signal: events.signal});
    owner = {dispose() { disposed = true; events.abort(); chartController?.abort(); }};
    on(window,'pagehide',()=>owner?.dispose());
    host.replaceChildren();
    host.classList.add('scenario-visual');
    function field(control, caption) {
      const group=element('div',null,control.parentNode);group.className='form-group';
      const label=element('label',caption,group);control.id=uid();label.htmlFor=control.id;
      group.appendChild(control);
    }
    const top = element('div', null, host); top.className = 'scenario-toolbar';
    const exchange = element('select', null, top); exchange.setAttribute('aria-label', 'Reference exchange');
    (options.context.exchanges || []).forEach(ex => { const node = element('option', ex, exchange); node.value = ex; });
    if(reference?.key===referenceKey && (options.context.exchanges||[]).includes(reference.exchange))exchange.value=reference.exchange;
    const source = element('select', null, top); source.setAttribute('aria-label', 'Reference coin and dataset');
    field(exchange,'Reference exchange');field(source,'Reference coin / dataset');
    const feedback = element('div', 'Reference chart: choose local candles. Optimizer coins/exchanges are unchanged.', host);
    feedback.className = 'scenario-feedback'; feedback.setAttribute('aria-live', 'polite');
    const tools = element('div', null, host); tools.className = 'scenario-toolbar';
    const button = (caption, fn, parent=tools) => { const node=element('button', caption, parent); node.type='button'; node.className='act-btn'; on(node,'click',fn); return node; };
    let drawRole = null;
    function iconButton(label,path,action){
      const node=button('',action);node.classList.add('scenario-icon');node.title=label;node.setAttribute('aria-label',label);
      const icon=document.createElementNS('http://www.w3.org/2000/svg','svg');
      icon.setAttribute('viewBox','0 0 24 24');icon.setAttribute('aria-hidden','true');
      const stroke=document.createElementNS(icon.namespaceURI,'path');stroke.setAttribute('d',path);icon.appendChild(stroke);node.appendChild(icon);return node;
    }
    const undoButton=iconButton('Undo','M9 5 4 10l5 5 M4 10h10a6 6 0 0 1 0 12',()=>history(false));
    const redoButton=iconButton('Redo','m15 5 5 5-5 5 M20 10H10a6 6 0 0 0 0 12',()=>history(true));
    const trash=iconButton('Delete selected window','M4 6h16 M9 6V3h6v3 M6 6l1 15h10l1-15 M10 10v7 M14 10v7',removeSelected);
    trash.classList.add('scenario-trash');trash.title='Delete selected window or drop a window here';
    iconButton('Full range','M8 3H3v5 M16 3h5v5 M3 16v5h5 M21 16v5h-5 M3 3l6 6 M21 3l-6 6 M3 21l6-6 M21 21l-6-6',()=>{viewStart=minimum;viewEnd=maximum;render();});
    const magnet=iconButton('Snap windows','M5 3v9a7 7 0 0 0 14 0V3h-5v9a2 2 0 0 1-4 0V3z M5 7h5 M14 7h5',()=>{
      magnetEnabled=!magnetEnabled;magnet.setAttribute('aria-pressed',String(magnetEnabled));
    });
    magnet.setAttribute('aria-pressed',String(magnetEnabled));
    magnet.title='Snap windows to nearby boundaries (on/off)';
    const ghost=element('div',null,host);ghost.className='scenario-drag-ghost';ghost.hidden=true;
    const canvas = element('div', null, host); canvas.className='scenario-canvas';
    const svg = document.createElementNS('http://www.w3.org/2000/svg','svg'); canvas.appendChild(svg);
    svg.setAttribute('preserveAspectRatio','xMinYMin meet');
    svg.setAttribute('role','group'); svg.setAttribute('aria-label','Price chart and editable training and holdout windows');
    const actions=element('div',null,host);actions.className='scenario-actions';
    const problem=element('div',null,host);problem.className='scenario-feedback';problem.setAttribute('aria-live','polite');
    const applyStatus=element('span','',actions);applyStatus.className='scenario-feedback';applyStatus.setAttribute('role','status');
    let applying=false;
    const apply=button('Check & Apply windows',async()=>{
      if(applying)return;
      if(JSON.stringify(split())!==JSON.stringify(windows)){save();changed();}
      const errors=validate();if(errors.length){problem.textContent=errors.join(' ');return;}
      applying=true;apply.disabled=true;apply.textContent='Checking…';applyStatus.textContent='Validating windows and creating scenarios…';
      try{await options.apply(copy(windows));if(!disposed)applyStatus.textContent='Windows applied.';}
      catch(error){if(!disposed){applyStatus.textContent=error.message||'Could not apply windows.';applyStatus.style.color='var(--red, #ed7777)';problem.textContent=applyStatus.textContent;}}
      finally{applying=false;if(!disposed){apply.disabled=false;apply.textContent='Check & Apply windows';}}

    },actions);
    apply.className='btn primary';
    function windowCaption(w){return w.start_date+' — '+w.end_date+' · '+(day(w.end_date)-day(w.start_date)+1)+' days';}
    function snapWindow(w,part){
      if(!magnetEnabled)return;
      const start=day(w.start_date),end=day(w.end_date)+1;
      const tolerance=8/plotWidth*(viewEnd-viewStart);
      const edges=windows.filter(other=>other.id!==w.id).flatMap(other=>[day(other.start_date),day(other.end_date)+1]);
      const candidates=[];
      edges.forEach(edge=>{
        if(part==='move'){
          [edge-start,edge-end].forEach(shift=>{if(start+shift>=minimum&&end+shift<=maximum)candidates.push(shift);});
        }else if(part==='left'&&edge>=minimum&&edge<end)candidates.push(edge-start);
        else if(part==='right'&&edge>start&&edge<=maximum)candidates.push(edge-end);
      });
      candidates.sort((a,b)=>Math.abs(a)-Math.abs(b));
      const shift=candidates[0];if(shift===undefined||Math.abs(shift)>tolerance)return;
      if(part!=='right')w.start_date=iso(start+shift);
      if(part!=='left')w.end_date=iso(end+shift-1);
    }
    function removeSelected(){
      if(!windows.some(w=>w.id===selected))return;
      save();windows=windows.filter(w=>w.id!==selected);selected=windows[0]?.id;changed();
    }
    function overTrash(event){
      const box=trash.getBoundingClientRect();
      return event.clientX>=box.left&&event.clientX<=box.right&&event.clientY>=box.top&&event.clientY<=box.bottom;
    }
    function connections(){
      const ranges=windows.map(w=>({a:day(w.start_date),b:day(w.end_date)+1})).filter(w=>Number.isFinite(w.a)&&w.b>w.a);
      const edges=[...new Set(ranges.flatMap(w=>[w.a,w.b]))].sort((a,b)=>a-b);
      for(let i=0;i<edges.length-1;i++){
        const a=edges[i],b=edges[i+1],count=ranges.filter(w=>w.a<=a&&w.b>=b).length;
        if(count===1)continue;
        const l=Math.max(a,viewStart),r=Math.min(b,viewEnd);if(l>=r)continue;
        const kind=count?'overlap':'gap';
        const marker=shape('rect',{x:x(l),y:10,width:Math.max(2,x(r)-x(l)),height:300,opacity:.12,fill:count?'#ed7777':'#e6a349','data-connection':kind});
        const title=document.createElementNS(svg.namespaceURI,'title');title.textContent=(count?'Overlap':'Gap')+': '+(b-a)+' days · '+iso(a)+' — '+iso(b-1)+(count?' · '+count+' windows':'');marker.appendChild(title);
      }
      edges.forEach(d=>{
        const before=ranges.filter(w=>w.a<d&&w.b>=d).length,after=ranges.filter(w=>w.a<=d&&w.b>d).length;
        if(before!==1||after!==1||!ranges.some(w=>w.b===d)||!ranges.some(w=>w.a===d)||d<viewStart||d>viewEnd)return;
        const marker=shape('rect',{x:x(d)-.5,y:10,width:1,height:300,fill:'#49c99a','data-connection':'adjacent'});
        const title=document.createElementNS(svg.namespaceURI,'title');title.textContent='Adjacent · '+iso(d-1)+' / '+iso(d)+' · no missing or shared day';marker.appendChild(title);
      });
    }
    function uniqueLabel(base){let label=base,n=2;while(windows.some(w=>w.label===label))label=base+'_'+n++;return label;}
    function save(){undo.push(copy(windows));if(undo.length>50)undo.shift();redo=[];}
    function history(forward){const from=forward?redo:undo,to=forward?undo:redo;if(!from.length)return;to.push(copy(windows));windows=from.pop();selected=windows[0]?.id;options.change(copy(windows));render();}
    function changed(){
      const result=split();
      if(result.length>64||result.filter(w=>w.role==='training').length>48){
        if(undo.length)windows=undo.pop();
        render();problem.textContent='This Holdout would create too many training windows (maximum 48). The change was undone.';return;
      }
      windows=result;
      if(!windows.some(w=>w.id===selected))selected=windows[0]?.id;
      options.change(copy(windows));render();
    }
    function shared(a,b){const x=a.scenario?.exchanges,y=b.scenario?.exchanges;return !x?.length||!y?.length||x.some(ex=>y.includes(ex));}
    function validate(){
      const errors=[],labels=new Set();
      if(!windows.some(w=>w.role==='training'))errors.push('At least one training window is required.');
      windows.forEach((a,i)=>{
        if(!/^[A-Za-z0-9_.-]{1,120}$/.test(a.label)||labels.has(a.label))errors.push('Use unique safe window names.');labels.add(a.label);
        if(!(day(a.start_date)>=minimum&&day(a.end_date)<maximum&&day(a.start_date)<=day(a.end_date)))errors.push(a.label+': invalid dates.');
        windows.slice(i+1).forEach(b=>{if(shared(a,b)&&a.role!==b.role&&a.start_date<=b.end_date&&b.start_date<=a.end_date)errors.push('Training/Holdout overlap: '+a.label+' / '+b.label+'.');});
      });
      return [...new Set(errors)];
    }
    function split(){
      const result=[],usedLabels=new Set(windows.map(w=>w.label));
      windows.forEach(w=>{
        if(w.role!=='training'){result.push(w);return;}
        let parts=[[day(w.start_date),day(w.end_date)]];
        windows.filter(h=>h.role==='holdout'&&shared(w,h)&&Number.isFinite(day(h.start_date))&&Number.isFinite(day(h.end_date))&&day(h.start_date)<=day(h.end_date)).forEach(h=>{
          const a=day(h.start_date),b=day(h.end_date);
          parts=parts.flatMap(([l,r])=>b<l||a>r?[[l,r]]:[...(l<a?[[l,a-1]]:[]),...(r>b?[[b+1,r]]:[])]);
        });
        parts.forEach(([l,r],i)=>{
          let label=w.label;
          if(i){const base=w.label.slice(0,100)+'_part';let n=2;label=base+n;while(usedLabels.has(label))label=base+(++n);usedLabels.add(label);}
          result.push({...copy(w),id:i?uid():w.id,label,start_date:iso(l),end_date:iso(r)});
        });
      });
      return result;
    }
    function shape(tag,attrs,text){const node=document.createElementNS(svg.namespaceURI,tag);Object.entries(attrs).forEach(([key,value])=>node.setAttribute(key,String(value)));if(text!=null)node.textContent=text;svg.appendChild(node);return node;}
    let plotWidth=880;
    const x=d=>100+plotWidth*(d-viewStart)/(viewEnd-viewStart);
    function render(){
      const width=Math.max(650,canvas.clientWidth||1000);plotWidth=width-120;
      svg.replaceChildren();const tracks=[];
      const layoutWindows=drag?.part==='move'?drag.before:windows;
      ['training','holdout'].forEach(role=>{
        const lanes=[];layoutWindows.filter(w=>w.role===role).sort((a,b)=>a.start_date.localeCompare(b.start_date)).forEach(w=>{let lane=lanes.findIndex(last=>last<w.start_date);if(lane<0){lane=lanes.length;lanes.push('');}lanes[lane]=w.end_date;tracks.push({w:windows.find(current=>current.id===w.id)||w,y:345+(role==='holdout'?Math.max(1,trainingLanes)*34+14:0)+lane*34});});
        if(role==='training')trainingLanes=lanes.length;
      });
      const holdoutTop=345+Math.max(1,trainingLanes)*34+14;
      const height=Math.max(holdoutTop+60,...tracks.map(t=>t.y+60));svg.setAttribute('viewBox','0 0 '+width+' '+height);svg.style.height=height+'px';
      shape('rect',{x:100,y:10,width:plotWidth,height:300,fill:'var(--bg2)'});
      for(let i=0;i<=5;i++){const d=viewStart+(viewEnd-viewStart)*i/5;shape('line',{x1:x(d),x2:x(d),y1:10,y2:height-25,stroke:'var(--border)'});shape('text',{x:x(d),y:height-7,'text-anchor':i===5?'end':i===0?'start':'middle',fill:'var(--text-dim)','font-size':11},iso(Math.min(maximum-1,d)));}
      if(chart){
        const data=chart.candles.filter(c=>day(c.date)>=viewStart&&day(c.date)<viewEnd);
        chart.missing_days.filter(d=>day(d)>=viewStart&&day(d)<viewEnd).forEach(d=>shape('rect',{x:x(day(d)),y:302,width:Math.max(1,x(day(d)+1)-x(day(d))),height:8,fill:'rgba(240,160,50,.5)'}));
        if(data.length){const low=Math.min(...data.map(c=>c.low)),high=Math.max(...data.map(c=>c.high)),span=high-low||1, y=p=>295-270*(p-low)/span;
          shape('text',{x:5,y:30,fill:'var(--text-dim)','font-size':12},high.toPrecision(5));shape('text',{x:5,y:295,fill:'var(--text-dim)','font-size':12},low.toPrecision(5));
          if(data.length<1200&&plotWidth/(viewEnd-viewStart)>=5)data.forEach(c=>{const px=x(day(c.date)+.5),color=c.close>=c.open?'#49c99a':'#ed7777';shape('line',{x1:px,x2:px,y1:y(c.high),y2:y(c.low),stroke:color});shape('rect',{x:px-2,y:Math.min(y(c.open),y(c.close)),width:4,height:Math.max(1,Math.abs(y(c.open)-y(c.close))),fill:color});});
          else{let path='',prev=null;data.forEach(c=>{const d=day(c.date);path+=(prev===null||d-prev>1?'M':'L')+x(d+.5)+','+y(c.close)+' ';prev=d;});shape('path',{d:path,fill:'none',stroke:'var(--accent)','stroke-width':2});}
        }
      }else shape('text',{x:120,y:110,fill:'var(--text-dim)','font-size':14},'Load local OHLCV for price context');
      connections();
      trash.disabled=!windows.some(w=>w.id===selected);
      shape('text',{x:4,y:363,fill:'var(--text)','font-size':12},'Training');
      shape('text',{x:4,y:363+Math.max(1,trainingLanes)*34+14,fill:'var(--text)','font-size':12},'Holdout');
      ['training','holdout'].forEach(role=>{
        const top=role==='training'?345:holdoutTop,bottom=role==='training'?holdoutTop-7:height-25;
        shape('rect',{x:100,y:top,width:plotWidth,height:bottom-top,fill:'transparent',stroke:'var(--border)','stroke-dasharray':'3 4','data-lane':role,'pointer-events':'none'});
      });
      tracks.forEach(({w,y})=>{
        if(drag?.part==='move'&&drag.active&&drag.id===w.id)return;
        const a=Math.max(viewStart,day(w.start_date)),b=Math.min(viewEnd,day(w.end_date)+1);if(a>=b)return;
        const attrs={y,height:26,fill:w.role==='training'?'var(--scenario-training)':'var(--scenario-holdout)',stroke:w.id===selected?'var(--accent)':'var(--border)','stroke-width':2};
        const bar=shape('rect',{...attrs,x:x(a),width:Math.max(3,x(b)-x(a)),'data-id':w.id,'data-part':'move',tabindex:0,role:'button','aria-label':w.label+' '+w.start_date+' to '+w.end_date});
        const capacity=Math.max(0,Math.floor((x(b)-x(a)-18)/6));
        if(capacity>3){
          const dates=w.start_date+' — '+w.end_date;
          shape('text',{x:x(a)+9,y:y+10,fill:'var(--text)','font-size':10,'pointer-events':'none'},dates.length>capacity?dates.slice(0,capacity-1)+'…':dates);
          shape('text',{x:x(a)+9,y:y+22,fill:'var(--text-dim)','font-size':10,'pointer-events':'none'},(day(w.end_date)-day(w.start_date)+1)+' days');
        }
        const title=document.createElementNS(svg.namespaceURI,'title');title.textContent=windowCaption(w);bar.appendChild(title);
        if(day(w.start_date)>=viewStart)shape('rect',{x:x(a),y,width:7,height:26,fill:'var(--accent)','data-id':w.id,'data-part':'left'});
        if(day(w.end_date)+1<=viewEnd)shape('rect',{x:x(b)-7,y,width:7,height:26,fill:'var(--accent)','data-id':w.id,'data-part':'right'});
      });
      tracks.forEach(leftTrack=>{
        const boundary=day(leftTrack.w.end_date)+1;
        if(boundary<=viewStart||boundary>=viewEnd)return;
        const following=tracks.filter(t=>day(t.w.start_date)===boundary&&shared(leftTrack.w,t.w));
        const preceding=tracks.filter(t=>day(t.w.end_date)+1===boundary&&shared(leftTrack.w,t.w));
        if(following.length!==1||preceding.length!==1)return;
        const rightTrack=following[0],top=Math.min(leftTrack.y,rightTrack.y),bottom=Math.max(leftTrack.y,rightTrack.y)+26;
        const endGrip=shape('rect',{x:x(boundary)-26,y:leftTrack.y,width:16,height:26,rx:2,fill:'transparent',
          'data-id':leftTrack.w.id,'data-part':'right',role:'button','aria-label':'Resize left window end only'});
        const endTitle=document.createElementNS(svg.namespaceURI,'title');endTitle.textContent='Left grip: resize left window end only';endGrip.appendChild(endTitle);
        const startGrip=shape('rect',{x:x(boundary)+10,y:rightTrack.y,width:16,height:26,rx:2,fill:'transparent',
          'data-id':rightTrack.w.id,'data-part':'left',role:'button','aria-label':'Resize right window start only'});
        const startTitle=document.createElementNS(svg.namespaceURI,'title');startTitle.textContent='Right grip: resize right window start only';startGrip.appendChild(startTitle);
        const handle=shape('rect',{x:x(boundary)-6,y:top,width:12,height:bottom-top,rx:2,fill:'transparent',
          'data-id':leftTrack.w.id,'data-next':rightTrack.w.id,'data-part':'joint',role:'button',
          'aria-label':'Resize adjacent windows together'});
        const title=document.createElementNS(svg.namespaceURI,'title');title.textContent='Drag shared boundary · keep both windows connected';handle.appendChild(title);
        // Slim visible marks preserve captions; transparent rectangles retain larger mouse targets.
        shape('rect',{x:x(boundary)-21,y:leftTrack.y+24,width:6,height:2,fill:'var(--accent)','pointer-events':'none'});
        shape('rect',{x:x(boundary)+15,y:rightTrack.y+24,width:6,height:2,fill:'var(--accent)','pointer-events':'none'});
        shape('line',{x1:x(boundary),x2:x(boundary),y1:top,y2:bottom,stroke:'var(--accent)','stroke-width':2,'pointer-events':'none'});
      });
      const errors=validate();problem.textContent=errors.join(' ');apply.disabled=applying;undoButton.disabled=!undo.length;redoButton.disabled=!redo.length;
    }
    let trainingLanes=0, drag=null;
    const pointerDay=event=>{const rect=svg.getBoundingClientRect();return Math.round(viewStart+(event.clientX-rect.left-100)/plotWidth*(viewEnd-viewStart));};
    svg.addEventListener('wheel',event=>{
      event.preventDefault();if(drag)return;
      const rect=svg.getBoundingClientRect(),fraction=Math.max(0,Math.min(1,(event.clientX-rect.left-100)/plotWidth));
      const oldSize=viewEnd-viewStart,anchor=viewStart+fraction*oldSize;
      const size=Math.max(7,Math.min(maximum-minimum,oldSize*Math.exp(Math.max(-1,Math.min(1,event.deltaY*.002)))));
      viewStart=Math.max(minimum,Math.min(maximum-size,anchor-fraction*size));viewEnd=viewStart+size;render();
    },{passive:false,signal:events.signal});
    on(svg,'dblclick',event=>{if(event.target.dataset.id)return;viewStart=minimum;viewEnd=maximum;render();});
    on(svg,'pointerdown',event=>{
      if(event.button!==0&&event.button!==1)return;const id=event.target.dataset.id;
      if(!id&&(event.shiftKey||event.button===1)){
        const rect=svg.getBoundingClientRect();if(event.clientY-rect.top>310)return;
        event.preventDefault();svg.setPointerCapture(event.pointerId);
        drag={part:'pan',clientX:event.clientX,viewStart,viewEnd};return;
      }
      event.preventDefault();svg.setPointerCapture(event.pointerId);
      if(id&&event.target.dataset.part==='joint'){
        selected=id;drag={part:'joint',id,next:event.target.dataset.next,before:copy(windows)};return;
      }
      if(id){selected=id;const w=windows.find(w=>w.id===id);drag={id,part:event.target.dataset.part,start:pointerDay(event),original:copy(w),grabX:event.clientX-event.target.getBoundingClientRect().left,grabY:event.clientY-event.target.getBoundingClientRect().top,barWidth:event.target.getBoundingClientRect().width,holdoutY:345+Math.max(1,trainingLanes)*34+14,before:copy(windows)};}
      else {
        const localY=event.clientY-svg.getBoundingClientRect().top;
        drawRole=localY>=345+Math.max(1,trainingLanes)*34+14?'holdout':'training';
        drag={part:'draw',clientX:event.clientX,start:Math.max(minimum,Math.min(maximum-1,pointerDay(event))),before:copy(windows)};
      }
    });
    on(window,'pointermove',event=>{
      if(!drag)return;
      if(drag.part==='joint'){
        const first=windows.find(w=>w.id===drag.id),second=windows.find(w=>w.id===drag.next);
        const boundary=Math.max(day(first.start_date)+1,Math.min(day(second.end_date),pointerDay(event)));
        first.end_date=iso(boundary-1);second.start_date=iso(boundary);render();return;
      }
      if(drag.part==='pan'){
        const size=drag.viewEnd-drag.viewStart,shift=(event.clientX-drag.clientX)/plotWidth*size;
        viewStart=Math.max(minimum,Math.min(maximum-size,drag.viewStart-shift));viewEnd=viewStart+size;render();return;
      }
      if(drag.part==='move'){
        drag.active=true;ghost.hidden=false;
        const bounds=svg.getBoundingClientRect(),localY=event.clientY-bounds.top;
        const inLane=event.clientX>=bounds.left+100&&event.clientX<=bounds.right-20&&localY>=345&&localY<bounds.height-25;
        const previewRole=inLane?(localY>=drag.holdoutY?'holdout':'training'):drag.original.role;
        ghost.dataset.role=previewRole;
        ghost.textContent=windowCaption(windows.find(w=>w.id===drag.id));
        ghost.style.width=drag.barWidth+'px';
        ghost.style.left=(event.clientX-drag.grabX)+'px';ghost.style.top=(event.clientY-drag.grabY)+'px';
      }
      const dropping=drag.part==='move'&&overTrash(event);
      trash.classList.toggle('is-drop-target',dropping);
      if(dropping){render();return;}
      const d=pointerDay(event);
      if(drag.part==='draw'){
        render();
        const end=Math.max(minimum,Math.min(maximum-1,d));
        shape('rect',{x:x(Math.min(end,drag.start)),y:320,width:x(Math.max(end,drag.start)+1)-x(Math.min(end,drag.start)),height:26,fill:'rgba(77,166,255,.45)','pointer-events':'none'});
        return;
      }
      const w=windows.find(w=>w.id===drag.id),a=day(drag.original.start_date),b=day(drag.original.end_date),delta=d-drag.start;
      if(drag.part==='move'){const shift=Math.max(minimum-a,Math.min(maximum-1-b,delta));w.start_date=iso(a+shift);w.end_date=iso(b+shift);}
      if(drag.part==='left')w.start_date=iso(Math.max(minimum,Math.min(b,a+delta)));
      if(drag.part==='right')w.end_date=iso(Math.min(maximum-1,Math.max(a,b+delta)));
      snapWindow(w,drag.part);
      if(drag.part==='move')ghost.textContent=windowCaption(w);
      render();
    });
    on(window,'pointerup',event=>{
      if(!drag)return;
      if(drag.part==='pan'){drag=null;return;}
      ghost.hidden=true;
      trash.classList.remove('is-drop-target');
      if(drag.part==='move'&&!overTrash(event)){
        const rect=svg.getBoundingClientRect(),localY=event.clientY-rect.top;
        if(event.clientX>=rect.left+100&&event.clientX<=rect.right-20&&localY>=345&&localY<rect.height-25){
          windows.find(w=>w.id===drag.id).role=localY>=drag.holdoutY?'holdout':'training';
        }
      }
      if(drag.part==='move'&&overTrash(event)){
        windows=drag.before.filter(w=>w.id!==drag.id);selected=windows[0]?.id;
      }
      if(drag.part==='draw'&&Math.abs(event.clientX-drag.clientX)>4&&windows.length<64){const d=Math.max(minimum,Math.min(maximum-1,pointerDay(event)));const w={id:uid(),label:uniqueLabel(drawRole+'_window'),role:drawRole,start_date:iso(Math.min(d,drag.start)),end_date:iso(Math.max(d,drag.start)),scenario:{}};windows.push(w);selected=w.id;}
      if(JSON.stringify(drag.before)!==JSON.stringify(windows)){undo.push(drag.before);if(undo.length>50)undo.shift();redo=[];}
      drag=null;drawRole=null;changed();
    });
    on(window,'pointercancel',()=>{ghost.hidden=true;trash.classList.remove('is-drop-target');if(drag){if(drag.part==='pan'){viewStart=drag.viewStart;viewEnd=drag.viewEnd;}else windows=drag.before;drag=null;render();}});
    on(svg,'keydown',event=>{const id=event.target.dataset.id;if(id&&(event.key==='Enter'||event.key===' ')){event.preventDefault();selected=id;render();}});
    async function fetchJSON(url,signal){const response=await fetch(options.apiBase+url,{signal});if(!response.ok)throw new Error('Local chart unavailable (HTTP '+response.status+')');return response.json();}
    async function loadChart(){
      chartController?.abort();chartController=new AbortController();const current=++generation;chart=null;
      const row=source.value===''?null:sourceRows[Number(source.value)];if(!row){feedback.textContent='No local candle source available for this exchange.';render();return;}
      reference={key:referenceKey,exchange:exchange.value,coin:row.coin,dataset:row.dataset};
      feedback.title='';feedback.textContent='Loading local daily candles…';render();
      try{const params=new URLSearchParams({exchange:exchange.value,dataset:row.dataset,coin:row.coin,start:iso(minimum),end:iso(maximum-1)});const data=await fetchJSON('/scenario-templates/chart?'+params,chartController.signal);if(disposed||current!==generation)return;chart=data;const coverage=[];
        if(data.missing_days.length)coverage.push(data.missing_days.length+' missing days');
        if(data.incomplete_days.length)coverage.push(data.incomplete_days.length+' incomplete days');
        if(!data.candles.length)coverage.push('No data');
        feedback.textContent=exchange.value+' · '+(row.base_coin||row.coin)+' · '+data.candles.length+' days · '+(coverage.join(' · ')||'Complete');
        feedback.title='Reference: '+exchange.value+' / '+row.coin+' / '+row.dataset+'. Local candles aggregated daily for this chart; optimizer resolution is unchanged. Missing days appear as an orange strip. Coverage applies only to this reference dataset, not other optimizer coins or exchanges.';render();}catch(error){if(error.name!=='AbortError'&&!disposed&&current===generation)feedback.textContent=error.message;}
    }
    async function loadSources(){
      chartController?.abort();chartController=new AbortController();const current=++generation;chart=null;source.replaceChildren();sourceRows=[];feedback.textContent='Finding local candle sources…';
      try{const data=await fetchJSON('/scenario-templates/sources?'+new URLSearchParams({exchange:exchange.value}),chartController.signal);if(disposed||current!==generation)return;sourceRows=(data.sources||[]).filter(row=>(options.context.coins||[]).includes(row.base_coin)||(options.context.coins||[]).includes(row.coin));
        const placeholder=element('option','Choose reference coin…',source);placeholder.value='';
        sourceRows.forEach((row,i)=>{const opt=element('option',row.coin+' · '+row.dataset,source);opt.value=i;});
        const coins=options.context.coins||[];
        let chosen=reference?.key===referenceKey && reference.exchange===exchange.value?sourceRows.findIndex(row=>row.coin===reference.coin&&row.dataset===reference.dataset):-1;
        if(chosen<0)chosen=sourceRows.findIndex(row=>coins.includes(row.base_coin)||coins.includes(row.coin));
        source.value=chosen<0?'':String(chosen);
        if(chosen>=0)await loadChart();else{feedback.textContent=sourceRows.length?'Choose a reference coin. No local source matches the configured coins.':'No local candles found. Window editing remains available.';render();}}catch(error){if(error.name!=='AbortError'&&!disposed&&current===generation)feedback.textContent=error.message;}
    }
    on(exchange,'change',loadSources);on(source,'change',loadChart);
    const previousDispose=owner.dispose;
    let observedWidth=canvas.clientWidth;
    const observer=new ResizeObserver(()=>{
      if(disposed||canvas.clientWidth===observedWidth)return;
      observedWidth=canvas.clientWidth;
      render();
    });
    owner.dispose=()=>{observer.disconnect();previousDispose();};
    observer.observe(canvas);
    render();if(exchange.value)loadSources();
  }
  window.PBGuiScenarioVisual={mount,dispose(){owner?.dispose();owner=null;}};
}());
