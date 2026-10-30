/* Browser-only test selectors; all application code still runs unmodified. */
(() => {
  window.__pbguiMatch=(actual,wanted,exact)=>wanted===null||wanted===undefined||
    (wanted instanceof RegExp?wanted.test(actual||''):exact?actual===wanted:String(actual||'').includes(wanted));
  window.__pbguiVisible=e=>{
    if(!e||getComputedStyle(e).visibility==='hidden'||getComputedStyle(e).display==='none')return false;
    for(let p=e.parentElement;p;p=p.parentElement)if(p.tagName==='DETAILS'&&!p.open&&
      !p.querySelector(':scope > summary')?.contains(e))return false;
    return !!e.getClientRects().length&&e.getBoundingClientRect().width>0&&e.getBoundingClientRect().height>0;
  };
  window.__pbguiQuery=(root,selector)=>{
    if(!root)return [];
    if(selector==='..'||selector==='xpath=..')return root.parentElement?[root.parentElement]:[];
    if(selector.includes(' >> '))return selector.split(' >> ').reduce((roots,part)=>
      roots.flatMap(r=>__pbguiQuery(r,part)),[root]);
    const filters=[];
    selector=selector.replace(/:(has-text|text-is|text)\(("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')\)/g,
      (_,kind,text)=>{const value=text[0]==='"'?JSON.parse(text):text.slice(1,-1).replace(/\\'/g,"'");
        filters.push(e=>__pbguiMatch(e.textContent,value,kind==='text-is'));return '';});
    if(selector.includes(':visible')){selector=selector.replace(/:visible/g,'');filters.push(__pbguiVisible);}
    const nth=selector.match(/:nth-match\((.+),\s*(\d+)\)$/);
    if(nth)return __pbguiQuery(root,nth[1]).slice(Number(nth[2])-1,Number(nth[2]));
    let values=Array.from(root.querySelectorAll(selector||'*'));
    for(const filter of filters)values=values.filter(filter);
    return values;
  };
  const accessible=e=>{
    if(e.hasAttribute('aria-label'))return e.getAttribute('aria-label');
    const ids=e.getAttribute('aria-labelledby');
    if(ids)return ids.split(/\s+/).map(id=>e.ownerDocument.getElementById(id)?.textContent||'').join(' ');
    if(e.labels?.length)return Array.from(e.labels).map(l=>l.textContent).join(' ').trim();
    return e.textContent?.trim()||e.getAttribute('alt')||e.getAttribute('title')||'';
  };
  window.__pbguiRole=(root,role,name,exact,includeHidden=false)=>{
    const native={button:'button,input[type="button"],input[type="submit"]',link:'a[href]',
      textbox:'textarea,input:not([type]),input[type="text"],input[type="password"],input[type="search"],input[type="email"],input[type="url"],input[type="date"],input[type="datetime-local"],input[type="time"]',
      checkbox:'input[type="checkbox"]',radio:'input[type="radio"]',combobox:'select:not([multiple])',
      searchbox:'input[type="search"]',spinbutton:'input[type="number"]',
      listbox:'select[multiple]',option:'option',dialog:'dialog',heading:'h1,h2,h3,h4,h5,h6',
      row:'tr',cell:'td',table:'table',columnheader:'th',img:'img'};
    return Array.from(root.querySelectorAll(`[role="${CSS.escape(role)}"]${native[role]?','+native[role]:''}`))
      .filter(e=>{if(includeHidden)return true;for(let p=e;p;p=p.parentElement){const css=getComputedStyle(p);
        if(p.getAttribute('aria-hidden')==='true'||p.inert||css.display==='none'||css.visibility==='hidden')return false;
        if(p!==e&&p.tagName==='DETAILS'&&!p.open&&!p.querySelector(':scope > summary')?.contains(e))return false;
      }return true;})
      .filter(e=>__pbguiMatch(accessible(e).replace(/\s+/g,' '),name,exact));
  };
  window.__pbguiText=(root,text,exact)=>Array.from(root.querySelectorAll('*'))
    .filter(e=>!['SCRIPT','STYLE','HEAD'].includes(e.tagName)&&__pbguiMatch(e.textContent?.trim(),text,exact)&&
      !Array.from(e.children).some(child=>__pbguiMatch(child.textContent?.trim(),text,exact)));
  window.__pbguiLabel=(root,text,exact)=>{
    const result=Array.from(root.querySelectorAll('[aria-label]')).filter(e=>__pbguiMatch(e.getAttribute('aria-label'),text,exact));
    for(const label of root.querySelectorAll('label'))if(__pbguiMatch(label.textContent.trim(),text,exact)){
      const control=label.control||label.querySelector('input,select,textarea');if(control&&!result.includes(control))result.push(control);
    }return result;
  };
})();
