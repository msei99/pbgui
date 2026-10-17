/* Owner-authenticated GitHub reporting. Credentials never enter URLs or browser storage. */
(function () {
  'use strict';
  function node(tag,text) { var n=document.createElement(tag); if(text!=null)n.textContent=text; return n; }
  function create(root,options) {
    var section=node('details'); section.appendChild(node('summary','GitHub connection and publication'));
    var connection=node('p'), settings=node('div'), label=node('label','GitHub personal access token'), token=node('input');
    token.type='password'; token.autocomplete='new-password'; token.maxLength=255; token.setAttribute('aria-label','GitHub personal access token'); label.appendChild(token);
    var tokenLink=node('a','Create a GitHub token (public_repo)'); tokenLink.href='https://github.com/settings/tokens/new?description=PBGui%20Support&scopes=public_repo'; tokenLink.target='_blank'; tokenLink.rel='noopener noreferrer';
    var connect=node('button','Connect GitHub'), disconnect=node('button','Disconnect GitHub');
    connect.type=disconnect.type='button'; connect.className=disconnect.className='support-screenshot-button';
    var credentials=node('div'); credentials.className='support-ai-actions'; credentials.append(connect,disconnect);
    settings.append(label,tokenLink,credentials);
    var imageNotice=node('p','Optional: selected screenshots below will be PUBLIC in the GitHub issue and your support asset repository. Unselected images stay encrypted.');
    var imageChoices=node('div'); imageChoices.className='support-ai-image-choices'; imageChoices.setAttribute('aria-label','Public GitHub screenshots');
    var selected=new Set();
    var reviewButton=node('button','Prepare publication review'); reviewButton.type='button'; reviewButton.className='support-screenshot-button';
    var reviewInfo=node('p'), reviewedImages=node('div'), publish=node('button','Publish reviewed issue'), discard=node('button','Discard local review'); reviewedImages.className='support-ai-image-choices';
    publish.type=discard.type='button'; publish.className=discard.className='support-screenshot-button'; publish.hidden=discard.hidden=true;
    var issue=node('a','Open published issue'); issue.target='_blank'; issue.rel='noopener noreferrer'; issue.hidden=true;
    var status=node('p'); status.setAttribute('role','status'); var error=node('p'); error.className='support-issue-error'; error.setAttribute('role','alert');
    section.append(connection,settings,imageNotice,imageChoices,reviewButton,reviewInfo,reviewedImages,publish,discard,issue,status,error); root.appendChild(section);
    var review=null, configured=false, working=false, publishing=false, generation=0, poll=null, opened=false;
    var storageKey='pbgui.support.github.review', imageUrls=[], imageRequest=null, imagesLoading=false, previewedReviewId=null;
    function clearReviewedImages(){if(imageRequest)imageRequest.abort();imageRequest=null;imageUrls.forEach(function(url){URL.revokeObjectURL(url);});imageUrls=[];reviewedImages.replaceChildren();imagesLoading=false;previewedReviewId=null;}
    async function api(path,init) {
      var response=await fetch(window.API_BASE+'/github'+path,Object.assign({credentials:'same-origin',cache:'no-store'},init||{}));
      var value=await response.json(); if(!response.ok)throw new Error(typeof value.detail==='string'?value.detail:'GitHub operation failed.'); return value;
    }
    function controls() {
      var remoteBusy=review&&review.status==='publishing';
      connect.disabled=disconnect.disabled=reviewButton.disabled=working||remoteBusy;
      publish.disabled=working||imagesLoading||remoteBusy; disconnect.hidden=!configured;
      token.parentElement.hidden=configured; tokenLink.hidden=configured; connect.hidden=configured;
    }
    function remember(id) { try { if(id)sessionStorage.setItem(storageKey,id); else sessionStorage.removeItem(storageKey); } catch(_){} }
    function imageRows() {
      var images=options.getScreenshots();
      selected.forEach(function(id){if(!images.some(function(item){return item.id===id;}))selected.delete(id);});
      imageNotice.hidden=!images.length; imageChoices.replaceChildren();
      images.forEach(function(item,index){
        var button=node('button'); button.type='button'; button.className='support-public-image'; button.disabled=publishing;
        button.setAttribute('aria-label','Publish screenshot '+(index+1)+' publicly'); button.setAttribute('aria-pressed',String(selected.has(item.id))); button.classList.toggle('selected',selected.has(item.id));
        var thumb=node('img'); thumb.src=item.url; thumb.alt=''; button.append(thumb,node('span',item.name));
        button.addEventListener('click',function(){ invalidate(); if(selected.has(item.id))selected.delete(item.id); else selected.add(item.id); imageRows(); });
        imageChoices.appendChild(button);
      });
    }
    function invalidate() {
      if(publishing||(review&&review.status==='publishing'))return;
      generation++; clearTimeout(poll); poll=null;
      clearReviewedImages();
      if(review&&review.status==='prepared') {
        // Discard only a local unapproved review; never delete remote resources or receipts.
        api('/reviews/'+review.review_id,{method:'DELETE'}).catch(function(){});
      }
      var hadReview=Boolean(review);
      review=null; publish.hidden=discard.hidden=true; issue.hidden=true; reviewInfo.textContent=''; status.textContent=''; error.textContent='';
      if(hadReview){remember(null);options.resetPreview();}
    }
    function showReview(value) {
      review=value; remember(value.review_id); if(value.status!=='published')options.preview(value.body);
      reviewInfo.textContent='Reviewed title: '+value.title+' · Target: '+value.target+' · GitHub account: '+value.login+'. '+
        (value.assets_repository?'Publishing creates/uses PUBLIC '+value.assets_repository+' and a release for '+(value.encrypted_attachment?'an encrypted debug ZIP':'attachments')+'; '+value.public_image_count+' public screenshot(s).':'No attachment repository or files will be created.');
      publish.hidden=value.status==='published'||value.status==='publishing'; discard.hidden=value.status!=='prepared';
      publish.textContent=value.status==='uncertain'?'Check previous publication':'Publish reviewed issue';
      status.textContent=value.error || (value.status==='prepared'?'Review the exact public issue preview above, then publish.':value.status);
      issue.hidden=!/^https:\/\/github\.com\/msei99\/pbgui\/issues\/\d+$/.test(value.issue_url||'');
      if(!issue.hidden)issue.href=value.issue_url;
      if(value.public_image_count&&value.status!=='published'&&value.status!=='uncertain'){
        if(previewedReviewId!==value.review_id){clearReviewedImages();previewedReviewId=value.review_id;loadReviewedImages(value);}
      }else clearReviewedImages();
      controls();
    }
    async function loadReviewedImages(value){
      imagesLoading=true;controls();imageRequest=new AbortController();var signal=imageRequest.signal,current=generation;
      try{
        for(var index=0;index<value.public_image_count;index++){
          var response=await fetch(window.API_BASE+'/github/reviews/'+value.review_id+'/images/'+index,{credentials:'same-origin',cache:'no-store',signal:signal});
          if(!response.ok)throw new Error('Cannot preview the reviewed public screenshots. Prepare the review again.');
          var blob=await response.blob();if(current!==generation||signal.aborted)return;
          var url=URL.createObjectURL(blob);imageUrls.push(url);var image=node('img');image.src=url;image.alt='Reviewed public screenshot '+(index+1);image.className='support-reviewed-image';reviewedImages.appendChild(image);
        }
        imagesLoading=false;
      }catch(exc){if(current===generation&&exc.name!=='AbortError')error.textContent=exc.message;}
      finally{if(current===generation&&!signal.aborted){imageRequest=null;controls();}}
    }
    async function readStatus() {
      var current=generation;
      try {
        var account=await api(''); if(current!==generation||!opened)return;
        configured=Boolean(account.configured); connection.textContent=configured?'Connected as '+account.login:'Connect GitHub to publish directly from PBGui. Your token is stored encrypted on your own PBGui server.'; controls();
      }catch(exc){if(current===generation&&opened)error.textContent=exc.message;}
    }
    async function prepare() {
      if(working||options.isBusy())return;
      invalidate(); var current=generation;
      try {
        if(!configured){section.open=true; token.focus(); return;}
        var payload=options.payload();
        payload.public_images=[];
        options.getScreenshots().forEach(function(item,index){if(selected.has(item.id))payload.public_images.push(index);});
        var serialized=JSON.stringify(payload);
        if(new Blob([serialized]).size>12*1024*1024)throw new Error('Publication request exceeds 12 MiB. Reduce selected diagnostics or images.');
        working=true; controls(); status.textContent='Preparing the encrypted attachment and exact public review…';
        var value=await api('/review',{method:'POST',headers:{'Content-Type':'application/json'},body:serialized});
        if(current!==generation||!opened){api('/reviews/'+value.review_id,{method:'DELETE'}).catch(function(){});return;}
        showReview(value);
      }catch(exc){if(current===generation)error.textContent=exc.message;}
      finally{working=false;controls();}
    }
    async function restore(identifier) {
      var current=generation;
      try {
        var value=await api('/reviews/'+identifier); if(current!==generation||!opened)return;
        showReview(value);
        if(value.status==='publishing')poll=setTimeout(function(){restore(identifier);},1500);
      }catch(exc){if(current===generation&&opened)error.textContent=exc.message;}
    }
    async function open() {
      opened=true; section.open=true; imageRows(); await readStatus();
      if(!opened)return;
      var saved;try{saved=sessionStorage.getItem(storageKey);}catch(_){}
      if(saved&&/^[a-f0-9]{32}$/.test(saved))await restore(saved);
      else if(configured)await prepare();
      else token.focus();
    }
    connect.addEventListener('click',async function(){
      if(working)return; var secret=token.value; token.value='';
      working=true;controls();error.textContent='';
      try{var account=await api('/connect',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:secret})});configured=Boolean(account.configured);connection.textContent='Connected as '+account.login;}
      catch(exc){error.textContent=exc.message;}
      finally{secret='';working=false;controls();}
      if(configured&&opened)await prepare();
    });
    disconnect.addEventListener('click',async function(){
      if(working)return; invalidate();working=true;controls();
      try{await api('',{method:'DELETE'});configured=false;connection.textContent='GitHub disconnected.';}
      catch(exc){error.textContent=exc.message;}
      finally{working=false;controls();}
    });
    reviewButton.addEventListener('click',prepare);
    discard.addEventListener('click',invalidate);
    publish.addEventListener('click',async function(){
      if(working||!review)return;
      var identifier=review.review_id;working=publishing=true;controls();imageRows();status.textContent='Publishing the reviewed issue…';error.textContent='';
      try{showReview(await api('/publish',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({review_id:identifier})}));}
      catch(exc){error.textContent=exc.message;await restore(identifier);}
      finally{working=publishing=false;controls();imageRows();}
    });
    function hide(){opened=false;token.value='';generation++;clearTimeout(poll);poll=null;clearReviewedImages();}
    window.addEventListener('pagehide',hide);
    controls();imageRows();
    return {open:open,hide:hide,invalidate:invalidate,imagesChanged:function(){invalidate();imageRows();}};
  }
  window.PBGuiSupportGitHub={create:create};
})();
