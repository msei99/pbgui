/* Shared inert Markdown and scroll anchoring for both AI chat surfaces. */
(function () {
  'use strict';
  if (window.PBGuiAIMessageView) return;
  function normalizeTables(text) {
    // Repair only a header followed by a matching Markdown separator row.
    // Keep fenced/indented code literal and leave stored/copied text untouched.
    var lines = String(text || '').replace(/\r\n/g, '\n').split('\n'), output = [], fence = null;
    function cells(line) {
      return line.trim().replace(/^\|/, '').replace(/(?<!\\)\|$/, '').split(/(?<!\\)\|/).map(function (cell) {return cell.trim();});
    }
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i], marker = line.match(/^ {0,3}(`{3,}|~{3,})(.*)$/);
      if (marker) {
        if (!fence) fence = marker[1];
        else if (marker[1][0] === fence[0] && marker[1].length >= fence.length && !marker[2].trim()) fence = null;
        output.push(line); continue;
      }
      if (fence || /^( {4}|\t)/.test(line) || i + 1 >= lines.length) {output.push(line); continue;}
      var separator = lines[i + 1], columns = cells(separator);
      if (/^( {4}|\t)/.test(separator) || columns.length < 2 || !columns.every(function (cell) {return /^:?-+:?$/.test(cell);})) {
        output.push(line); continue;
      }
      var header = line, prefix = '', firstPipe = /(?<!\\)\|/.exec(line);
      if (cells(header).length !== columns.length && firstPipe && firstPipe.index > 0) {
        prefix = line.slice(0, firstPipe.index).trimEnd(); header = line.slice(firstPipe.index);
      }
      if (cells(header).length !== columns.length) {output.push(line); continue;}
      if (prefix) output.push(prefix);
      if (output.length && output[output.length - 1].trim()) output.push('');
      output.push(header, separator); i += 2;
      while (i < lines.length && /(?<!\\)\|/.test(lines[i]) && !/^\s*(`{3,}|~{3,})/.test(lines[i])) {
        output.push(lines[i++]);
      }
      if (i < lines.length && lines[i].trim()) output.push('');
      i -= 1;
    }
    return output.join('\n');
  }
  function render(target, text) {
    target.classList.add('pbgui-ai-markdown');
    target.textContent = '';
    if (!window.marked || !window.DOMPurify) { target.textContent = String(text || ''); return; }
    var fragment = DOMPurify.sanitize(marked.parse(normalizeTables(text), {gfm:true, breaks:true}), {
      RETURN_DOM_FRAGMENT:true, ALLOW_DATA_ATTR:false, ALLOW_ARIA_ATTR:false,
      ALLOWED_TAGS:['p','br','strong','em','del','code','pre','blockquote','ul','ol','li','h1','h2','h3','h4','hr','table','thead','tbody','tr','th','td','a'],
      ALLOWED_ATTR:['href','title','start']
    });
    fragment.querySelectorAll('a').forEach(function (link) {
      try {
        var url = new URL(link.getAttribute('href'));
        if (url.protocol !== 'https:' || url.username || url.password) throw new Error('Unsafe link');
        link.href = url.href; link.target = '_blank'; link.rel = 'noopener noreferrer'; link.referrerPolicy = 'no-referrer';
      } catch (_) { link.removeAttribute('href'); }
    });
    fragment.querySelectorAll('table').forEach(function (table) {
      var wrap = document.createElement('div'); wrap.className = 'pbgui-ai-table-scroll';
      wrap.tabIndex = 0; wrap.setAttribute('role', 'region'); wrap.setAttribute('aria-label', 'Response table');
      table.replaceWith(wrap); wrap.appendChild(table);
      table.querySelectorAll('thead th').forEach(function (th) { th.scope = 'col'; });
    });
    target.appendChild(fragment);
  }
  function key(element) {
    if (element.dataset.researchId) return 'research:' + element.dataset.researchId;
    if (element.dataset.messageIndex != null) return 'message:' + element.dataset.messageIndex;
    return null;
  }
  function capture(box, fresh) {
    box.classList.add('pbgui-ai-transcript');
    var top = box.getBoundingClientRect().top;
    var anchor = Array.from(box.children).find(function (e) {return key(e) && e.getBoundingClientRect().bottom > top;});
    return {bottom: !!fresh || box.scrollHeight - box.clientHeight - box.scrollTop < 48,
      top:box.scrollTop, key:anchor && key(anchor), offset:anchor ? anchor.getBoundingClientRect().top - top : 0};
  }
  function restore(box, saved) {
    if (!saved) return;
    if (saved.bottom) {box.scrollTop = box.scrollHeight; return;}
    var anchor = Array.from(box.children).find(function (e) {return key(e) === saved.key;});
    if (anchor) box.scrollTop += anchor.getBoundingClientRect().top - box.getBoundingClientRect().top - saved.offset;
    else box.scrollTop = saved.top;
  }
  window.PBGuiAIMessageView = {render:render, capture:capture, restore:restore};
}());
