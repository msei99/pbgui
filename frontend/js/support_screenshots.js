/* Local screenshot drafts: no upload, browser storage, or remote image sources. */
(function () {
  'use strict';
  var MAX_FILES = 5, MAX_FILE_BYTES = 4 * 1024 * 1024, MAX_REPORT_BYTES = 12 * 1024 * 1024;
  function node(tag, text) { var result = document.createElement(tag); if (text != null) result.textContent = text; return result; }
  function create(root, options) {
    var entries = [], readers = new Set(), epoch = 0, pending = 0, nextId = 0, queue = Promise.resolve();
    root.className = 'support-screenshots'; root.setAttribute('aria-label', 'Screenshots');
    var caption = node('h4', 'Screenshots'), add = node('button', 'Add screenshots');
    add.type = 'button'; add.className = 'support-screenshot-button';
    var input = node('input'); input.type = 'file'; input.accept = 'image/png,image/jpeg,image/webp'; input.multiple = true; input.hidden = true; input.setAttribute('aria-label', 'Choose screenshots');
    var hint = node('p', 'Drop or paste images · PNG/JPEG/WebP · up to 5 × 4 MiB');
    var status = node('p'); status.className = 'support-screenshot-status'; status.setAttribute('role', 'status');
    var error = node('p'); error.className = 'support-issue-error'; error.setAttribute('role', 'alert');
    var list = node('div'); list.className = 'support-screenshot-list';
    root.append(caption, add, input, hint, status, error, list);
    function payload() { return entries.map(function (entry) { return entry.payload; }); }
    function snapshot(extra) {
      var data = Object.assign({}, options.getReport() || {}), images = payload().concat(extra || []);
      if (images.length) data.screenshots = images;
      var result = JSON.stringify(data, null, 2);
      if (new Blob([result]).size > MAX_REPORT_BYTES) throw new Error('Report and screenshots exceed 12 MiB. Remove images or deselect diagnostic areas.');
      return result;
    }
    function render() {
      list.replaceChildren();
      entries.forEach(function (entry, index) {
        var card = node('div'); card.className = 'support-screenshot-card';
        var image = node('img'); image.src = entry.url; image.alt = entry.payload.name;
        var label = node('span', entry.payload.name + ' · ' + Math.ceil(entry.bytes / 1024) + ' KiB');
        var remove = node('button', 'Remove'); remove.type = 'button'; remove.className = 'support-screenshot-button';
        remove.setAttribute('aria-label', 'Remove screenshot ' + (index + 1));
        remove.addEventListener('click', function () { URL.revokeObjectURL(entry.url); entries.splice(entries.indexOf(entry), 1); render(); options.onChange(); });
        card.append(image, label, remove); list.appendChild(card);
      });
      status.textContent = entries.length + ' screenshot(s) · ' + Math.ceil(entries.reduce(function (sum, item) { return sum + item.bytes; }, 0) / 1024) + ' KiB';
    }
    function read(file) {
      return new Promise(function (resolve, reject) {
        var reader = new FileReader(); readers.add(reader);
        function finish() { readers.delete(reader); }
        reader.onload = function () { finish(); resolve(String(reader.result).split(',')[1]); };
        reader.onerror = function () { finish(); reject(new Error('Cannot read screenshot.')); };
        reader.onabort = function () { finish(); reject(new Error('Screenshot import cancelled.')); };
        reader.readAsDataURL(file);
      });
    }
    function importFiles(files) {
      var batch = Array.from(files || []), current = epoch;
      if (!batch.length) return;
      error.textContent = ''; pending++; options.onBusy(true);
      queue = queue.then(async function () {
        for (var file of batch) {
          if (current !== epoch) return;
          if (entries.length >= MAX_FILES) throw new Error('You can attach at most 5 screenshots.');
          if (!['image/png','image/jpeg','image/webp'].includes(file.type)) throw new Error('Use PNG, JPEG or WebP screenshots.');
          if (!file.size || file.size > MAX_FILE_BYTES) throw new Error('Each screenshot must be between 1 byte and 4 MiB.');
          var header = new Uint8Array(await file.slice(0,24).arrayBuffer());
          if (current !== epoch) return;
          var prefix = String.fromCharCode.apply(null, header);
          var matches = file.type === 'image/png' && prefix.startsWith('\x89PNG\r\n\x1a\n') ||
            file.type === 'image/jpeg' && prefix.startsWith('\xff\xd8\xff') ||
            file.type === 'image/webp' && prefix.startsWith('RIFF') && prefix.slice(8,12) === 'WEBP';
          if (!matches) throw new Error('Image content does not match its file type.');
          if (file.type === 'image/png' && header.length >= 24) {
            var dimensions = new DataView(header.buffer);
            if (dimensions.getUint32(16) * dimensions.getUint32(20) > 12000000) throw new Error('Screenshots may contain at most 12 million pixels.');
          }
          var bitmap;
          try { bitmap = await createImageBitmap(file); } catch (_) { throw new Error('The selected file is not a valid image.'); }
          var width = bitmap.width, height = bitmap.height; bitmap.close();
          if (width * height > 12000000) throw new Error('Screenshots may contain at most 12 million pixels.');
          if (current !== epoch) return;
          var encoded = await read(file);
          if (current !== epoch) return;
          var candidate = {name: String(file.name || 'screenshot').split(/[\\/]/).pop().slice(0,120), media_type:file.type,
            width:width, height:height, data_base64:encoded};
          snapshot([candidate]);
          entries.push({id:String(++nextId),payload:candidate,bytes:file.size,url:URL.createObjectURL(file)});
          render(); options.onChange();
        }
      }).catch(function (exc) { if (current === epoch) error.textContent = exc.message; })
        .finally(function () { pending--; options.onBusy(pending > 0); });
    }
    add.addEventListener('click', function () { input.click(); });
    input.addEventListener('change', function () { importFiles(input.files); input.value = ''; });
    var surface = options.surface || root;
    surface.addEventListener('dragover', function (event) { if (options.isOpen() && Array.from(event.dataTransfer.types || []).includes('Files')) event.preventDefault(); });
    surface.addEventListener('drop', function (event) { if (options.isOpen() && event.dataTransfer.files.length) { event.preventDefault(); importFiles(event.dataTransfer.files); } });
    surface.addEventListener('paste', function (event) {
      if (!options.isOpen()) return;
      var files = Array.from(event.clipboardData.items || []).filter(function (item) { return item.kind === 'file'; }).map(function (item) { return item.getAsFile(); }).filter(Boolean);
      if (files.length) { event.preventDefault(); importFiles(files); }
    });
    window.addEventListener('pagehide', function (event) {
      epoch++; readers.forEach(function (reader) { reader.abort(); });
      if (!event.persisted) { entries.forEach(function (entry) { URL.revokeObjectURL(entry.url); }); entries = []; render(); }
    });
    render();
    return {snapshot:snapshot, count:function () { return entries.length; }, isBusy:function () { return pending > 0; },
      forAI:function () { return entries.map(function (entry) { return {id:entry.id, name:entry.payload.name, url:entry.url,
        image:{media_type:entry.payload.media_type,data_base64:entry.payload.data_base64}}; }); }};
  }
  window.PBGuiSupportScreenshots = {create:create};
})();
