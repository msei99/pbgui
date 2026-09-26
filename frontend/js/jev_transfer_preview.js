(function () {
  'use strict';

  function requestedSources(message) {
    var source = String(message || '').trim();
    if (source.indexOf('```jev') === 0 && source.endsWith('```')) source = source.slice(6, -3).trim();
    if (source.charAt(0) !== '{') return false;
    try {
      var request = JSON.parse(source);
      return !!(request && Array.isArray(request.sources) && request.sources.length);
    } catch (_) {
      return false;
    }
  }

  async function discard(api, conversationId, previewId) {
    if (!previewId) return;
    await api('/conversations/' + encodeURIComponent(conversationId) + '/jev-preview/' + encodeURIComponent(previewId), {
      method: 'DELETE'
    });
  }

  async function review(options) {
    if (options.provider !== 'openrouter' || !requestedSources(options.message)) return { previewId: '' };
    var path = '/conversations/' + encodeURIComponent(options.conversationId) + '/jev-preview';
    var preview = await options.api(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: options.message, model: options.model })
    });
    var previewId = String(preview.preview_id || '');
    var confirmed = false;
    try {
      confirmed = await options.confirm({
        title: 'Review Jev data transfer',
        message: 'PBGui will send exactly this request, including the listed PBGui data, to OpenRouter. Review the full payload before approving.',
        detail: JSON.stringify(preview.payload, null, 2),
        wide: true,
        codeDetail: true,
        confirmText: 'Send to OpenRouter'
      });
    } finally {
      if (!confirmed) await discard(options.api, options.conversationId, previewId).catch(function () {});
    }
    return confirmed ? { previewId: previewId } : { cancelled: true };
  }

  window.PBGuiJevTransferPreview = { requestedSources: requestedSources, review: review, discard: discard };
})();
