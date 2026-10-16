(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) {
    module.exports = api;
  }
  // Only expose to the browser global; avoid polluting Node/globalThis when required in tests.
  if (root && root.document) {
    root.__APP = root.__APP || {};
    root.__APP.reviewJump = api;
  }
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  const REVIEW_JUMP_TARGET_FILE_SHARE = 'file-share';
  const REVIEW_JUMP_DEFAULT_LABEL = '资源下载';
  const REVIEW_JUMP_MARKUP_RE = /\[\[jump:([a-z0-9-]+)\|([\s\S]*?)\]\]/g;

  function parseReviewInlineTokens(reviewText) {
    const text = typeof reviewText === 'string' ? reviewText : String(reviewText || '');
    if (!text) return [];

    const tokens = [];
    REVIEW_JUMP_MARKUP_RE.lastIndex = 0;
    let cursor = 0;
    let match;

    while ((match = REVIEW_JUMP_MARKUP_RE.exec(text)) !== null) {
      const raw = match[0];
      const markerStart = match.index;
      const markerEnd = markerStart + raw.length;

      if (markerStart > cursor) {
        tokens.push({ type: 'text', value: text.slice(cursor, markerStart) });
      }

      const target = String(match[1] || '').trim().toLowerCase();
      const label = String(match[2] || '').trim();

      if (target === REVIEW_JUMP_TARGET_FILE_SHARE) {
        tokens.push({
          type: 'jump',
          target: REVIEW_JUMP_TARGET_FILE_SHARE,
          label: label || REVIEW_JUMP_DEFAULT_LABEL,
        });
      } else {
        tokens.push({ type: 'text', value: raw });
      }

      cursor = markerEnd;
    }

    if (cursor < text.length) {
      tokens.push({ type: 'text', value: text.slice(cursor) });
    }

    return tokens;
  }

  return {
    REVIEW_JUMP_TARGET_FILE_SHARE,
    REVIEW_JUMP_DEFAULT_LABEL,
    parseReviewInlineTokens,
  };
});
