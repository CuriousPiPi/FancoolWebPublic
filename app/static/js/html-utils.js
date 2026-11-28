(function () {
  'use strict';
  const entities = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
  window.FcHtml = Object.freeze({
    escape(value) {
      return String(value).replace(/[&<>"']/g, char => entities[char]);
    },
  });
})();
