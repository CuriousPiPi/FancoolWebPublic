(function () {
  'use strict';

  const defaultSelector = 'a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])';

  function create(getModal, selector = defaultSelector) {
    let lastFocused = null;
    let previousOverflow = '';
    let active = false;

    function focusable(modal) {
      return Array.from(modal.querySelectorAll(selector)).filter(el => el.offsetParent !== null);
    }

    function onKeydown(event) {
      const modal = getModal();
      if (!modal || modal.hasAttribute('hidden') || event.key !== 'Tab') return;
      const elements = focusable(modal);
      if (!elements.length) {
        event.preventDefault();
        modal.focus({ preventScroll: true });
        return;
      }
      const first = elements[0], last = elements[elements.length - 1];
      if (event.shiftKey && (document.activeElement === first || !modal.contains(document.activeElement))) {
        event.preventDefault();
        last.focus({ preventScroll: true });
      } else if (!event.shiftKey && (document.activeElement === last || !modal.contains(document.activeElement))) {
        event.preventDefault();
        first.focus({ preventScroll: true });
      }
    }

    return {
      open(beforeFocus) {
        const modal = getModal();
        if (!modal) return;
        if (active) {
          if (beforeFocus) beforeFocus();
          return;
        }
        lastFocused = document.activeElement;
        modal.removeAttribute('hidden');
        previousOverflow = document.body.style.overflow;
        document.body.style.overflow = 'hidden';
        if (beforeFocus) beforeFocus();
        try { (focusable(modal)[0] || modal).focus({ preventScroll: true }); } catch (_) {}
        document.addEventListener('keydown', onKeydown, true);
        active = true;
      },
      close() {
        const modal = getModal();
        if (!modal || !active) return;
        modal.setAttribute('hidden', '');
        document.removeEventListener('keydown', onKeydown, true);
        document.body.style.overflow = previousOverflow;
        if (lastFocused && typeof lastFocused.focus === 'function') lastFocused.focus();
        lastFocused = null;
        active = false;
      },
    };
  }

  window.FcModalController = Object.freeze({ create });
})();
