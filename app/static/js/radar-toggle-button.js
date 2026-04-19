/* =============================================================
   radar-toggle-button.js
   Reusable radar toggle/add button module.

   Renders a flat-top regular hexagon button with two visual states:
     "add"   – green hexagon, centered + (pseudo-transparent)
     "added" – red hexagon, centered × (+ rotated 45°)

   API (exposed on window):
     buildRadarToggleBtnEl(cfg)        → HTMLButtonElement
     setRadarToggleBtnState(btn, state) → void

   cfg: { modelId, brand, label, state }
     state: 'add' | 'added'  (default: 'add')
   ============================================================= */

(function initRadarToggleButton() {
  'use strict';

  function EH(s) {
    if (typeof window.escapeHtml === 'function') return window.escapeHtml(s);
    const _ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
    return String(s ?? '').replace(/[&<>"']/g, c => _ESC[c]);
  }

  // Flat-top regular hexagon, viewBox 0 0 28 24
  // circumradius=12 centered at (14,12):
  //   right=(26,12), top-right=(20,1.6), top-left=(8,1.6),
  //   left=(2,12),   bot-left=(8,22.4),  bot-right=(20,22.4)
  const HEX_POINTS = '20,1.6 26,12 20,22.4 8,22.4 2,12 8,1.6';

  // SVG inner markup: hexagon + two pseudo-transparent bars forming +/×
  // bars are filled with --bg-secondary so they appear as holes cut through
  // the colored hexagon.  Rotation via CSS drives the + → × transition.
  const _SVG_INNER =
    `<svg class="fc-radar-toggle-svg" viewBox="0 0 28 24" ` +
    `xmlns="http://www.w3.org/2000/svg" aria-hidden="true">` +
      `<polygon class="fc-radar-toggle-bg" points="${HEX_POINTS}"/>` +
      `<g class="fc-radar-toggle-icon">` +
        `<rect class="fc-radar-toggle-bar" x="12" y="4.5" width="4" height="15" rx="1.5"/>` +
        `<rect class="fc-radar-toggle-bar" x="6.5" y="10" width="15" height="4" rx="1.5"/>` +
      `</g>` +
    `</svg>`;

  /**
   * Build a radar toggle button element.
   *
   * @param {Object} cfg
   *   cfg.modelId  {string|number}
   *   cfg.brand    {string}
   *   cfg.label    {string}   model display name
   *   cfg.state    {'add'|'added'}  default 'add'
   * @returns {HTMLButtonElement}
   */
  function buildRadarToggleBtnEl(cfg) {
    const {
      modelId = '',
      brand   = '',
      label   = '',
      state   = 'add',
    } = cfg || {};

    const btn = document.createElement('button');
    btn.type      = 'button';
    btn.className = 'fc-radar-toggle-btn js-mini-radar-add fc-tooltip-target';
    btn.setAttribute('data-model-id', EH(String(modelId)));
    btn.setAttribute('data-brand',    EH(String(brand)));
    btn.setAttribute('data-label',    EH(String(label)));
    btn.innerHTML = _SVG_INNER;
    setRadarToggleBtnState(btn, state);
    return btn;
  }

  /**
   * Update the visual state of an existing radar toggle button.
   *
   * @param {HTMLElement} btn
   * @param {'add'|'added'} state
   */
  function setRadarToggleBtnState(btn, state) {
    const isAdded = state === 'added';
    btn.setAttribute('data-state',   isAdded ? 'added' : 'add');
    btn.setAttribute('data-tooltip', isAdded ? '从雷达图移除' : '添加到雷达对比');
  }

  window.buildRadarToggleBtnEl    = buildRadarToggleBtnEl;
  window.setRadarToggleBtnState   = setRadarToggleBtnState;

})();
