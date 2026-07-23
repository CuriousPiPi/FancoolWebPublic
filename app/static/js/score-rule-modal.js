/* =============================================================
   score-rule-modal.js
   Score rule explanation modal UI logic.
   ============================================================= */

(function initScoreRuleModalModule() {
  'use strict';

  function initAlgorithmExplainModal() {
    function getModal() { return document.getElementById('scoreRuleModal'); }
    function getLoading() { return document.getElementById('scoreRuleLoading'); }
    function getTableWrap() { return document.getElementById('scoreRuleTableWrap'); }
    function getFootnote() { return document.getElementById('scoreRuleFootnote'); }

    function renderExplain(data) {
      const wrap = getTableWrap();
      const foot = getFootnote();
      if (!wrap || !foot) return;

      const items = Array.isArray(data && data.items) ? data.items : [];
      if (!items.length) {
        wrap.innerHTML = '<p style="color:var(--text-secondary);padding:20px 0">暂无说明内容，请稍后重试。</p>';
        foot.innerHTML = '';
        return;
      }

      const container = document.createElement('div');
      container.className = 'fc-sr-algo-list';
      items.forEach(it => {
        const block = document.createElement('div');
        block.className = 'fc-sr-algo-item';
        const h4 = document.createElement('h4');
        h4.textContent = String(it.title || '');
        const p = document.createElement('p');
        p.textContent = String(it.content || '');
        block.appendChild(h4);
        block.appendChild(p);
        container.appendChild(block);
      });

      const cw = data && data.composite_weighting;
      const cwRows = Array.isArray(cw && cw.conditions) ? cw.conditions : [];
      if (cwRows.length) {
        const cBlock = document.createElement('div');
        cBlock.className = 'fc-sr-algo-item';
        const cTitle = document.createElement('h4');
        cTitle.textContent = String((cw && cw.title) || '综合分权重');
        const cDesc = document.createElement('p');
        cDesc.textContent = String((cw && cw.content) || '');
        cBlock.appendChild(cTitle);
        cBlock.appendChild(cDesc);

        const wtWrap = document.createElement('div');
        wtWrap.className = 'fc-sr-wt-wrap';
        const wt = document.createElement('table');
        wt.className = 'fc-sr-weight-table';
        const trHead = document.createElement('tr');
        const trWeight = document.createElement('tr');

        const firstHead = document.createElement('th');
        firstHead.textContent = '工况';
        trHead.appendChild(firstHead);
        const firstWeight = document.createElement('th');
        firstWeight.textContent = '权重占比';
        trWeight.appendChild(firstWeight);

        cwRows.forEach(row => {
          const th = document.createElement('th');
          const cname = String(row.condition_name || '').trim();
          const cid = Number(row.condition_id);
          th.textContent = cname || (Number.isFinite(cid) ? `工况 ${cid}` : '工况');
          trHead.appendChild(th);

          const td = document.createElement('td');
          const pct = Number(row.weight_pct);
          td.textContent = Number.isFinite(pct) ? `${pct.toFixed(1)}%` : '-';
          trWeight.appendChild(td);
        });

        wt.appendChild(trHead);
        wt.appendChild(trWeight);
        wtWrap.appendChild(wt);
        cBlock.appendChild(wtWrap);
        container.appendChild(cBlock);
      }

      wrap.innerHTML = '';
      wrap.appendChild(container);
      foot.innerHTML = '';
    }

    let lastFocused = null;
    let prevBodyOverflow = '';
    let focusTrapBound = false;

    function getFocusableElements(root) {
      if (!root) return [];
      return Array.from(root.querySelectorAll(
        'a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])'
      )).filter(el => el.offsetParent !== null);
    }

    function handleFocusTrapKeydown(e) {
      const modal = getModal();
      if (!modal || modal.hasAttribute('hidden') || e.key !== 'Tab') return;
      const list = getFocusableElements(modal);
      if (!list.length) {
        e.preventDefault();
        modal.focus({ preventScroll: true });
        return;
      }
      const first = list[0];
      const last = list[list.length - 1];
      if (e.shiftKey) {
        if (document.activeElement === first || !modal.contains(document.activeElement)) {
          e.preventDefault();
          last.focus({ preventScroll: true });
        }
      } else if (document.activeElement === last || !modal.contains(document.activeElement)) {
        e.preventDefault();
        first.focus({ preventScroll: true });
      }
    }

    function activateFocusTrap() {
      if (focusTrapBound) return;
      document.addEventListener('keydown', handleFocusTrapKeydown, true);
      focusTrapBound = true;
    }

    function deactivateFocusTrap() {
      if (!focusTrapBound) return;
      document.removeEventListener('keydown', handleFocusTrapKeydown, true);
      focusTrapBound = false;
    }

    function openModal() {
      const modal = getModal();
      if (!modal) return;
      lastFocused = document.activeElement;
      modal.removeAttribute('hidden');
      prevBodyOverflow = document.body.style.overflow;
      document.body.style.overflow = 'hidden';
      const focusables = getFocusableElements(modal);
      try { (focusables[0] || modal).focus({ preventScroll: true }); } catch (_) {}
      activateFocusTrap();
    }

    function closeModal() {
      const modal = getModal();
      if (!modal) return;
      modal.setAttribute('hidden', '');
      deactivateFocusTrap();
      document.body.style.overflow = prevBodyOverflow;
      if (lastFocused && typeof lastFocused.focus === 'function') lastFocused.focus();
      lastFocused = null;
    }

    function loadExplain() {
      const loading = getLoading();
      const wrap = getTableWrap();
      if (loading) { loading.style.display = ''; loading.textContent = '加载中…'; }
      if (wrap) { wrap.style.display = 'none'; wrap.innerHTML = ''; }

      fetch('/api/score_rule_explain')
        .then(async r => {
          let body = null;
          try { body = await r.json(); } catch (_) {}
          if (!r.ok) {
            const msg = body && (body.error_message || body.message || body.error);
            throw new Error(msg || `请求失败(${r.status})`);
          }
          return body;
        })
        .then(resp => {
          if (!resp || !resp.success) throw new Error('请求失败');
          if (loading) loading.style.display = 'none';
          if (wrap) wrap.style.display = '';
          renderExplain(resp.data || {});
        })
        .catch((err) => {
          if (loading) {
            const msg = err && err.message ? String(err.message) : '';
            loading.textContent = msg ? `加载失败：${msg}` : '加载失败，请稍后再试。';
            loading.style.display = '';
          }
        });
    }

    const explainBtn = document.getElementById('scoreRuleExplainBtn');
    const modal = getModal();
    const closeBtn = document.getElementById('scoreRuleClose');
    const backdrop = document.getElementById('scoreRuleBackdrop');
    if (!explainBtn || !modal) return;

    explainBtn.addEventListener('click', () => {
      loadExplain();
      openModal();
    });

    if (closeBtn) closeBtn.addEventListener('click', closeModal);
    if (backdrop) backdrop.addEventListener('click', closeModal);
    document.addEventListener('keydown', e => {
      if (e.key === 'Escape' && !modal.hasAttribute('hidden')) closeModal();
    });
  }

  window.ScoreRuleModal = { initAlgorithmExplainModal };
})();
