/* =============================================================
   score-profile.js
   Global scoring profile selector: LOW / MED / HI.

   Reads/writes LocalState.getScoreProfile() / setScoreProfile().
   Exposes window.ScoreProfile with:
     getProfile()            → 'low' | 'med' | 'hi'
     setProfile(p)           → sets and broadcasts the profile
     getScoreForItem(item)   → resolved composite_score from item
     getCondScoresForItem(item, radarCids?) → resolved condition_scores dict
     onProfileChange(fn)     → register a listener called on profile changes

   Other modules can call ScoreProfile.getScoreForItem(item) and
   ScoreProfile.getCondScoresForItem(item) instead of reading
   item.composite_score / item.condition_scores directly.
   ============================================================= */

(function () {
  'use strict';

  const PROFILES = ['low', 'med', 'hi'];
  const DEFAULT_PROFILE = 'med';
  let _listeners = [];

  function _getLS() {
    return (typeof window.LocalState !== 'undefined' && window.LocalState) || null;
  }

  function getProfile() {
    const ls = _getLS();
    const validProfiles = (ls && ls.VALID_SCORE_PROFILES) || PROFILES;
    if (ls && typeof ls.getScoreProfile === 'function') return ls.getScoreProfile();
    const v = (localStorage.getItem('fc_score_profile_v1') || '').trim();
    return validProfiles.includes(v) ? v : DEFAULT_PROFILE;
  }

  function setProfile(p) {
    const validProfiles = (_getLS() && _getLS().VALID_SCORE_PROFILES) || PROFILES;
    const profile = validProfiles.includes(p) ? p : DEFAULT_PROFILE;
    const ls = _getLS();
    if (ls && typeof ls.setScoreProfile === 'function') {
      ls.setScoreProfile(profile);
    } else {
      try { localStorage.setItem('fc_score_profile_v1', profile); } catch (_) {}
    }
    _broadcast(profile);
    _syncSelectorUI(profile);
    return profile;
  }

  function _broadcast(profile) {
    _listeners.forEach(fn => { try { fn(profile); } catch (_) {} });
  }

  function onProfileChange(fn) {
    if (typeof fn === 'function') _listeners.push(fn);
  }

  /**
   * Resolve composite_score for an item given the current profile.
   * Falls back to item.composite_score (med / legacy) if score_profiles is absent.
   */
  function getScoreForItem(item) {
    if (!item) return null;
    const profile = getProfile();
    const sp = item.score_profiles;
    if (sp && sp[profile] && sp[profile].composite_score !== undefined) {
      return sp[profile].composite_score;
    }
    // Fallback: med / legacy field
    return item.composite_score !== undefined ? item.composite_score : null;
  }

  /**
   * Resolve condition_scores for an item given the current profile.
   * radarCids is optional; if provided, missing cids are filled with null.
   */
  function getCondScoresForItem(item, radarCids) {
    if (!item) return {};
    const profile = getProfile();
    const sp = item.score_profiles;
    let cond = null;
    if (sp && sp[profile] && sp[profile].condition_scores) {
      cond = sp[profile].condition_scores;
    } else {
      cond = item.condition_scores || {};
    }
    if (!radarCids) return cond;
    const result = {};
    radarCids.forEach(cid => { result[cid] = cond[cid] !== undefined ? cond[cid] : null; });
    return result;
  }

  // ---- Selector UI wiring ----

  // Module-level hook so _syncSelectorUI can update the custom button label.
  let _customUpdateLabel = null;

  function _syncSelectorUI(profile) {
    const sel = document.getElementById('scoreProfileSelect');
    if (sel) sel.value = profile;
    if (typeof _customUpdateLabel === 'function') _customUpdateLabel(profile);
  }

  // Human-readable labels for each profile value (no prefix).
  const _PROFILE_LABELS = { low: '只在意中低转表现', med: '家用场景均衡策略', hi: '高转速大风量优先' };

  function _buildCustomDropdown() {
    const sel = document.getElementById('scoreProfileSelect');
    if (!sel || sel.dataset.spsCustomBuilt) return;
    sel.dataset.spsCustomBuilt = '1';

    const wrap = document.getElementById('scoreProfileDropdown');
    if (!wrap) return;

    // Hide the native select; the custom button takes over.
    sel.style.display = 'none';

    // --- Trigger button: [arrow] [label text] ---
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'fc-sps-btn';
    btn.setAttribute('aria-haspopup', 'listbox');
    btn.setAttribute('aria-expanded', 'false');

    const arrowEl = document.createElement('i');
    arrowEl.className = 'fa-solid fa-chevron-down fc-sps-arrow';

    const labelEl = document.createElement('span');
    labelEl.className = 'fc-sps-label';

    btn.appendChild(arrowEl);
    btn.appendChild(labelEl);
    wrap.appendChild(btn);

    // --- Options panel (portal, styled via existing fc-custom-options) ---
    const panel = document.createElement('div');
    panel.className = 'fc-custom-options fc-portal hidden';
    panel.setAttribute('role', 'listbox');
    document.body.appendChild(panel);

    function _buildOptions() {
      panel.innerHTML = '';
      const header = document.createElement('div');
      header.className = 'fc-sps-panel-header';
      header.textContent = '不同策略评分无可比性';
      panel.appendChild(header);
      Array.from(sel.options).forEach(opt => {
        const item = document.createElement('div');
        item.className = 'fc-option';
        item.setAttribute('role', 'option');
        item.dataset.value = opt.value;
        item.textContent = _PROFILE_LABELS[opt.value] || opt.text;
        panel.appendChild(item);
      });
    }

    function _updateLabel(v) {
      const text = _PROFILE_LABELS[v] || v;
      labelEl.textContent = text ? '评分策略：' + text : '';
    }

    _buildOptions();
    _updateLabel(getProfile());

    // Expose label updater so _syncSelectorUI can reach it.
    _customUpdateLabel = _updateLabel;

    // --- Portal positioning (simplified: always opens below if space allows) ---
    const MIN_SPACE_BELOW = 120; // px threshold below which we prefer opening upward
    let _bound = false;

    function _placePanel() {
      const r = btn.getBoundingClientRect();
      panel.style.visibility = 'hidden';
      panel.classList.remove('hidden');
      const spaceBelow = window.innerHeight - r.bottom - 4;
      const spaceAbove = r.top - 4;
      const openUp = spaceBelow < MIN_SPACE_BELOW && spaceAbove > spaceBelow;
      panel.style.bottom = '';
      panel.style.top = openUp
        ? (r.top - panel.offsetHeight - 4) + 'px'
        : (r.bottom + 4) + 'px';
      panel.style.left = r.left + 'px';
      panel.style.minWidth = r.width + 'px';
      const pr = panel.getBoundingClientRect();
      if (pr.right > window.innerWidth - 4) {
        panel.style.left = Math.max(4, r.right - panel.offsetWidth) + 'px';
      }
      if (pr.left < 4) panel.style.left = '4px';
      panel.style.visibility = '';
    }

    function _openPanel() {
      document.querySelectorAll('.fc-custom-options').forEach(p => {
        if (p !== panel) p.classList.add('hidden');
      });
      _placePanel();
      btn.setAttribute('aria-expanded', 'true');
      arrowEl.classList.add('fc-sps-arrow--open');
      if (!_bound) {
        _bound = true;
        window.addEventListener('scroll', _placePanel, true);
        window.addEventListener('resize', _placePanel, { passive: true });
        document.addEventListener('click', _onDocClick, true);
      }
    }

    function _closePanel() {
      panel.classList.add('hidden');
      btn.setAttribute('aria-expanded', 'false');
      arrowEl.classList.remove('fc-sps-arrow--open');
      if (_bound) {
        _bound = false;
        window.removeEventListener('scroll', _placePanel, true);
        window.removeEventListener('resize', _placePanel);
        document.removeEventListener('click', _onDocClick, true);
      }
    }

    function _onDocClick(e) {
      if (!btn.contains(e.target) && !panel.contains(e.target)) _closePanel();
    }

    btn.addEventListener('click', () => {
      const isOpen = btn.getAttribute('aria-expanded') === 'true';
      if (isOpen) _closePanel(); else _openPanel();
    });

    panel.addEventListener('click', e => {
      const item = e.target.closest('.fc-option');
      if (!item) return;
      const v = item.dataset.value;
      if (!PROFILES.includes(v)) return;
      setProfile(v);
      _closePanel();
    });
  }

  // Also listen to LocalState's score_profile_changed event
  window.addEventListener('localstate:score_profile_changed', e => {
    const profile = e.detail && e.detail.profile;
    if (profile) {
      _broadcast(profile);
      _syncSelectorUI(profile);
    }
  });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _buildCustomDropdown);
  } else {
    _buildCustomDropdown();
  }

  // ---- 评分规则说明弹层 ----

  (function _initScoreRuleModal() {
    // Cache fetched rule data per profile to avoid repeat requests
    const _ruleCache = {};
    let _currentModalProfile = null;

    function _getModal()    { return document.getElementById('scoreRuleModal'); }
    function _getLoading()  { return document.getElementById('scoreRuleLoading'); }
    function _getTableWrap(){ return document.getElementById('scoreRuleTableWrap'); }
    function _getFootnote() { return document.getElementById('scoreRuleFootnote'); }

    // Simple focus-trap state for the score rule modal
    let _lastFocused = null;
    let _prevBodyOverflow = '';
    let _focusTrapHandler = null;

    function _getFocusableElements(container) {
      if (!container) return [];
      const selectors = [
        'a[href]',
        'area[href]',
        'button:not([disabled])',
        'input:not([disabled]):not([type="hidden"])',
        'select:not([disabled])',
        'textarea:not([disabled])',
        '[tabindex]:not([tabindex="-1"])'
      ];
      const nodes = Array.prototype.slice.call(
        container.querySelectorAll(selectors.join(','))
      );
      return nodes.filter(function (el) {
        if (el.hasAttribute('hidden')) return false;
        if (el.getAttribute('aria-hidden') === 'true') return false;
        const style = window.getComputedStyle ? window.getComputedStyle(el) : null;
        if (style && (style.display === 'none' || style.visibility === 'hidden')) return false;
        return true;
      });
    }

    function _activateFocusTrap(modal) {
      if (!modal || _focusTrapHandler) return;
      _focusTrapHandler = function (event) {
        if (event.key !== 'Tab' && event.keyCode !== 9) return;
        const focusable = _getFocusableElements(modal);
        if (!focusable.length) {
          // If nothing is focusable inside, keep focus on the modal itself.
          event.preventDefault();
          modal.focus();
          return;
        }
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        const active = document.activeElement;
        const isShift = event.shiftKey;

        if (!modal.contains(active)) {
          // If focus escaped somehow, bring it back inside.
          event.preventDefault();
          (isShift ? last : first).focus();
          return;
        }

        if (!isShift && active === last) {
          event.preventDefault();
          first.focus();
        } else if (isShift && active === first) {
          event.preventDefault();
          last.focus();
        }
      };
      document.addEventListener('keydown', _focusTrapHandler, true);
    }

    function _deactivateFocusTrap() {
      if (!_focusTrapHandler) return;
      document.removeEventListener('keydown', _focusTrapHandler, true);
      _focusTrapHandler = null;
    }

    function _openModal() {
      const modal = _getModal();
      if (!modal) return;

      // Remember the element that had focus before opening
      const active = document.activeElement;
      if (active && active !== document.body && typeof active.focus === 'function') {
        _lastFocused = active;
      } else {
        _lastFocused = null;
      }

      modal.removeAttribute('hidden');
      // Save and restore previous overflow to co-exist with other scroll-lock components
      // (e.g. sidebar.js overlay mode uses the same mechanism with prevBodyOverflow)
      _prevBodyOverflow = document.body.style.overflow;
      document.body.style.overflow = 'hidden';

      // Activate focus trap and move focus inside the modal
      _activateFocusTrap(modal);
      var focusable = _getFocusableElements(modal);
      if (focusable.length) {
        focusable[0].focus();
      } else if (typeof modal.focus === 'function') {
        modal.focus();
      }
    }

    function _closeModal() {
      const modal = _getModal();
      if (!modal) return;

      // Deactivate focus trap before hiding the modal
      _deactivateFocusTrap();

      modal.setAttribute('hidden', '');
      // Restore the overflow that was in place before we locked it
      document.body.style.overflow = _prevBodyOverflow;

      // Restore focus to the element that opened the modal, if possible
      if (_lastFocused && typeof _lastFocused.focus === 'function') {
        _lastFocused.focus();
      }
      _lastFocused = null;
    }

    function _pct(w) {
      // Round to one decimal place (e.g. 0.25 → '25%')
      return (Math.round(w * 1000) / 10) + '%';
    }

    // "满分基准" label and unit per dimension key
    const _DIM_BEST = {
      wa1: { label: '满分风量',    unit: 'CFM' },
      wa2: { label: '满分风量',    unit: 'CFM' },
      wa3: { label: '满分风量',    unit: 'CFM' },
      wb:  { label: '满分风量',    unit: 'CFM' },
      wc:  { label: '满分点赞数',  unit: '赞' },
      wd:  { label: '满分风噪比',  unit: '' },
    };

    // Maximum length for displayed model names before truncation (characters).
    // 12 chars fits comfortably in a ~90px cell at font-size 10px.
    const _MODEL_MAX_LEN = 12;

    function _truncModel(name) {
      if (!name) return '';
      return name.length > _MODEL_MAX_LEN ? name.slice(0, _MODEL_MAX_LEN) + '…' : name;
    }

    function _val(v, unit) {
      if (v === null || v === undefined) return '—';
      // Use non-breaking space (\u00a0) between value and unit to prevent line-breaks mid-label.
      return unit ? v + '\u00a0' + unit : String(v);
    }

    function _escHtml(s) {
      return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    function _renderTable(data) {
      const wrap = _getTableWrap();
      const fnEl = _getFootnote();
      if (!wrap || !fnEl) return;

      const conditions = data.conditions || [];
      const modelLabels = data.model_labels || {};
      const genericFormulas = data.generic_formulas || {};

      if (!conditions.length) {
        wrap.innerHTML = '<p style="color:var(--text-secondary);padding:20px 0">暂无缓存数据，请稍后刷新。</p>';
        fnEl.innerHTML = '';
        return;
      }

      // ── build table ──
      // Layout: first column = dimension info, subsequent columns = one per condition
      let html = '<table class="fc-sr-table"><thead><tr>';
      html += '<th class="fc-sr-dim-cell">评分维度</th>';
      conditions.forEach(c => {
        html += `<th>${_escHtml(c.condition_name)}</th>`;
      });
      html += '</tr></thead><tbody>';

      // Collect all dimension keys in order from the first condition
      const dimKeys = conditions[0].dimensions.map(d => d.key);

      dimKeys.forEach(key => {
        const firstDim = conditions[0].dimensions.find(d => d.key === key);
        const label   = firstDim ? firstDim.label : key;
        const weight  = firstDim ? firstDim.weight : 0;
        const formula = genericFormulas[key] || null;
        const best    = _DIM_BEST[key] || { label: '满分基准', unit: '' };

        // Single combined row per dimension:
        // - Dimension column: name (weight %), then formula on a second line
        // - Each condition column: ref_db on top, 满分 value below
        html += '<tr class="fc-sr-dim-row">';

        // Dimension column
        let dimHtml = `<span class="fc-sr-dim-name">${_escHtml(label)}（权重 ${_pct(weight)}）</span>`;
        if (formula) {
          dimHtml += `<span class="fc-sr-dim-formula">${_escHtml(formula)}</span>`;
        }
        html += `<td class="fc-sr-dim-cell">${dimHtml}</td>`;

        // Condition columns
        conditions.forEach(c => {
          const dim = c.dimensions.find(d => d.key === key) || {};

          // model_id: 满分基准 source model (dimension-dependent; e.g. airflow-at-ref-dB for WA)
          const mid = dim.model_id;
          const modelName = mid !== null && mid !== undefined
            ? (_truncModel(modelLabels[String(mid)]) || `#${mid}`)
            : null;
          const modelTag = modelName
            ? `（<span class="fc-sr-model-tag" title="${_escHtml(modelLabels[String(mid)] || '')}">${_escHtml(modelName)}</span>）`
            : '';

          // ref_db_model_id: the model whose dB bound sets the reference dB value (WA1/WA2 only).
          // WA1: model with the highest min_db sets low_db.
          // WA2: model with the lowest max_db (or anchor model if it clamps high_db higher).
          // WA3: ref_db is a mathematical midpoint — no independent source model.
          const refMid = dim.ref_db_model_id;
          const refModelName = (refMid !== null && refMid !== undefined)
            ? (_truncModel(modelLabels[String(refMid)]) || `#${refMid}`)
            : null;
          const refModelTag = (key === 'wa3' || !refModelName)
            ? ''
            : `（<span class="fc-sr-model-tag" title="${_escHtml(modelLabels[String(refMid)] || '')}">${_escHtml(refModelName)}</span>）`;

          let cellHtml = '';

          // Row 1: reference dB value + ref-dB source model tag (WA1/WA2 only).
          // WA3 has no independent ref-dB source model (it's a computed midpoint).
          if (dim.ref_db !== null && dim.ref_db !== undefined) {
            cellHtml += `<span class="fc-sr-ref-db">${_escHtml(String(dim.ref_db))}\u00a0dB(A)${refModelTag}</span>`;
          }

          // Row 2: 满分基准 value + unit + 满分风量 source model tag.
          if (dim.ref_value !== null && dim.ref_value !== undefined) {
            const valStr = _val(dim.ref_value, best.unit);
            const soloClass = (dim.ref_db !== null && dim.ref_db !== undefined) ? '' : ' fc-sr-best-solo';
            cellHtml += `<span class="fc-sr-best-score${soloClass}"><span class="fc-sr-best-label">${_escHtml(best.label)}</span>${_escHtml(valStr)}${modelTag}</span>`;
          }

          if (!cellHtml) cellHtml = '—';
          html += `<td>${cellHtml}</td>`;
        });

        html += '</tr>';
      });

      html += '</tbody></table>';
      wrap.innerHTML = html;

      // ── build footnote: "评分维度取值逻辑" ──
      const anchorId = data.wa2_anchor_model_id;
      const anchorName = anchorId
        ? (_truncModel(modelLabels[String(anchorId)]) || `#${anchorId}`)
        : null;
      const wa2ClampNote = anchorName
        ? `，并由「${_escHtml(anchorName)}」向上钳制以避免取值偏低`
        : '，不做额外钳制';

      fnEl.innerHTML = `
<h4>评分维度取值逻辑</h4>
<ul>
  <li><strong>低噪点风量</strong>：该工况下所有型号能覆盖到的最低分贝点，即各型号最低分贝值中的最大值；满分风量为该分贝点下表现最优的型号风量。</li>
  <li><strong>高噪点风量</strong>：参考分贝为该工况评分实际使用的高分贝点 high_db。该值从共同覆盖最高点（common_high_db）起算，在存在锚点型号数据时可按锚点型号在该工况下实际可达的更高分贝上调；若该工况缺少锚点型号数据，则保持使用共同最高点${wa2ClampNote}。若某个型号在该工况下无法覆盖该参考分贝，则以其在该工况下可达的最大风量点参与评分；满分风量为参考分贝点（或替代点）下表现最优的型号风量。</li>
  <li><strong>中段风量</strong>：参考分贝为该工况可见评分区间 [low_db, high_db] 的数值中点（(low_db + high_db) / 2）；若某个型号在该工况下无法覆盖该中点分贝，则以其在该工况下可达的最大风量点参与评分；满分风量为参考分贝点（或替代点）下表现最优的型号风量。</li>
  <li><strong>最大风量</strong>：每个风扇在该工况下测得的最大风量（无噪音限制）。</li>
  <li><strong>总点赞数</strong>：每个风扇在该工况下累计获得的总点赞数。</li>
  <li><strong>风噪比</strong>：每个风扇在最高测试转速下的风量与分贝之比（CFM/dB）。</li>
</ul>`;

      // ── build condition composite-weight summary table ──
      const condWeights = data.condition_composite_weights;
      if (condWeights && condWeights.length && conditions.length) {
        // Build a name map from conditions list
        const nameMap = {};
        conditions.forEach(function (c) { nameMap[c.condition_id] = c.condition_name; });

        let wtHtml = '<h4>综合评分工况权重</h4>';
        // Wrap the table in a scrollable div so it can't expand the panel width.
        wtHtml += '<div class="fc-sr-wt-wrap"><table class="fc-sr-weight-table"><thead><tr>';
        condWeights.forEach(function (w) {
          // Use non-breaking space so "工况 N" never breaks across lines in a narrow cell.
          const name = nameMap[w.condition_id] || ('工况\u00a0' + w.condition_id);
          wtHtml += '<th>' + _escHtml(name) + '</th>';
        });
        wtHtml += '</tr></thead><tbody><tr>';
        condWeights.forEach(function (w) {
          wtHtml += '<td>' + _escHtml(String(w.weight_pct)) + '%</td>';
        });
        wtHtml += '</tr></tbody></table></div>';
        fnEl.innerHTML += wtHtml;
      }
    }

    function _loadProfile(profile) {
      if (!PROFILES.includes(profile)) return;
      _currentModalProfile = profile;

      // Update tab UI (class, aria-selected, tabindex)
      document.querySelectorAll('#scoreRuleTabs .fc-sr-tab').forEach(btn => {
        const isActive = btn.dataset.profile === profile;
        btn.classList.toggle('active', isActive);
        btn.setAttribute('aria-selected', isActive ? 'true' : 'false');
        btn.setAttribute('tabindex', isActive ? '0' : '-1');
      });

      const loading = _getLoading();
      const wrap = _getTableWrap();
      const fn = _getFootnote();

      // Use cached data if available
      if (_ruleCache[profile]) {
        if (loading) loading.style.display = 'none';
        if (wrap) wrap.style.display = '';
        _renderTable(_ruleCache[profile]);
        return;
      }

      // Show loading state
      if (loading) { loading.style.display = ''; loading.textContent = '加载中…'; }
      if (wrap) { wrap.innerHTML = ''; wrap.style.display = 'none'; }
      if (fn) fn.innerHTML = '';

      fetch(`/api/score_rule_explain?score_profile=${encodeURIComponent(profile)}`)
        .then(r => {
          if (!r.ok) {
            return r.json().then(body => {
              throw new Error((body && (body.error_message || body.message)) || '请求失败');
            }).catch(() => {
              // If response is not JSON or parsing fails, fall back to generic message
              throw new Error('请求失败');
            });
          }
          return r.json();
        })
        .then(resp => {
          if (!resp.success) {
            throw new Error(resp.error_message || resp.message || '请求失败');
          }
          _ruleCache[profile] = resp.data;
          // Only render if this profile is still selected
          if (_currentModalProfile !== profile) return;
          if (loading) loading.style.display = 'none';
          if (wrap) wrap.style.display = '';
          _renderTable(resp.data);
        })
        .catch(err => {
          if (_currentModalProfile !== profile) return;
          if (loading) {
            loading.textContent = '加载失败，请稍后再试。';
            loading.style.display = '';
          }
          console.warn('[scoreRuleModal] fetch error:', err);
        });
    }

    function _init() {
      const explainBtn = document.getElementById('scoreRuleExplainBtn');
      const modal      = document.getElementById('scoreRuleModal');
      const closeBtn   = document.getElementById('scoreRuleClose');
      const backdrop   = document.getElementById('scoreRuleBackdrop');
      const tabsEl     = document.getElementById('scoreRuleTabs');
      if (!explainBtn || !modal) return;

      explainBtn.addEventListener('click', () => {
        _loadProfile(getProfile());
        _openModal();
      });

      if (closeBtn) closeBtn.addEventListener('click', _closeModal);
      if (backdrop) backdrop.addEventListener('click', _closeModal);

      document.addEventListener('keydown', e => {
        if (e.key === 'Escape' && !modal.hasAttribute('hidden')) _closeModal();
      });

      if (tabsEl) {
        tabsEl.addEventListener('click', e => {
          const tab = e.target.closest('.fc-sr-tab');
          if (!tab) return;
          const p = tab.dataset.profile;
          if (PROFILES.includes(p)) _loadProfile(p);
        });
      }
    }

    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', _init);
    } else {
      _init();
    }
  })();

  window.ScoreProfile = { getProfile, setProfile, onProfileChange, getScoreForItem, getCondScoresForItem, PROFILES };
})();
