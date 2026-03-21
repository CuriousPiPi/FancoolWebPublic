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

  window.ScoreProfile = { getProfile, setProfile, onProfileChange, getScoreForItem, getCondScoresForItem, PROFILES };
})();
