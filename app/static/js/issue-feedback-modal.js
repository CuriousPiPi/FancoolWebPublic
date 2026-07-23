/* =============================================================
   issue-feedback-modal.js
   "反馈问题" modal UI logic — two tabs:
     1. 反馈问题: select options + optional free text, submit
     2. 我的反馈: list by model, revoke individual items
   ============================================================= */

(function initIssueFeedbackModal() {
  'use strict';

  // Other-text frontend max length (Chinese UI: 20 chars; future EN UI: 40 chars)
  var OTHER_TEXT_MAX_ZH = 20;

  // ---- DOM helpers ----
  function $id(id) { return document.getElementById(id); }

  // ---- State ----
  var _currentModelId = null;
  var _currentModelLabel = ''; // "品牌 型号"
  var _options = [];           // [{option_id, option_name_zh, is_other}]
  var _selectedOptions = new Set(); // option_id (number)
  var _otherOptionId = null;
  var _lastFocused = null;
  var _prevBodyOverflow = '';
  var _focusTrapBound = false;
  var _myDataDirty = true;    // flag: reload my-feedback tab when opened

  // ---- Getters ----
  function getModal()        { return $id('feedbackModal'); }
  function getTabSubmit()    { return $id('feedbackTabSubmit'); }
  function getTabMy()        { return $id('feedbackTabMy'); }
  function getPaneSubmit()   { return $id('feedbackPaneSubmit'); }
  function getPaneMy()       { return $id('feedbackPaneMy'); }
  function getOptionsLoading(){ return $id('feedbackOptionsLoading'); }
  function getOptionsWrap()  { return $id('feedbackOptionsWrap'); }
  function getOtherWrap()    { return $id('feedbackOtherWrap'); }
  function getOtherInput()   { return $id('feedbackOtherInput'); }
  function getSubmitBtn()    { return $id('feedbackSubmitBtn'); }
  function getSubmitMsg()    { return $id('feedbackSubmitMsg'); }
  function getModelName()    { return $id('feedbackModelName'); }
  function getMyLoading()    { return $id('feedbackMyLoading'); }
  function getMyEmpty()      { return $id('feedbackMyEmpty'); }
  function getMyList()       { return $id('feedbackMyList'); }

  // ---- Focus trap ----
  function getFocusable(root) {
    if (!root) return [];
    return Array.from(root.querySelectorAll(
      'a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])'
    )).filter(function(el) { return el.offsetParent !== null; });
  }

  function _trapKeydown(e) {
    var modal = getModal();
    if (!modal || modal.hasAttribute('hidden') || e.key !== 'Tab') return;
    var list = getFocusable(modal);
    if (!list.length) { e.preventDefault(); modal.focus({ preventScroll: true }); return; }
    var first = list[0], last = list[list.length - 1];
    if (e.shiftKey) {
      if (document.activeElement === first || !modal.contains(document.activeElement)) {
        e.preventDefault(); last.focus({ preventScroll: true });
      }
    } else if (document.activeElement === last || !modal.contains(document.activeElement)) {
      e.preventDefault(); first.focus({ preventScroll: true });
    }
  }

  function activateTrap() {
    if (_focusTrapBound) return;
    document.addEventListener('keydown', _trapKeydown, true);
    _focusTrapBound = true;
  }

  function deactivateTrap() {
    if (!_focusTrapBound) return;
    document.removeEventListener('keydown', _trapKeydown, true);
    _focusTrapBound = false;
  }

  // ---- Open / Close ----
  function openModal(modelId, modelLabel) {
    var modal = getModal();
    if (!modal) return;
    _currentModelId = modelId;
    _currentModelLabel = modelLabel || '';
    _lastFocused = document.activeElement;
    modal.removeAttribute('hidden');
    _prevBodyOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';

    // Reset to submit tab
    _switchTab('submit');

    // Update model name display
    var mn = getModelName();
    if (mn) mn.textContent = _currentModelLabel ? ('当前型号：' + _currentModelLabel) : '';

    // Load options if not yet loaded
    if (!_options.length) {
      _loadOptions();
    } else {
      _renderOptions();
    }

    activateTrap();
    try { (getFocusable(modal)[0] || modal).focus({ preventScroll: true }); } catch(_) {}
  }

  function closeModal() {
    var modal = getModal();
    if (!modal) return;
    modal.setAttribute('hidden', '');
    deactivateTrap();
    document.body.style.overflow = _prevBodyOverflow;
    if (_lastFocused && typeof _lastFocused.focus === 'function') {
      try { _lastFocused.focus(); } catch(_) {}
    }
    _lastFocused = null;
  }

  // ---- Tab switching ----
  function _switchTab(tab) {
    var tabSubmit = getTabSubmit(), tabMy = getTabMy();
    var paneSubmit = getPaneSubmit(), paneMy = getPaneMy();
    if (!tabSubmit || !tabMy || !paneSubmit || !paneMy) return;

    if (tab === 'submit') {
      tabSubmit.classList.add('active'); tabSubmit.setAttribute('aria-selected', 'true'); tabSubmit.tabIndex = 0;
      tabMy.classList.remove('active'); tabMy.setAttribute('aria-selected', 'false'); tabMy.tabIndex = -1;
      paneSubmit.removeAttribute('hidden');
      paneMy.setAttribute('hidden', '');
    } else {
      tabMy.classList.add('active'); tabMy.setAttribute('aria-selected', 'true'); tabMy.tabIndex = 0;
      tabSubmit.classList.remove('active'); tabSubmit.setAttribute('aria-selected', 'false'); tabSubmit.tabIndex = -1;
      paneMy.removeAttribute('hidden');
      paneSubmit.setAttribute('hidden', '');
      if (_myDataDirty) _loadMyFeedback();
    }
  }

  // ---- Options loading / rendering ----
  function _loadOptions() {
    var loading = getOptionsLoading(), wrap = getOptionsWrap();
    if (loading) { loading.style.display = ''; loading.textContent = '加载中…'; }
    if (wrap) { wrap.style.display = 'none'; wrap.innerHTML = ''; }

    fetch('/api/feedback/options')
      .then(function(r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function(resp) {
        if (!resp || !resp.success) throw new Error(resp && resp.error_message || '加载失败');
        _options = (resp.data && resp.data.options) || [];
        // Identify "other" option
        _otherOptionId = null;
        _options.forEach(function(o) {
          if (o.is_other) _otherOptionId = o.option_id;
        });
        if (loading) loading.style.display = 'none';
        _renderOptions();
      })
      .catch(function(err) {
        if (loading) {
          loading.textContent = '加载失败：' + (err && err.message ? err.message : '请稍后再试');
          loading.style.display = '';
        }
      });
  }

  function _renderOptions() {
    var wrap = getOptionsWrap();
    if (!wrap) return;
    wrap.innerHTML = '';
    _selectedOptions = new Set();
    _hideOtherInput();

    _options.forEach(function(opt) {
      var label = document.createElement('label');
      label.className = 'fc-feedback-option-label';

      var cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.className = 'fc-feedback-option-cb';
      cb.dataset.optionId = String(opt.option_id);
      cb.dataset.isOther = opt.is_other ? '1' : '0';

      cb.addEventListener('change', function() {
        var oid = parseInt(this.dataset.optionId, 10);
        if (this.checked) {
          _selectedOptions.add(oid);
        } else {
          _selectedOptions.delete(oid);
        }
        // Show/hide other input
        if (opt.is_other) {
          if (this.checked) _showOtherInput();
          else _hideOtherInput();
        }
      });

      var span = document.createElement('span');
      span.textContent = opt.option_name_zh || '';

      label.appendChild(cb);
      label.appendChild(span);
      wrap.appendChild(label);
    });

    wrap.style.display = '';
    _clearSubmitMsg();
  }

  function _showOtherInput() {
    var ow = getOtherWrap(), inp = getOtherInput();
    if (ow) ow.style.display = '';
    if (inp) {
      inp.maxLength = OTHER_TEXT_MAX_ZH;
      inp.placeholder = '请填写其他问题（最多 ' + OTHER_TEXT_MAX_ZH + ' 字）';
    }
  }

  function _hideOtherInput() {
    var ow = getOtherWrap(), inp = getOtherInput();
    if (ow) ow.style.display = 'none';
    if (inp) inp.value = '';
  }

  // ---- Submit ----
  function _buildSubmitItems() {
    var items = [];
    _selectedOptions.forEach(function(oid) {
      var item = { option_id: oid };
      if (oid === _otherOptionId) {
        var inp = getOtherInput();
        item.other_text = inp ? inp.value.trim().slice(0, OTHER_TEXT_MAX_ZH) : '';
        if (!item.other_text) return; // skip empty other
      }
      items.push(item);
    });
    return items;
  }

  function _clearSubmitMsg() {
    var msg = getSubmitMsg();
    if (msg) { msg.style.display = 'none'; msg.textContent = ''; msg.className = 'fc-feedback-msg'; }
  }

  function _showSubmitMsg(text, type) {
    var msg = getSubmitMsg();
    if (!msg) return;
    msg.textContent = text;
    msg.className = 'fc-feedback-msg fc-feedback-msg--' + (type || 'info');
    msg.style.display = '';
  }

  function _submitFeedback() {
    if (!_currentModelId) {
      _showSubmitMsg('请先选择型号', 'error');
      return;
    }
    var items = _buildSubmitItems();
    if (!items.length) {
      _showSubmitMsg('请至少选择一项问题', 'error');
      return;
    }
    // Validate other text not empty if other is selected
    var otherSelected = _selectedOptions.has(_otherOptionId);
    if (otherSelected && _otherOptionId !== null) {
      var inp = getOtherInput();
      if (inp && !inp.value.trim()) {
        _showSubmitMsg('请填写"其他"问题内容', 'error');
        inp.focus();
        return;
      }
    }

    var btn = getSubmitBtn();
    if (btn) { btn.disabled = true; btn.textContent = '提交中…'; }
    _clearSubmitMsg();

    fetch('/api/feedback/submit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model_id: _currentModelId,
        items: items,
        source_page: 'extra_panel',
      }),
    })
      .then(function(r) {
        if (!r.ok) return r.json().then(function(b) { throw new Error(b && b.error_message || 'HTTP ' + r.status); });
        return r.json();
      })
      .then(function(resp) {
        if (!resp || !resp.success) throw new Error(resp && resp.error_message || '提交失败');
        var d = resp.data || {};
        var created = d.created_count || 0;
        var restored = d.restored_count || 0;
        var dup = d.duplicate_count || 0;

        var parts = [];
        if (created > 0) parts.push('新增 ' + created + ' 条反馈');
        if (restored > 0) parts.push('恢复 ' + restored + ' 条反馈');
        if (dup > 0) parts.push(dup + ' 条已存在');
        var msg = parts.length ? parts.join('，') : '提交成功';
        _showSubmitMsg(msg, 'success');

        // Uncheck all + reset
        _selectedOptions = new Set();
        var allCbs = document.querySelectorAll('#feedbackOptionsWrap .fc-feedback-option-cb');
        allCbs.forEach(function(cb) { cb.checked = false; });
        _hideOtherInput();

        // Mark my-feedback as dirty so it reloads next time
        _myDataDirty = true;
      })
      .catch(function(err) {
        _showSubmitMsg('提交失败：' + (err && err.message ? err.message : '请稍后再试'), 'error');
      })
      .finally(function() {
        if (btn) { btn.disabled = false; btn.textContent = '提交反馈'; }
      });
  }

  // ---- My Feedback ----
  function _loadMyFeedback() {
    var loading = getMyLoading(), empty = getMyEmpty(), list = getMyList();
    if (loading) { loading.style.display = ''; loading.textContent = '加载中…'; }
    if (empty) empty.style.display = 'none';
    if (list) list.innerHTML = '';
    _myDataDirty = false;

    fetch('/api/feedback/my')
      .then(function(r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function(resp) {
        if (!resp || !resp.success) throw new Error(resp && resp.error_message || '加载失败');
        if (loading) loading.style.display = 'none';
        var groups = (resp.data && resp.data.groups) || [];
        if (!groups.length) {
          if (empty) empty.style.display = '';
          return;
        }
        _renderMyFeedback(groups);
      })
      .catch(function(err) {
        if (loading) {
          loading.textContent = '加载失败：' + (err && err.message ? err.message : '请稍后再试');
          loading.style.display = '';
        }
      });
  }

  function _renderMyFeedback(groups) {
    var list = getMyList();
    if (!list) return;
    list.innerHTML = '';

    groups.forEach(function(group) {
      var groupEl = document.createElement('div');
      groupEl.className = 'fc-feedback-my-group';

      var header = document.createElement('div');
      header.className = 'fc-feedback-my-group-header';
      var brand = group.brand_name_zh || '';
      var model = group.model_name || '';
      header.textContent = [brand, model].filter(Boolean).join(' ') || '未知型号';
      groupEl.appendChild(header);

      (group.feedbacks || []).forEach(function(fb) {
        var row = document.createElement('div');
        row.className = 'fc-feedback-my-row';
        row.dataset.feedbackId = String(fb.feedback_id);

        var label = document.createElement('span');
        label.className = 'fc-feedback-my-label';
        var text = fb.option_name_zh || '';
        if (fb.is_other && fb.other_text) text += '：' + fb.other_text;
        label.textContent = text;

        var revokeBtn = document.createElement('button');
        revokeBtn.type = 'button';
        revokeBtn.className = 'fc-feedback-revoke-btn';
        revokeBtn.setAttribute('aria-label', '撤回该条反馈');
        revokeBtn.innerHTML = '&times;';
        revokeBtn.dataset.feedbackId = String(fb.feedback_id);

        revokeBtn.addEventListener('click', function() {
          _revokeFeedback(parseInt(this.dataset.feedbackId, 10), row);
        });

        row.appendChild(label);
        row.appendChild(revokeBtn);
        groupEl.appendChild(row);
      });

      list.appendChild(groupEl);
    });
  }

  function _revokeFeedback(feedbackId, rowEl) {
    var btn = rowEl && rowEl.querySelector('.fc-feedback-revoke-btn');
    if (btn) { btn.disabled = true; btn.textContent = '…'; }

    fetch('/api/feedback/revoke', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ feedback_id: feedbackId }),
    })
      .then(function(r) {
        if (!r.ok) return r.json().then(function(b) { throw new Error(b && b.error_message || 'HTTP ' + r.status); });
        return r.json();
      })
      .then(function(resp) {
        if (!resp || !resp.success) throw new Error(resp && resp.error_message || '撤回失败');
        if (rowEl) {
          rowEl.classList.add('fc-feedback-my-row--revoked');
          setTimeout(function() {
            if (rowEl.parentNode) rowEl.parentNode.removeChild(rowEl);
            _cleanupEmptyGroups();
          }, 300);
        }
        // My-feedback list is updated in-place; no need to reload when switching tabs
        _myDataDirty = false;
        if (typeof window.showInfo === 'function') window.showInfo('已撤回该条反馈');
      })
      .catch(function(err) {
        if (btn) { btn.disabled = false; btn.innerHTML = '&times;'; }
        if (typeof window.showInfo === 'function') window.showInfo('撤回失败：' + (err && err.message ? err.message : '请稍后再试'));
      });
  }

  function _cleanupEmptyGroups() {
    var list = getMyList(), empty = getMyEmpty();
    if (!list) return;
    var groups = list.querySelectorAll('.fc-feedback-my-group');
    groups.forEach(function(g) {
      var rows = g.querySelectorAll('.fc-feedback-my-row');
      if (!rows.length) g.parentNode.removeChild(g);
    });
    var remaining = list.querySelectorAll('.fc-feedback-my-group');
    if (!remaining.length && empty) empty.style.display = '';
  }

  // ---- Init ----
  function init() {
    var modal = getModal();
    if (!modal) return;

    var openBtn = $id('feedbackOpenBtn');
    var closeBtn = $id('feedbackClose');
    var backdrop = $id('feedbackBackdrop');
    var tabSubmit = getTabSubmit();
    var tabMy = getTabMy();
    var submitBtn = getSubmitBtn();

    if (closeBtn) closeBtn.addEventListener('click', closeModal);
    if (backdrop) backdrop.addEventListener('click', closeModal);

    document.addEventListener('keydown', function(e) {
      if (e.key === 'Escape' && modal && !modal.hasAttribute('hidden')) closeModal();
    });

    if (tabSubmit) {
      tabSubmit.addEventListener('click', function() { _switchTab('submit'); });
    }
    if (tabMy) {
      tabMy.addEventListener('click', function() { _switchTab('my'); });
    }
    if (submitBtn) {
      submitBtn.addEventListener('click', _submitFeedback);
    }

    // Wire open button to extra panel's current model
    if (openBtn) {
      openBtn.addEventListener('click', function() {
        // Resolve current model from extra panel
        var modelId = null;
        var modelLabel = '';
        try {
          if (window.__feedbackCurrentModelId) {
            modelId = window.__feedbackCurrentModelId;
            modelLabel = window.__feedbackCurrentModelLabel || '';
          }
        } catch(_) {}
        if (!modelId) {
          if (typeof window.showInfo === 'function') window.showInfo('请先加载一个型号');
          return;
        }
        openModal(modelId, modelLabel);
      });
    }
  }

  // ---- Public API ----
  window.IssueFeedbackModal = {
    init: init,
    open: openModal,
    close: closeModal,
    /** Called by fancool.js whenever the extra panel model changes. */
    setCurrentModel: function(modelId, modelLabel) {
      _currentModelId = modelId;
      _currentModelLabel = modelLabel || '';
      window.__feedbackCurrentModelId = modelId;
      window.__feedbackCurrentModelLabel = modelLabel || '';

      // Show/hide the open button
      var btn = $id('feedbackOpenBtn');
      if (btn) {
        if (modelId) btn.removeAttribute('hidden');
        else btn.setAttribute('hidden', '');
      }

      // If modal is open on submit tab, update the model name display
      var modal = getModal();
      if (modal && !modal.hasAttribute('hidden')) {
        var mn = getModelName();
        if (mn) mn.textContent = modelLabel ? ('当前型号：' + modelLabel) : '';
      }
    },
  };

  // Auto-init after DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
