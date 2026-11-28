(function (window, document) {
  'use strict';

  const en = () => !!window.FcI18n?.isEnglish?.();
  const text = (zh, english) => en() ? english : zh;
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
  const defaults = () => ({ scenario: 'any', intake: 'any', radiator: 'any', tower: 'any', size: 'any', thickness: 'any', speed: 'any', budget: 'any', color: 'any', rgb: 'any', locations: [] });
  let answers = defaults();
  let metadata = null;
  let results = [];
  let selected = new Set();
  let step = 0;
  let view = 'entry';
  let busy = false;
  let pendingRequest = null;
  let notice = '';
  const skipEntryKey = 'fc_guided_skip_entry_v2';
  const capacity = () => Number(window.APP_CONFIG?.maxItems) || 8;
  let skipEntry = window.__FC_SHOW_GUIDED_ENTRY === false;
  const dialog = document.getElementById('guidedSelectionDialog');
  const content = document.getElementById('guidedSelectionContent');
  const openButton = document.getElementById('guidedSelectionOpen');
  const hint = document.getElementById('guidedRadarHint');
  if (!dialog || !content || !openButton || !hint) return;

  function option(value, zh, english, zhDetail, enDetail) {
    return { value, label: text(zh, english), detail: text(zhDetail, enDetail) };
  }
  function steps() {
    const list = [{
      key: 'scenario', label: text('使用场景', 'Use'),
      question: text('你计划把风扇用在哪里？', 'Where will you use the fan?'),
      detail: text('选择最接近的用途，以便了解应参考的测试工况。', 'Choose the closest use to learn which test conditions to consider.'),
      tip: text('测试使用特定装置，实际机箱或散热器表现可能不同。', 'Tests use specific fixtures; results may differ in your own system.'),
      options: [
        option('intake', '机箱进风', 'Case intake', '参考进气工况', 'Consider intake'),
        option('exhaust', '机箱排风', 'Case exhaust', '参考排气工况', 'Consider exhaust'),
        option('radiator', '水冷冷排换扇', 'Liquid radiator', '选择吹吸方向', 'Choose push or pull'),
        option('tower', '风冷散热器换扇', 'Air cooler', '选择塔体类型', 'Choose cooler type'),
        option('any', '不确定 / 多种用途', 'Not sure / multiple uses', '先看综合表现', 'Start with overall performance')
      ]
    }];
    if (answers.scenario === 'intake') list.push({
      key: 'intake', label: text('面板距离', 'Panel clearance'),
      question: text('风扇安装面与机箱面板距离有多大（不包括纱网）？', 'How far is the fan mounting surface from the case panel（Mesh not included）?'),
      detail: text('距离较近时，进气遮挡可能引起啸叫。', 'Nearby obstructions can cause intake whistle.'),
      tip: text('不确定时不限制啸叫等级，请结合实际安装空间判断。', 'If unsure, whistle level is not restricted.'),
      options: [option('near', '1 cm 及以内', '1 cm or less', '仅显示低啸叫，可能导致筛选结果较少', 'Low whistle only, may result in fewer results'),
        option('far', '大于 1 cm', 'Over 1 cm', '不限制啸叫', 'No whistle limit'),
        option('any', '不确定', 'Not sure', '不限制啸叫', 'No whistle limit')]
    });
    if (answers.scenario === 'radiator') list.push({
      key: 'radiator', label: text('冷排方向', 'Radiator direction'),
      question: text('风扇如何向冷排送风？', 'How does air pass through the radiator?'),
      detail: text('根据风扇与冷排的位置选择；不确定可以跳过。', 'Choose the fan position, or skip if unsure.'),
      tip: text('测试使用 27 mm 冷排，另一侧加装进气面板。', 'Tests use a 27 mm radiator with an intake panel on the other side.'),
      options: [option('push', '吹冷排', 'Push', '风扇向冷排吹风', 'Fan blows into radiator'), option('pull', '吸冷排', 'Pull', '风扇从冷排吸风，排除高啸叫型号', 'Fan draws air through radiator; excludes high-whistle models'), option('any', '不确定', 'Not sure', '比较两种工况', 'Compare both conditions')]
    });
    if (answers.scenario === 'tower') list.push({
      key: 'tower', label: text('风冷塔体', 'Air cooler'),
      question: text('使用哪种风冷散热器？', 'What kind of air cooler?'),
      detail: text('目前只测试了常规厚度单塔，其他塔体只能近似参考。', 'Only a standard single tower was tested; other designs are approximate.'),
      tip: text('厚单塔指塔体厚度超过 70 mm；双塔和厚单塔均为估算，并非实测。', 'Thick single towers exceed 70 mm. Dual and thick towers are estimates, not direct tests.'),
      options: [option('normal', '常规厚度单塔', 'Standard single tower', '测试塔体厚度 55 mm', 'Tested tower thickness: 55 mm'),
        option('dual_single', '双塔单扇', 'Dual tower, one fan', '阻力略大于测试工况，可大致参考测试数据', 'Slightly higher resistance than tested; results are an approximate reference'),
        option('dual_double', '双塔双扇', 'Dual tower, two fans', '阻力略小于测试工况，可大致参考测试数据', 'Slightly lower resistance than tested; results are an approximate reference'),
        option('thick_single', '厚单塔单扇', 'Thick single tower, one fan', '阻力较大，结合吹冷排数据加权计算', 'Higher resistance; weighted with push-radiator data'),
        option('thick_double', '厚单塔双扇', 'Thick single tower, two fans', '基于常规风冷测试，排除高啸叫型号', 'Standard air-cooler tests, excluding high-whistle models'),
        option('any', '不确定', 'Not sure', '查看风冷表现', 'See air-cooler performance')]
    });
    list.push({
      key: 'size', label: text('安装尺寸', 'Size'),
      question: text('需要什么尺寸的风扇？', 'What fan size do you need?'),
      detail: text('请先确认机箱或散热器的安装孔位。', 'Check the mounting size of your case or cooler.'),
      tip: text('能否安装仍需以设备说明为准。', 'Check your equipment specifications for compatibility.'),
      options: [...(metadata?.sizes || []).map(s => option(String(s.value), `${s.label} mm`, `${s.label} mm`, '安装尺寸', 'Mounting size')), option('any', '不确定 / 不限', 'Not sure / any', '不按尺寸筛选', 'Do not filter by size')]
    }, {
      key: 'thickness', label: text('厚度空间', 'Thickness'),
      question: text('安装空间有厚度限制吗？', 'Is there a thickness limit?'),
      detail: text('请确认与机箱、冷排及周围硬件之间的空间。', 'Check clearance around the case, radiator and other hardware.'),
      tip: text('以上为最大厚度，包含所选数值。', 'The selected maximum thickness is inclusive.'),
      options: [20, 25].map(n => option(String(n), `最多 ${n} mm`, `Up to ${n} mm`, `${n} mm 及以下`, `${n} mm or less`)).concat([option('any', '不限', 'Any', '默认最多 50 mm', 'Default maximum 50 mm')])
    }, {
      key: 'budget', label: text('预算范围', 'Budget'),
      question: text('单把风扇的预算大概是？', 'What is your budget per fan?'),
      detail: text('按参考价格筛选，实际成交价格可能不同。', 'Filter by reference price; actual prices may differ.'),
      tip: text('价格未知按 0 元参与筛选，结果中标注价格未知。', 'Unknown prices are treated as zero for filtering and marked as unknown.'),
      options: [50, 100, 150].map(n => option(String(n), `${n} 元以内`, `Up to ¥${n}`, `含 ${n} 元`, `Including ¥${n}`)).concat([option('any', '不限', 'Any', '不按价格筛选', 'Do not filter by price')])
    }, {
      key: 'color', label: text('颜色偏好', 'Color'),
      question: text('喜欢什么颜色的风扇？', 'What color fan do you prefer?'),
      detail: text('选择型号提供的颜色。', 'Choose an available model color.'),
      tip: text('其它 / 不限包含所有颜色。', 'Other / any includes all colors.'),
      options: [option('black', '黑色', 'Black', '包含黑色款', 'Available in black'),
        option('white', '白色', 'White', '包含白色款', 'Available in white'),
        option('any', '其它 / 不限', 'Other / any', '不限颜色', 'Any color')]
    }, {
      key: 'rgb', label: text('灯光偏好', 'Lighting'),
      question: text('需要 RGB 灯光吗？', 'Do you want RGB lighting?'),
      detail: text('灯光偏好不影响性能评分。', 'Lighting does not affect performance scores.'),
      tip: text('灯效控制方式与接口兼容性需要额外确认。', 'Check lighting controls and connector compatibility separately.'),
      options: [option('yes', '需要 RGB', 'RGB', '下一步可选灯光位置', 'Choose lighting positions next'), option('no', '不要 RGB', 'No RGB', '筛选无灯款', 'Only unlit fans'), option('any', '不确定 / 不限', 'Not sure / any', '不按灯光筛选', 'Do not filter by lighting')]
    });
    if (['120', '140'].includes(answers.size)) list.splice(list.findIndex(item => item.key === 'budget'), 0, {
      key: 'speed', label: text('转速偏好', 'Speed preference'),
      question: text('你更看重风扇极限性能还是静音省心？', 'Do you prefer peak performance or effortless quiet?'),
      detail: text('根据风扇最高转速筛选，选择更适合你的使用方式。', 'Filter by maximum fan speed to suit how you use your fan.'),
      tip: text('实际噪音和性能因型号、尺寸及安装环境而异；调速还需设备支持。最高转速未知的型号不会显示。', 'Actual noise and performance vary by model, size, and installation. Speed control requires compatible equipment. Fans with unknown maximum speed are excluded.'),
      options: [
        option('performance', '性能优先', 'Performance first', '最高转速较高，性能潜力更大，但满速噪音通常也更大；适合追求极限性能或愿意自行调速。', 'Higher maximum speed can offer greater performance, but is usually louder at full speed. Suitable for peak performance or manual speed control.'),
        option('quiet', '省心静音', 'Effortless quiet', '最高转速较低，满速通常更安静，但性能上限略低；适合不想手动调速。', 'Lower maximum speed is usually quieter at full speed, but may limit peak performance. Suitable if you prefer not to adjust speeds.'),
        option('any', '不确定 / 不限', 'Not sure / any', '不按最高转速筛选', 'Do not filter by maximum speed')
      ]
    });
    if (answers.rgb === 'yes') list.push({
      key: 'locations', label: text('灯光位置', 'Lighting positions'), multi: true,
      question: text('喜欢哪些位置有灯光？', 'Which lighting positions do you prefer?'),
      detail: text('可多选，符合任意一个位置即可；不选则不限。', 'Select any number. A match at any selected position is enough; select none for all.'),
      tip: text('多选为“任一匹配”，不要求同时具备所有位置。', 'Multiple selections match any position, not necessarily all.'),
      options: (metadata?.rgb_flags || []).filter(r => Number(r.value) > 0).map(r => ({
        value: String(r.value), label: en() ? (r.label_en || r.label_zh) : (r.label_zh || r.label_en)
      }))
    });
    return list;
  }
  function recommendation() {
    if (answers.scenario === 'intake') return text('建议参考进气工况，实际机箱环境可能不同。', 'Consider intake tests; your case may differ.');
    if (answers.scenario === 'exhaust') return text('建议参考排气工况，实际机箱环境可能不同。', 'Consider exhaust tests; your case may differ.');
    if (answers.scenario === 'radiator') {
      if (answers.radiator === 'push') return text('建议参考吹冷排工况。', 'Consider radiator push tests.');
      if (answers.radiator === 'pull') return text('建议参考吸冷排工况。', 'Consider radiator pull tests.');
      return text('方向未确定，先显示综合表现；建议分别查看吹冷排和吸冷排。', 'Direction is unknown, so overall performance is shown; compare push and pull separately.');
    }
    if (answers.scenario === 'tower') {
      if (answers.tower === 'thick_single') return text('厚单塔估算使用风冷 80% + 吹冷排 20%，并非实测。高级搜索仅按风冷排序，结果可能不同。', 'Thick tower estimate: 80% air cooler + 20% push radiator, not a direct test. Advanced search uses air cooler only and may differ.');
      if (answers.tower.startsWith('dual') || answers.tower === 'thick_double') return text('以风冷工况近似参考，并非实测。', 'An approximation using air-cooler tests, not a direct measurement.');
      if (answers.tower === 'any') return text('塔体未确定，显示风冷工况表现；请核对散热器规格。', 'Cooler type is unknown; air-cooler test results are shown. Check the specifications.');
      return text('建议参考风冷工况；测试使用常规厚度单塔。', 'Consider air-cooler tests using a standard single tower.');
    }
    return text('用途未确定，可先看综合表现，再选择工况。', 'Start with overall performance, then choose a condition.');
  }
  function payload() {
    let scenario = answers.scenario;
    if (scenario === 'radiator') scenario = answers.radiator === 'any' ? 'any' : `radiator_${answers.radiator}`;
    if (scenario === 'tower') scenario = `tower_${answers.tower === 'any' ? 'normal' : answers.tower}`;
    const p = { scenario };
    if (answers.scenario === 'intake' && answers.intake === 'near') p.whistle = 'low';
    if (answers.scenario === 'radiator' && answers.radiator === 'pull') p.whistle = 'medium';
    if (answers.scenario === 'tower' && answers.tower === 'thick_double') p.whistle = 'medium';
    if (answers.size !== 'any') p.size_values = [answers.size];
    if (answers.thickness !== 'any') p.thickness_max = Number(answers.thickness);
    if (['120', '140'].includes(answers.size) && answers.speed !== 'any') p.speed_preference = answers.speed;
    if (answers.budget !== 'any') p.price_max = Number(answers.budget);
    if (answers.color === 'black') p.color_mask = 1;
    if (answers.color === 'white') p.color_mask = 2;
    if (answers.rgb === 'no') p.rgb_include_none = true;
    if (answers.rgb === 'yes') {
      const positions = answers.locations.length ? answers.locations : metadata.rgb_flags.filter(r => Number(r.value) > 0).map(r => String(r.value));
      p.rgb_mask = positions.reduce((mask, value) => mask | Number(value), 0);
      if (!p.rgb_mask) throw new Error(text('未能获取 RGB 类型，请重试。', 'RGB options are unavailable. Please retry.'));
    }
    return p;
  }
  async function request(url, options) {
    let response;
    try { response = await fetch(url, options); }
    catch (error) { throw new Error(text('网络连接失败，请重试。', 'Network error. Please retry.'), { cause: error }); }
    let body;
    try { body = await response.json(); }
    catch (error) { throw new Error(text('服务器响应无效，请重试。', 'Invalid server response. Please retry.'), { cause: error }); }
    const normalized = window.normalizeApiResponse(body);
    if (!response.ok || !normalized.ok) throw new Error(normalized.message || normalized.error_message || text('请求失败，请重试。', 'Request failed. Please retry.'));
    return normalized.data;
  }
  const bladePath = 'M4.7390546 -20.4582834 A36 36 0 0 0 -19.7034062 -52.1226993 A1 1 0 0 1 -19.7277897 -54.0098539 A57.5 57.5 0 0 1 15.0136233 -55.5053251 A5 5 0 0 1 17.9049967 -53.3964913 A36 36 0 0 1 16.7806576 -12.6257487 A21 21 0 0 0 4.7390546 -20.4582834 Z';
  const padPath = 'M-54 -59 H-40.9511905 A1 1 0 0 1 -40.3744131 -57.1830986 A70 70 0 0 0 -57.1830986 -40.3744131 A1 1 0 0 1 -59 -40.9511905 V-54 A5 5 0 0 1 -54 -59 Z';
  const circlePath = (x, y, r) => `M${x - r} ${y} a${r} ${r} 0 1 0 ${r * 2} 0 a${r} ${r} 0 1 0 ${-r * 2} 0 Z`;
  const roundedSquare = (half, r) => `M${-half + r} ${-half} H${half - r} A${r} ${r} 0 0 1 ${half} ${-half + r} V${half - r} A${r} ${r} 0 0 1 ${half - r} ${half} H${-half + r} A${r} ${r} 0 0 1 ${-half} ${half - r} V${-half + r} A${r} ${r} 0 0 1 ${-half + r} ${-half} Z`;
  let fanLightId = 0;
  function fanArtwork(position = '', logo = false) {
    // Keep the 179-unit footprint while insetting all internal geometry by 5%.
    const scale = (179 / 120) * 0.95;
    const placement = `translate(100 100) scale(${scale})`;
    const frameFace = roundedSquare(59.2, 5.2);
    const outlineInset = 179 / (2 * scale) - 59.2;
    const frameOuter = roundedSquare(59.2 + outlineInset, 5.2 + outlineInset);
    const corners = [[1, 1], [-1, 1], [1, -1], [-1, -1]];
    const holes = corners.map(([x, y]) => circlePath(-52.5 * x, -52.5 * y, 2.5)).join(' ');
    const pads = corners.map(([x, y]) => `<path class="pad" fill-rule="evenodd"
      transform="scale(${x} ${y})" d="${padPath} ${circlePath(-52.5, -52.5, 2.5)}"/>`).join('');
    const blades = Array.from({ length: 7 }, (_, i) =>
      `<path class="blade" d="${bladePath}" transform="rotate(${i * 360 / 7})"/>`).join('');
    const maskBlades = position === 'blade' ? Array.from({ length: 7 }, (_, i) =>
      `<path fill="white" d="${bladePath}" transform="rotate(${i * 360 / 7})"/>`).join('') : '';
    const shape = position === 'blade' ? `<g class="fc-guided-rotor"><g transform="${placement}">${maskBlades}</g></g>`
      : position === 'hub' ? `<g transform="${placement}"><path fill="white" fill-rule="evenodd" d="${circlePath(0, 0, 20.3)} ${circlePath(0, 0, 16.8)}"/></g>`
      : `<g transform="${placement}"><path fill="white" fill-rule="evenodd" d="${frameOuter} ${frameFace}"/></g>`;
    const maskId = position ? `guided-light-${position}-${++fanLightId}` : '';
    const mask = position ? `<defs><mask id="${maskId}" maskUnits="userSpaceOnUse" x="0" y="0" width="200" height="200">${shape}</mask></defs>` : '';
    const light = position ? `<foreignObject class="fc-guided-illuminated" x="0" y="0" width="200" height="200" mask="url(#${maskId})"><div xmlns="http://www.w3.org/1999/xhtml" class="fc-guided-wheel-window"><div class="fc-guided-color-wheel"></div></div></foreignObject>` : '';
    const hub = `<g transform="${placement}"><circle class="hub-outline" r="22.5"/><circle class="hub" r="21.1"/></g>
      ${position === 'hub' ? light : ''}<g transform="${placement}"><circle class="hub-core" r="16"/></g>`;
    return `${mask}<g transform="${placement}">
        <path class="frame" fill-rule="evenodd" d="${frameOuter} ${circlePath(0, 0, 58)} ${holes}"/>
        <path class="interior" fill-rule="evenodd" d="${frameFace} ${circlePath(0, 0, 59)} ${holes}"/>
        <path class="inner" fill-rule="evenodd" d="${circlePath(0, 0, 59)} ${circlePath(0, 0, 58)}"/>
      </g>${position === 'frame' ? light : ''}<g transform="${placement}">${pads}</g>
      <g class="${logo ? 'fc-guided-logo-rotor' : 'fc-guided-rotor'}"><g transform="${placement}">${blades}</g>${logo ? hub : ''}</g>
      ${position === 'blade' ? light : ''}${logo ? '' : hub}`;
  }
  function logoSvg() {
    return `<svg class="fc-guided-logo" viewBox="0 0 200 200" aria-hidden="true">${fanArtwork('', true)}</svg>`;
  }
  function fanSvg(position = '', dimension = '') {
    const fan = fanArtwork(position);
    if (!dimension) return `<svg viewBox="0 0 200 200" aria-hidden="true">${fan}</svg>`;
    const label = escape(dimension);
    return `<svg viewBox="0 0 260 250" role="img" aria-label="${label} × ${label} mm">
      <defs><marker id="guided-front-arrow" markerWidth="7" markerHeight="7" refX="3.5" refY="3.5" orient="auto-start-reverse"><path d="M7 0 L0 3.5 L7 7 Z" fill="#2768dc"/></marker></defs>
      <g transform="translate(52 30) scale(.82)">${fan}</g>
      <path class="extension" d="M60.6 194 V218 M207.4 194 V218 M30 38.6 H54 M30 185.4 H54"/>
      <path class="dimension" d="M60.6 207 H207.4 M37 38.6 V185.4" marker-start="url(#guided-front-arrow)" marker-end="url(#guided-front-arrow)"/>
      <text class="dim-text" x="134" y="230" text-anchor="middle">${label} mm</text>
      <text class="dim-text" x="19" y="112" text-anchor="middle" transform="rotate(-90 19 112)">${label} mm</text></svg>`;
  }
  function sideSvg(thickness) {
    const width = Number(thickness) * 146.8 / 120, left = 130 - width / 2, right = 130 + width / 2;
    return `<svg viewBox="0 0 260 250" role="img" aria-label="${escape(thickness)} mm">
      <defs><marker id="guided-side-arrow" markerWidth="7" markerHeight="7" refX="3.5" refY="3.5" orient="auto-start-reverse"><path d="M7 0 L0 3.5 L7 7 Z" fill="#2768dc"/></marker>
      <linearGradient id="guided-side-shading" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="var(--guided-frame)"/><stop offset=".5" stop-color="var(--guided-fan)"/><stop offset="1" stop-color="var(--guided-frame)"/></linearGradient></defs>
      <rect class="side-outline" x="${left}" y="38.6" width="${width}" height="146.8" rx="3"/>
      <path class="side-rail" d="M${left} 39 h${width} v13 h-${width} Z M${left} 172.4 h${width} v13 h-${width} Z"/>
      <rect x="${left + 5}" y="54" width="${width - 10}" height="116" fill="url(#guided-side-shading)"/>
      <path class="side-detail" d="M${left} 54 H${right} M${left} 170 H${right} M${left + 5} 57 V167 M${right - 5} 57 V167"/>
      <path class="extension" d="M${left} 194 V218 M${right} 194 V218"/>
      <path class="dimension" d="M${left} 207 H${right}" marker-start="url(#guided-side-arrow)" marker-end="url(#guided-side-arrow)"/>
      <text class="dim-text" x="130" y="230" text-anchor="middle">${escape(thickness)} mm</text></svg>`;
  }
  function stars(score) {
    if (score == null || !Number.isFinite(Number(score))) return '—';
    const halves = Math.round(Math.max(0, Math.min(100, Number(score))) / 10);
    return `<span class="fc-guided-stars" aria-label="${halves / 2} / 5">${Array.from({ length: 5 }, (_, i) =>
      `<span class="fc-guided-star ${halves >= (i + 1) * 2 ? 'full' : halves === i * 2 + 1 ? 'half' : ''}">★</span>`).join('')}</span>`;
  }
  const resultThumbCache = new Map();
  function loadResultThumbs() {
    const nodes = content.querySelectorAll?.('img[data-guided-thumb]') || [];
    const load = img => {
      const modelId = img.dataset.guidedThumb;
      if (!resultThumbCache.has(modelId)) {
        const metadata = fetch(`/api/gallery/${encodeURIComponent(modelId)}`).then(r => {
          if (!r.ok) throw new Error(`gallery ${r.status}`);
          return r.json();
        }).catch(error => {
          resultThumbCache.delete(modelId);
          throw error;
        });
        resultThumbCache.set(modelId, metadata);
      }
      resultThumbCache.get(modelId).then(json => {
        const data = json.data || {}, fallback = data.items?.[0]?.thumb_url;
        const preferred = img.dataset.rgb === '1' ? data.lighting_preferred_thumb_url : null;
        if (preferred || fallback) {
          img.onload = () => { img.hidden = false; };
          img.onerror = () => { img.onerror = null; if (preferred && fallback && preferred !== fallback) img.src = fallback; };
          img.src = preferred || fallback;
        }
      }).catch(error => console.warn('[GuidedSelection] Thumbnail unavailable:', error));
    };
    if (!window.IntersectionObserver) { nodes.forEach(load); return; }
    const observed = new Map();
    const observer = new window.IntersectionObserver(entries => entries.forEach(entry => {
      if (entry.isIntersecting) { observer.unobserve(entry.target); load(observed.get(entry.target)); }
    }), { root: content.querySelector('.fc-guided-scroll'), rootMargin: '100px' });
    resultThumbObserver = observer;
    nodes.forEach(node => {
      const target = node.hidden ? node.parentElement : node;
      observed.set(target, node);
      observer.observe(target);
    });
  }
  let resultThumbObserver = null;
  function shell(body, actions = '', progress = '') {
    resultThumbObserver?.disconnect();
    resultThumbObserver = null;
    content.innerHTML = `<div class="fc-guided-top"><strong>${logoSvg()} <span class="fc-guided-brand-title">${escape(text('风扇库', 'FanCool DB'))}</span></strong><button type="button" data-action="close" aria-label="${escape(text('关闭', 'Close'))}">×</button></div>${progress}<div class="fc-guided-scroll">${notice ? `<p class="fc-guided-notice" role="alert">${escape(notice)}</p>` : ''}${body}</div>${actions}`;
  }
  let finishOpening = () => {};
  function render(focusTitle = false) {
    finishOpening();
    openButton.textContent = text('引导式筛选', 'Guided selection');
    hint.querySelector('button').setAttribute('aria-label', text('关闭', 'Close'));
    if (view === 'entry') {
      shell(`<div class="fc-guided-entry"><p class="fc-guided-kicker">WELCOME TO FANCOOL</p><h2 id="guidedSelectionTitle" tabindex="-1" autofocus>${escape(text('找到适合你的风扇', 'Find the right fan'))}</h2><p>${escape(text('选择适合你的方式开始。', 'Choose how you would like to begin.'))}</p><div class="fc-guided-entry-grid"><div class="fc-guided-expert"><button type="button" data-action="expert"><svg class="fc-guided-entry-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"><path d="M4 6h16M4 12h16M4 18h16"/><circle cx="8" cy="6" r="2" fill="var(--bg-secondary, #fff)"/><circle cx="16" cy="12" r="2" fill="var(--bg-secondary, #fff)"/><circle cx="10" cy="18" r="2" fill="var(--bg-secondary, #fff)"/></svg><strong>${escape(text('我是风扇老烧', 'I know what I need'))}</strong><span>${escape(text('直接进入现有对比工作台', 'Go directly to the comparison workspace'))}</span></button><label><input type="checkbox" data-skip-entry ${skipEntry ? 'checked' : ''}>${escape(text('以后不再显示，直接进入工作台', 'Don’t show this again; go directly to the workspace'))}</label></div><button type="button" data-action="start"><svg class="fc-guided-entry-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="m15.5 8.5-2 5-5 2 2-5z"/></svg><strong>${escape(text('我是萌新，帮我选（测试版）', 'Help me choose (beta)'))}</strong><span>${escape(text('回答几个问题，筛选适合的型号', 'Answer a few questions to narrow the options'))}</span></button></div></div>`);
    } else if (view === 'wizard') {
      const visible = steps(), current = visible[step];
      const opts = current.options.map(o => {
        const checked = current.multi ? answers[current.key].includes(o.value) : answers[current.key] === o.value;
        const position = current.key === 'locations' ? ({ '1': 'blade', '2': 'hub', '4': 'frame' })[o.value] : null;
        return `<button type="button" class="fc-guided-option${checked ? ' is-selected' : ''}" data-choice="${escape(o.value)}" aria-pressed="${checked}">${position ? `<span class="fc-guided-art">${fanSvg(position)}</span>` : ''}<strong>${escape(o.label)}</strong>${o.detail ? `<small>${escape(o.detail)}</small>` : ''}</button>`;
      }).join('');
      const illustration = current.key === 'size' ? fanSvg('', answers.size === 'any' ? '120' : answers.size)
        : current.key === 'thickness' ? sideSvg(answers.thickness === 'any' ? '25' : answers.thickness) : '';
      shell(`<div class="fc-guided-body"><p class="fc-guided-kicker">STEP ${step + 1} / ${visible.length} · ${escape(current.label)}</p><h2 id="guidedSelectionTitle" tabindex="-1">${escape(current.question)}</h2><p>${escape(current.detail)}</p>${illustration ? `<div class="fc-guided-dimension-art">${illustration}</div>` : ''}<div class="fc-guided-options${current.key === 'locations' ? ' fc-guided-locations' : ''}">${opts}</div><p class="fc-guided-tip">${escape(current.tip)}</p></div>`,
        `<div class="fc-guided-actions"><button type="button" data-action="back">${escape(text('← 上一步', '← Back'))}</button><div><button type="button" data-action="skip">${escape(text('跳过', 'Skip'))}</button><button type="button" class="fc-guided-primary" data-action="next" ${busy ? 'disabled' : ''}>${escape(busy ? text('查询中…', 'Searching…') : step === visible.length - 1 ? text('查看结果 →', 'See results →') : text('下一步 →', 'Next →'))}</button></div></div>`,
        `<div class="fc-guided-progress"><span style="width:${(step + 1) / visible.length * 100}%"></span></div>`);
    } else {
      const rgbLayout = answers.rgb === 'yes';
      const rows = results.map(item => {
        const id = Number(item.model_id);
        const brand = en() ? (item.brand_name_en || item.brand_name_zh) : (item.brand_name_zh || item.brand_name_en);
        const name = en() ? (item.model_name_en || item.model_name_zh) : (item.model_name_zh || item.model_name_en);
        const price = item.reference_price == null ? text('价格未知', 'Price unknown') : `¥${item.reference_price}`;
        const rgbTag = Number(item.rgb_flags) > 0 ? `<span class="rpv2-mi-rgb-tag">RGB</span>` : '';
        const flags = item.recommendation_incomplete ? ` · ${escape(text('数据不完整', 'Incomplete data'))}` : '';
        const checkbox = `<input class="fc-guided-select" type="checkbox" data-select="${id}" ${selected.has(id) ? 'checked' : ''} ${busy || (selected.size >= capacity() && !selected.has(id)) ? 'disabled' : ''}>`;
        const score = `<span class="fc-guided-score"><small>${escape(text('气动表现指数', 'Aerodynamic performance index'))}</small>${stars(item.recommendation_score)}</span>`;
        const dimensions = `${escape(item.size || '—')} · ${escape(item.thickness || '—')} mm${flags}`;
        if (rgbLayout) return `<label class="fc-guided-result fc-guided-result-card">${checkbox}<span class="fc-guided-card-image"><span class="fc-guided-card-placeholder">${escape(text('暂无图片', 'No image'))}</span><img class="fc-guided-thumb" data-guided-thumb="${id}" data-rgb="${Number(item.rgb_flags) > 0 ? 1 : 0}" alt="" hidden></span><span class="fc-guided-card-details"><strong>${escape(brand)} ${escape(name)}</strong><small>${dimensions} ${rgbTag}</small><span class="fc-guided-card-bottom"><span>${escape(price)}</span>${score}</span></span></label>`;
        return `<label class="fc-guided-result fc-guided-result-row">${checkbox}<img class="fc-guided-thumb" data-guided-thumb="${id}" data-rgb="${Number(item.rgb_flags) > 0 ? 1 : 0}" alt=""><span class="fc-guided-result-info"><strong class="fc-guided-result-title"><span>${escape(brand)}</span> <span class="fc-guided-model-name" title="${escape(name)}">${escape(name)}</span></strong><small class="fc-guided-result-specs">${dimensions} ${rgbTag}</small></span><span class="fc-guided-result-meta"><small class="fc-guided-result-price">${escape(price)}</small>${score}</span></label>`;
      }).join('');
      shell(`<div class="fc-guided-body"><p class="fc-guided-kicker">YOUR SHORTLIST</p><h2 id="guidedSelectionTitle" tabindex="-1">${escape(text('选择想要对比的风扇', 'Choose fans to compare'))}</h2><p>${escape(recommendation())}</p><p class="fc-guided-tip">${escape(text('缩略图为实拍或视频封面，多颜色型号通常仅展示其中一种颜色，图片可能与所选颜色不同。', 'Thumbnails show a photo or video cover, often in only one available color, which may differ from your selection.'))}</p><div class="fc-guided-results${rgbLayout ? ' fc-guided-results-cards' : ''}">${rows || `<p>${escape(text('没有符合条件的型号，请返回调整筛选条件。', 'No matches. Go back and adjust your filters.'))}</p>`}</div><p class="fc-guided-selection-limit" role="status" aria-live="polite">${selected.size >= capacity() ? escape(text(`已选满 ${capacity()} 款，取消选择后可更换。`, `You have selected ${capacity()} fans. Deselect one to choose another.`)) : ''}</p></div>`,
        `<div class="fc-guided-actions"><button type="button" data-action="edit">${escape(text('← 上一步', '← Back'))}</button><button type="button" class="fc-guided-primary" data-action="add" ${!selected.size || busy ? 'disabled' : ''}>${escape(busy ? text('正在添加…', 'Adding…') : text(`添加 ${selected.size} 款到雷达图 →`, `Add ${selected.size} to radar →`))}</button></div>`);
      loadResultThumbs();
    }
    if (focusTitle && dialog.open) content.querySelector?.('#guidedSelectionTitle')?.focus({ preventScroll: true });
  }
  function revealWorkspace() { document.documentElement.classList.remove('fc-guided-startup'); }
  function cancelPendingRequest() {
    if (pendingRequest) { pendingRequest.abort(); pendingRequest = null; busy = false; }
  }
  function open() { notice = ''; render(); if (!dialog.open) dialog.showModal(); }
  function playOpening() {
    if (window.matchMedia?.('(prefers-reduced-motion: reduce)').matches || typeof dialog.animate !== 'function') return;
    const targetLogo = content.querySelector('.fc-guided-logo');
    const targetTitle = content.querySelector('.fc-guided-brand-title');
    if (!targetLogo || !targetTitle) return;
    const logoRect = targetLogo.getBoundingClientRect(), titleRect = targetTitle.getBoundingClientRect();
    const style = window.getComputedStyle(dialog);
    const background = style.backgroundColor, border = style.borderColor, shadow = style.boxShadow;
    const previousInert = content.inert;
    const focused = document.activeElement;
    const animations = [];
    let interactionRestored = false;
    let flightLogo, flightTitle, subtitle;
    const restoreInteraction = () => {
      if (interactionRestored) return;
      interactionRestored = true;
      content.inert = previousInert;
      if (dialog.open && !previousInert && focused && content.contains(focused)) focused.focus({ preventScroll: true });
    };
    finishOpening = (continueRotation = false) => {
      animations.forEach(animation => animation.cancel());
      flightLogo?.remove(); flightTitle?.remove(); subtitle?.remove();
      targetLogo.style.visibility = targetTitle.style.visibility = '';
      restoreInteraction();
      dialog.classList.remove('fc-guided-opening');
      window.removeEventListener('resize', finishOpening);
      finishOpening = () => {};
      if (continueRotation === true) {
        const rotor = targetLogo.querySelector('.fc-guided-logo-rotor');
        const angle = -360 * 13.1;
        rotor.animate([{ transform: `rotate(${angle}deg)` }, { transform: `rotate(${angle - 360}deg)` }], { duration: 1000, iterations: Infinity });
      }
    };
    try {
      dialog.insertAdjacentHTML('beforeend', `<span class="fc-guided-flight fc-guided-flight-logo" aria-hidden="true">${logoSvg()}</span><span class="fc-guided-flight fc-guided-flight-title" aria-hidden="true">${escape(text('风扇库', 'FanCool DB'))}</span><span class="fc-guided-flight fc-guided-flight-subtitle" aria-hidden="true">${escape(text('一个风扇性能可视化自助查询 & 对比网站', 'A self-service site for fan performance lookup and comparison'))}</span>`);
      flightLogo = dialog.querySelector('.fc-guided-flight-logo');
      flightTitle = dialog.querySelector('.fc-guided-flight-title');
      subtitle = dialog.querySelector('.fc-guided-flight-subtitle');
      flightLogo.style.left = `${logoRect.left}px`; flightLogo.style.top = `${logoRect.top}px`;
      flightTitle.style.left = `${titleRect.left}px`; flightTitle.style.top = `${titleRect.top}px`;
      subtitle.style.top = `${window.innerHeight / 2 + 5 + titleRect.height * 1.85 + 7}px`;
      targetLogo.style.visibility = targetTitle.style.visibility = 'hidden';
      content.inert = true;
      dialog.classList.add('fc-guided-opening');
      const logoX = window.innerWidth / 2 - logoRect.width * 4.4 / 2 - logoRect.left;
      const logoY = window.innerHeight / 2 - 139 - logoRect.top;
      const titleX = window.innerWidth / 2 - titleRect.width * 1.85 / 2 - titleRect.left;
      const titleY = window.innerHeight / 2 + 5 - titleRect.top;
      const duration = 5600, movement = 'cubic-bezier(.22, 1, .36, 1)';
      const timing = { duration, fill: 'forwards' };
      const moveStart = 2600 / duration, moveEnd = 4400 / duration;
      animations.push(flightLogo.animate([
        { opacity: 0, transform: `translate(${logoX}px, ${logoY}px) scale(4.4)`, offset: 0 },
        { opacity: 1, transform: `translate(${logoX}px, ${logoY}px) scale(4.4)`, offset: 600 / duration },
        { opacity: 1, transform: `translate(${logoX}px, ${logoY}px) scale(4.4)`, offset: moveStart, easing: movement },
        { opacity: 1, transform: 'translate(0, 0) scale(1)', offset: moveEnd },
        { opacity: 1, transform: 'translate(0, 0) scale(1)', offset: 1 }
      ], timing));
      animations.push(flightTitle.animate([
        { opacity: 0, transform: `translate(${titleX}px, ${titleY}px) scale(1.85)`, offset: 0 },
        { opacity: 0, transform: `translate(${titleX}px, ${titleY}px) scale(1.85)`, offset: 500 / duration },
        { opacity: 1, transform: `translate(${titleX}px, ${titleY}px) scale(1.85)`, offset: 1300 / duration },
        { opacity: 1, transform: `translate(${titleX}px, ${titleY}px) scale(1.85)`, offset: moveStart, easing: movement },
        { opacity: 1, transform: 'translate(0, 0) scale(1)', offset: moveEnd },
        { opacity: 1, transform: 'translate(0, 0) scale(1)', offset: 1 }
      ], timing));
      animations.push(subtitle.animate([
        { opacity: 0, offset: 0 }, { opacity: 0, offset: 900 / duration },
        { opacity: 1, offset: 1500 / duration }, { opacity: 1, offset: 2400 / duration }, { opacity: 0, offset: 2900 / duration },
        { opacity: 0, offset: 1 }
      ], timing));
      animations.push(content.animate([{ opacity: 0, offset: 0 }, { opacity: 0, offset: 2600 / 4400, easing: 'ease-out' }, { opacity: 1, offset: 1 }], { duration: 4400, fill: 'forwards' }));
      animations[3].onfinish = restoreInteraction;
      animations.push(dialog.animate([
        { backgroundColor: 'transparent', borderColor: 'transparent', boxShadow: 'none', offset: 0 },
        { backgroundColor: 'transparent', borderColor: 'transparent', boxShadow: 'none', offset: moveStart, easing: 'ease-out' },
        { backgroundColor: background, borderColor: border, boxShadow: shadow, offset: moveEnd },
        { backgroundColor: background, borderColor: border, boxShadow: shadow, offset: 1 }
      ], timing));
      const frames = [];
      for (let time = 0; time <= duration; time += 20) {
        let turns = 0;
        if (time > 600 && time <= 1800) {
          const x = (time - 600) / 1200;
          turns = 4 * 1.2 * (x ** 3 - x ** 4 / 2);
        } else if (time > 1800 && time <= 2600) {
          turns = 2.4 + 4 * (time - 1800) / 1000;
        } else if (time > 2600) {
          const x = (time - 2600) / 3000;
          turns = 5.6 + 3 * (4 * x - 3 * (x ** 3 - x ** 4 / 2));
        }
        frames.push({ offset: time / duration, transform: `rotate(${-360 * turns}deg)` });
      }
      animations.push(flightLogo.querySelector('.fc-guided-logo-rotor').animate(frames, timing));
      animations[0].onfinish = () => finishOpening(true);
      window.addEventListener('resize', finishOpening);
    } catch (error) {
      finishOpening();
      console.warn('[GuidedSelection] Could not play opening animation:', error);
    }
  }
  async function start() {
    const controller = new AbortController();
    pendingRequest = controller;
    busy = true; notice = ''; view = 'entry'; render(true);
    try {
      const data = await request('/api/search_metadata', { signal: controller.signal });
      if (controller.signal.aborted) return;
      if (!Array.isArray(data?.sizes) || !Array.isArray(data?.rgb_flags)) throw new Error(text('筛选选项不完整，请重试。', 'Filter options are incomplete. Please retry.'));
      metadata = data;
      answers = defaults(); selected.clear(); step = 0; view = 'wizard';
    } catch (error) { if (!controller.signal.aborted) notice = error.message; }
    finally {
      if (pendingRequest === controller) { pendingRequest = null; busy = false; render(true); }
    }
  }
  async function advance() {
    if (step < steps().length - 1) { step++; render(true); return; }
    const controller = new AbortController();
    pendingRequest = controller;
    busy = true; notice = ''; render(true);
    try {
      const data = await request('/api/guided_fan_search', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload()), signal: controller.signal });
      if (controller.signal.aborted) return;
      if (!Array.isArray(data?.search_results)) throw new Error(text('搜索结果格式不正确。', 'Invalid search response.'));
      if (!data.search_payload || typeof window.FancoolSearch?.applyGuidedSearch !== 'function') throw new Error(text('高级搜索尚未就绪，请重试。', 'Advanced search is not ready. Please retry.'));
      await window.FancoolSearch.applyGuidedSearch(data.search_payload, { signal: controller.signal });
      if (controller.signal.aborted) return;
      results = data.search_results.slice(0, 200); selected.clear(); view = 'results';
    } catch (error) { if (!controller.signal.aborted) notice = error.message; }
    finally {
      if (pendingRequest === controller) { pendingRequest = null; busy = false; render(true); }
    }
  }
  async function addSelected() {
    busy = true; notice = ''; render();
    let added = 0, highlighted = false;
    try {
      await window.radarReady;
      if (!window.RadarState || typeof window.addModelToRadar !== 'function' ||
          typeof window.prepareRadarForGuidedSelection !== 'function' ||
          typeof window.startGuidedRadarOnboarding !== 'function') throw new Error(text('雷达图尚未就绪，请重试。', 'Radar is not ready. Please retry.'));
      const pending = results.filter(item => selected.has(Number(item.model_id)));
      if (!pending.length) throw new Error(text('请先选择型号。', 'Select at least one model.'));
      if (pending.length > capacity()) throw new Error(text(`雷达图最多容纳 ${capacity()} 款，请减少选择。`, `The radar holds at most ${capacity()} models. Reduce your selection.`));
      window.prepareRadarForGuidedSelection();
      for (const item of pending) {
        const brand = item.brand_name_zh || item.brand_name_en || '';
        const name = item.model_name_zh || item.model_name_en || '';
        if (!await window.addModelToRadar(item.model_id, brand, name, { source: 'guided_selection' })) throw new Error(text('部分型号未能添加，请检查提示后重试。', 'Some models could not be added. Check the message and retry.'));
        added++;
      }
      if (window.RadarState.getActiveConditions().size) throw new Error(text('工况状态已变化，请检查雷达图。', 'Conditions changed; please check the radar.'));
      hint.querySelector('span').textContent = text('型号已加入雷达图。请点击雷达图周围的工况标签选择查看性能曲线。并可在性能图表中播放任意转速噪音实录；亦可在图表标题切换至噪音-风量曲线，比较相同噪音下，不同风扇的有效风量大小。', 'Models have been added. Choose a radar condition to show curves; You may play recorded noise samples at any fan speed within the performance chart. You can also switch the chart title to the noise-airflow curve to compare the effective airflow of different fans at identical noise levels.');
      hint.hidden = false;
      window.startGuidedRadarOnboarding();
      highlighted = true;
      dialog.close();
    } catch (error) { notice = error.message; }
    finally {
      if (added && !highlighted) {
        try { window.startGuidedRadarOnboarding(); }
        catch (error) {
          console.error('[GuidedSelection] Could not highlight radar conditions:', error);
          notice = `${notice} ${text('工况提示无法显示，请检查雷达图。', 'Could not highlight conditions; check the radar.')}`.trim();
        }
      }
      busy = false; render();
    }
  }
  content.addEventListener('change', event => {
    if (event.target.matches('[data-skip-entry]')) {
      skipEntry = event.target.checked;
      return;
    }
    if (!event.target.matches('[data-select]')) return;
    const id = Number(event.target.dataset.select);
    if (event.target.checked && !selected.has(id) && selected.size >= capacity()) event.target.checked = false;
    else if (event.target.checked) selected.add(id);
    else selected.delete(id);
    content.querySelectorAll?.('[data-select]').forEach(input => { input.disabled = busy || (selected.size >= capacity() && !input.checked); });
    const limitNotice = content.querySelector?.('.fc-guided-selection-limit');
    if (limitNotice) limitNotice.textContent = selected.size >= capacity()
      ? text(`已选满 ${capacity()} 款，取消选择后可更换。`, `You have selected ${capacity()} fans. Deselect one to choose another.`) : '';
    const addButton = content.querySelector?.('[data-action="add"]');
    if (addButton) {
      addButton.disabled = !selected.size;
      addButton.textContent = text(`添加 ${selected.size} 款到雷达图 →`, `Add ${selected.size} to radar →`);
    }
  });
  content.addEventListener('click', event => {
    const button = event.target.closest('button');
    if (!button) {
      const input = event.target.closest('.fc-guided-result')?.querySelector('[data-select]');
      if (input && event.target !== input) {
        event.preventDefault();
        if (!busy) input.click();
      }
      return;
    }
    if (busy && !(pendingRequest && button.dataset.action === 'close')) return;
    if (button.dataset.choice) {
      const current = steps()[step], value = button.dataset.choice;
      const scrollTop = content.querySelector?.('.fc-guided-scroll')?.scrollTop || 0;
      if (current.multi) answers.locations = answers.locations.includes(value) ? answers.locations.filter(v => v !== value) : [...answers.locations, value];
      else {
        answers[current.key] = value;
        if (current.key === 'scenario') { answers.intake = 'any'; answers.radiator = 'any'; answers.tower = 'any'; }
        if (current.key === 'rgb') answers.locations = [];
      }
      notice = ''; render();
      const scroll = content.querySelector?.('.fc-guided-scroll');
      if (scroll) scroll.scrollTop = scrollTop;
      content.querySelector?.(`[data-choice="${value}"]`)?.focus({ preventScroll: true });
      return;
    }
    switch (button.dataset.action) {
      case 'close': cancelPendingRequest(); dialog.close(); break;
      case 'expert':
        try {
          if (skipEntry) localStorage.setItem(skipEntryKey, '1');
          else if (window.__FC_SHOW_GUIDED_ENTRY === false) localStorage.removeItem(skipEntryKey);
          window.__FC_SHOW_GUIDED_ENTRY = !skipEntry;
          dialog.close();
        } catch (error) {
          console.warn('[GuidedSelection] Could not save entry preference:', error);
          notice = text('无法保存入口偏好，请检查浏览器存储设置。', 'Could not save your preference. Check your browser storage settings.');
          render();
        }
        break;
      case 'start': start(); break;
      case 'back': if (step) step--; else view = 'entry'; notice = ''; render(true); break;
      case 'skip': {
        const current = steps()[step];
        answers[current.key] = current.multi ? [] : 'any';
        if (current.key === 'scenario') { answers.intake = 'any'; answers.radiator = 'any'; answers.tower = 'any'; }
        if (current.key === 'rgb') answers.locations = [];
        advance(); break;
      }
      case 'next': advance(); break;
      case 'edit': view = 'wizard'; step = steps().length - 1; notice = ''; render(true); break;
      case 'add': addSelected(); break;
    }
  });
  openButton.addEventListener('click', open);
  hint.querySelector('button').addEventListener('click', () => { hint.hidden = true; });
  dialog.addEventListener('cancel', event => {
    if (busy && !pendingRequest) event.preventDefault();
    else cancelPendingRequest();
  });
  dialog.addEventListener('close', () => {
    if (!dialog.open) cancelPendingRequest();
    finishOpening(); revealWorkspace();
  });
  document.addEventListener('fc:languagechange', () => { finishOpening(); render(); });
  document.addEventListener('DOMContentLoaded', () => {
    render();
    if (window.__FC_SHOW_GUIDED_ENTRY) {
      try { open(); playOpening(); }
      catch (error) {
        revealWorkspace();
        console.error('[GuidedSelection] Could not open entry:', error);
      }
    }
  });
})(window, document);
