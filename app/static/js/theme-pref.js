// 全局通用主题工具（单例）
(function initThemePref(){
  if (window.ThemePref) return;
  window.ThemePref = (function(){
    function normalize(theme){
      return (theme === 'dark') ? 'dark' : 'light';
    }
    function resolve(){
      // 1) localStorage 优先
      try {
        const saved = localStorage.getItem('theme');
        if (saved === 'dark' || saved === 'light') return saved;
      } catch(_) {}
      // 2) SSR/DOM 的 data-theme
      const attr = document.documentElement.getAttribute('data-theme');
      if (attr === 'dark' || attr === 'light') return attr;
      // 3) 系统偏好
      try {
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
          return 'dark';
        }
      } catch(_) {}
      // 4) 默认
      return 'light';
    }
    function setDom(theme){
      document.documentElement.setAttribute('data-theme', normalize(theme));
    }
    function save(theme, opts){
      const t = normalize(theme);
      try { localStorage.setItem('theme', t); } catch(_) {}
      const notify = !!(opts && opts.notifyServer);
      if (notify) {
        try {
          fetch('/api/theme', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ theme: t })
          }).catch(()=>{});
        } catch(_) {}
      }
      return t;
    }
    return { resolve, setDom, save };
  })();
})();