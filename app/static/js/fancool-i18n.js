(function (window, document) {
  'use strict';

  const STORAGE_KEY = 'fc_lang';
  const DEFAULT_LANG = 'zh';
  const SUPPORTED = new Set(['zh', 'en']);
  const listeners = new Set();
  const isMultiLangEnabled = () => window.__FC_MULTI_LANG_ENABLED === true
    || (window.APP_CONFIG && window.APP_CONFIG.multiLangEnabled === true);

  const DICT = {
    zh: {
      common: {
        loading: '加载中...',
        loadingEllipsis: '加载中…',
        noData: '暂无相关数据',
        none: '无',
        yes: '是',
        no: '否',
        notAvailable: '—',
        retryLater: '请稍后再试。',
      },
      site: {
        name: '风扇库',
        title: '风 扇 库',
        subtitle: '一个风扇性能可视化自助查询 & 对比网站',
        pageTitle: '风扇库',
        legalTitle: '版权及免责声明 - 风扇库',
        switchLabel: '切换语言',
        home: '返回主页',
      },
      game: {
        title: '转风扇挑战', description: '拨动风扇，积累风量并升级装备，挑战转速极限。',
        status: '状态与耐久', workshop: '风扇工坊', close: '关闭',
        airflow: '实时风量', turns: '累计圈数', todayBest: '今日最高', best: '历史最高',
        todayAirflow: '今日风量', totalAirflow: '累计风量', modelNote: '模型仅用于交互手感，不代表真实硬件测量。',
        todayPeakAirflow: '今日最大瞬时风量', weekPeakAirflow: '本周最大瞬时风量', bestAirflow: '历史最大瞬时风量',
        hint: '点击这里查看耐久，快速更换叶轮和装备。',
      },
      sidebar: {
        toggleLabel: '展开/收起侧栏',
        dataManager: '数据管理',
        resources: '资源下载',
        history: '浏览历史',
        likes: '最近点赞',
        loading: '加载中...',
        noHistory: '暂无浏览历史',
        likesLazy: '首次打开该页签时加载...',
        noRecentLikes: '暂无最近点赞',
      },
      home: {
        sourceInfo: '数据来源与说明',
        toggleTheme: '切换深浅色',
        addByModel: '按型号添加',
        advancedSearch: '进阶搜索',
        searchByModel: '输入型号直接搜索',
        modelKeywordPlaceholder: '输入型号关键字...',
        brand: '品牌',
        selectBrand: '-- 选择品牌 --',
        selectBrandFirst: '-- 请先选择品牌 --',
        modelComposite: '型号（综合评分）',
        selectModel: '-- 选择型号 --',
        selectModelFirst: '-- 请先选择型号 --',
        conditionScore: '工况评分',
        addToCompare: '添加至对比',
        conditionFilter: '工况筛选',
        selectCondition: '-- 选择测试工况 --',
        compositePerformance: '综合性能',
        sortBy: '排序依据',
        sortByHelp: '仅在“工况筛选”为非“综合性能”模式下可用。\n工况评分：按该工况的单项评分排序。\n全速风量：取风扇全速运行时的风量。\n同转速风量：取转速限制以下的最高风量。\n同分贝风量：取噪音限制以下的最高风量。',
        sortByHelpLabel: '排序依据说明',
        sortValue: '限制值',
        size: '尺寸(mm)',
        thickness: '风扇厚度(mm)',
        rgb: 'RGB灯光',
        colors: '可选颜色',
        others: '其它',
        price: '参考价格(元)',
        maxSpeed: '最大转速(RPM)',
        searchFan: '搜索风扇',
        scoreRule: '评分算法说明',
        fanChallenge: '转风扇挑战',
        ladder: '天梯图',
        queryCount: '已执行查询 {count} 次',
        rankings: '排行榜',
        searchResults: '搜索结果',
        recentUpdates: '近期更新',
        boardSwitch: '榜单切换',
        lightingBoard: '灯效榜',
        heatBoard: '热度榜',
        performanceBoard: '性能榜',
        searchHint: '使用进阶搜索以查看结果',
        recentUpdatesLazy: '首次点击“近期更新”加载...',
        viewAllModels: '查看所有型号 ›',
        collapseRadar: '收起雷达图',
        expandRadar: '展开雷达图',
        resetRadar: '重置雷达',
        reset: '重置',
        clearModels: '清空型号',
        clearConditions: '取消工况激活',
        resetColors: '重置配色',
        reviewSubtitle: '测试点评',
        feedback: '反馈问题',
        gallery: '图库 & 视频',
        playBilibili: '播放 B 站视频',
        bilibiliTitle: 'B 站视频播放',
        noImages: '暂无图片',
        toggleExtraPanel: '切换到点评/图库',
        galleryPreview: '图片预览',
        close: '关闭',
        previous: '上一张',
        next: '下一张',
        switchImages: '图片切换',
        radarOverviewTitle: '工况评分对比',
        radarOverviewHint: '点击工况标签可查看性能曲线和噪音实录',
      },
      chart: {
        axisSwitch: 'X轴切换',
        rpmShort: '转速',
        noiseShort: '噪音',
        rpmAxis: '转速(RPM)',
        noiseDbAxis: '噪音(dBA)',
        noiseSoneAxis: '噪音(sone)',
        airflowAxis: '风量(CFM)',
        airflowCurve: '风量曲线',
        spectrumToggle: '展开/收起频谱',
        expandSpectrum: '展开频谱',
        collapseSpectrum: '收起频谱',
        spectrumTitle: 'OCT A计权声级频谱',
        noiseUnitSwitch: '噪音单位切换',
        switchBackToMain: '切换回主图',
      },
      scoreRule: {
        title: '评分算法说明',
        intro: '评分核心思路为风扇在各测试工况下的「风噪比」，与风扇的最大风量及风压没有直接关联。<br>以下为当前评分算法的计算流程：',
        empty: '暂无说明内容，请稍后重试。',
        compositeWeight: '综合分权重',
        condition: '工况',
        weight: '权重占比',
        requestFailed: '请求失败',
        loadFailed: '加载失败，请稍后再试。',
      },
      ladder: {
        title: '天梯图',
        tabs: {
          composite: '综合天梯',
          intakeExhaust: '进排气天梯',
          radiator: '吹冷排天梯',
        },
        tablist: '天梯图分类',
        note: '<sup>* </sup>因各家风冷塔体设计和热管排布差异较大，为避免引起争议暂不展示风冷天梯图，仅在网页端提供有效风量排名参考。<br><sup>* </sup>经过改装、非原厂状态及停产较久的型号不作展示。',
        download: '下载图片',
      },
      feedback: {
        title: '反馈问题',
        tabs: '反馈页签',
        submitTab: '反馈问题',
        myTab: '我的反馈',
        disclaimer: '本页面仅用于网站收集用户使用反馈，并非品牌官方反馈或售后通道；如遇质量问题，请优先联系品牌官方售后处理。<br>请基于实际使用体验如实反馈问题，避免夸大、误报或无依据地贬损特定品牌或型号。',
        otherPlaceholder: '请填写其他问题（最多 {max} 字）',
        submit: '提交反馈',
        noRecords: '暂无反馈记录',
      },
      footer: {
        publicSecurity: '沪公网安备31011402021508号',
        icp: '沪ICP备2025145529号-1',
        group: '交流及反馈QQ群：1070155380',
        github: 'GitHub用户端代码开源',
        legal: '版权及免责声明',
        beianIconAlt: '备案图标',
      },
      legal: {
        title: '版权及免责声明',
        intro: '欢迎访问“风扇库”（下称“本站”）。在使用本站提供的各项功能与数据前，请仔细阅读本页内容。继续使用即表示你同意并接受以下条款。',
        sections: {
          copyright: '一、版权声明',
          data: '二、数据来源与使用限制',
          disclaimer: '三、免责声明',
          fairUse: '四、合理使用与引用',
          privacy: '五、隐私与 Cookies',
          updates: '六、条款更新',
          contact: '七、联系方式',
        },
        copyrightHtml: '1. 本网站内所有内容（包括但不限于风扇测试数据、图表、文字描述、图片、网站设计、程序代码等）均为网站运营方独立创作或拥有合法权利的内容。除法律另有规定外，本网站对上述内容享有完整的著作权、数据库权利及其他相关知识产权。未经本网站书面许可，任何单位或个人不得擅自复制、转载、摘编、传播、镜像、修改本网站内容，或用于商业用途（包括但不限于用于产品宣传、研发参考并从中获利等）。非商业性引用时，需明确标注来源为“风扇库”并保留本版权声明，否则视为侵权。如发现侵权行为，本网站将依法追究侵权方的法律责任。<br>2. 站内所引用之品牌名称、型号名称、商标等，其权利归各自所属权利人所有，仅用于客观描述与对比，不代表本站与其存在商业合作或背书关系。<br>3. 若你认为本站某些内容可能涉及侵权，请使用页面底部提供的联系方式告知，我们将在核实后及时处理。',
        dataHtml: '1. 数据主要来源于本站维护者基于现场测试所得（<a class="inline-link" href="/source-info">数据来源与说明</a>），部分可能参考公开资料或厂商文档。<br>2. 由于测试仪器精度、环境条件、样品个体差异等因素，数据存在一定误差，仅供爱好者技术交流与非商业参考。<br>3. 未经许可请勿将本站测试结果直接用于商业广告、官方对外发布或可能引导消费者决策的正式材料。',
        disclaimerHtml: '1. 本站力求数据与说明的客观准确，但不对其完整性、实时性、适用性做出任何保证。使用者据此做出的行为或决定造成的任何直接或间接损失，本站不承担责任。<br>2. 本站可能因维护、服务器故障、不可抗力等原因暂时中断或停止服务，对由此造成的损失不承担赔偿责任。<br>3. 对第三方链接（若存在）所指向内容的合法性、真实性与可用性不承担审查义务与责任。',
        fairUseHtml: '1. 你可以在非商业前提下引用少量数据或截图，用于讨论、研究、论坛分享，但需注明来源：“数据来源：风扇库”。<br>2. 批量数据抓取、自动化脚本高频访问等行为会对本站服务质量造成影响，本站保留采取限制或屏蔽措施的权利。',
        privacyHtml: '本站可能使用必要的 Cookies 以维持会话或统计访问。我们不会主动收集可直接识别个人身份的敏感信息。若未来政策调整，将在本页更新。',
        updatesHtml: '本站有权根据需要随时更新本页面内容。更新后条款一经公布即自动生效，请定期查阅。',
        contactHtml: '如有版权、合作、数据纠错、侵权投诉等，请发送邮件至：<a class="inline-link" href="mailto:541112159@qq.com">541112159@qq.com</a>。',
        updateDate: '最新更新日期：',
        rights: '© {year} 风扇库. 保留部分权利。',
      },
      sourceInfo: {
        pageTitle: '数据来源与说明 - 风扇库',
        title: '数据来源与测试环境说明',
        airflowEquipment: '风量测试装置',
        airflowCaption: '↑风量测量装置如上图所示↑',
        tunnel: '风洞设计部分参考amca-210标准，由整流段、渐缩段、测量段、扩散段组成。风扇出风侧连接风洞入口。',
        airflowMethod: '测试时使用波形发生器发出指定占空比的方波对风扇进行调速，使用示波器连接风扇测速线测量风扇转速，使用Testo425热敏式风速仪测量风速并通过计算转换成风量（标定时采用径向多点等面积法测量风速并得到标定风量）。',
        soundEnvironment: '声学测试环境',
        soundCaption: '↑噪音测试环境如上图所示↑',
        soundSetup: '风扇固定于隔振平台支架上。风扇出风侧前方>60cm净空，进风侧前方>40cm净空。',
        soundMethod: '测试环境本底噪音低于8dBA，使用爱华AWA5662专业声级计采集噪音数据，声级探头固定于风扇上方距离风扇外框顶部30cm处。',
        faq: '常见问答（Q&A）',
        probeQuestion: 'Q: 为什么声级探头平行于风扇旋转面而不是常见的正对风扇旋转面？',
        probeAnswer: 'A: 考虑到风扇在不同工况下安装方向不同，可能进风侧朝外也可能出风侧朝外，且负载也可能安装在风扇任意侧。把声级探头固定在平行于风扇旋转面的方向上可以兼顾两侧的收音以适应各种工况。需要注意的是这个方案下收音效果和传统正对旋转面的方案会有些许差异。',
        specQuestion: 'Q: 为什么有些型号的空载数据和官方标称不一样？',
        specAnswer: 'A: 不同厂家可能采用了不同的测试标准和环境，因此导致了数据的差异。并且我们的风洞为被动式风洞，会有少许风量损失。空载风量数值以官方数据为准，我们的测试结果仅供参考。',
        tests: '测试项目展示',
        intro: '以下展示测试所用的阻抗件和六种测试工况。各工况分别进行风量测试与噪音测试；示例照片中的风扇为猫头鹰 A12x25 G2。',
        equipment: '测试用阻抗件',
        panelsAlt: '排气面板和进气面板，排气面板在左，进气面板在右',
        panels: '排气与进气面板',
        panelsDescription: '排气面板开孔率为 70%；进气面板使用 34 目 PET 网，并带有 10 mm 垫高。',
        towerAlt: 'Arctic Freezer 36 风冷塔体',
        tower: '风冷塔体',
        towerDescription: '测试使用 Arctic Freezer 36 风冷塔体。',
        radiatorAlt: '测试用冷排',
        radiator: '冷排',
        radiatorDescription: '总厚度 27 mm，芯体厚度 20 mm，鳍片密度 20 FPI。',
        conditions: '六种测试工况',
        pairDescription: '每组左侧为风量测试，右侧为噪音测试。',
        cases: {
          free: { name: '空载', description: '不加装阻抗件，展示风扇空载测试状态。', airflow: '空载 · 风量测试', noise: '空载 · 噪音测试' },
          intake: { name: '进气', description: '模拟风扇通过机箱进气面板进风的场景。', airflow: '进气 · 风量测试', noise: '进气 · 噪音测试' },
          exhaust: { name: '排气', description: '模拟风扇通过机箱排气面板排风的场景。', airflow: '排气 · 风量测试', noise: '排气 · 噪音测试' },
          tower: { name: '风冷', description: '风扇搭配风冷塔体进行测试。', airflow: '风冷 · 风量测试', noise: '风冷 · 噪音测试' },
          push: { name: '吹冷排', description: '风扇向冷排吹风。', airflow: '吹冷排 · 风量测试', noise: '吹冷排 · 噪音测试' },
          pull: { name: '吸冷排', description: '风扇从冷排吸风。', airflow: '吸冷排 · 风量测试', noise: '吸冷排 · 噪音测试' },
        },
        radiatorPanel: '冷排另一侧加装进气面板',
        radiatorSetup: '冷排工况说明',
        radiatorSetupDescription: '吹冷排和吸冷排测试均在冷排另一侧加装进气面板，模拟冷排安装在机箱内的情况。',
        whistle: '啸叫测试',
        whistleIntro: '以进气测试中有效风量达到 40 CFM 时的转速和噪音为基准，交换排气面板与进气面板的顺序，比较风扇进风侧面板位置变化带来的噪音变化。',
        whistleReference: '进气测试基准：排气面板 → 进气面板 → 风扇进风面',
        whistleTest: '啸叫测试：进气面板 → 排气面板 → 风扇进风面',
        whistleMethod: '进气测试中，进气面板带有 10 mm 垫高；啸叫测试中，排气面板紧贴风扇进风面，可能激发啸叫。在基准转速下录制噪声，与进气测试同转速的噪声比较，通过噪音增加量和啸叫测点的音调度评判啸叫程度。',
        notes: '说明',
        disclaimer: '本页面图片及文字说明来自实际测试及记录。由于设备与环境并非标准实验室条件，结果适合作为相对对比参考，不作为严格标定依据。',
      },
      dynamic: {
        purchaseLinks: '购买链接',
        noPurchaseLinks: '暂无购买链接',
        noImage: '暂无图片',
        fanPhoto: '风扇照片',
        download: '下载',
        viewDetails: '展开详情',
        hideDetails: '收起详情',
        caution: '注意',
        whistle: '啸叫',
        whistlePlay: '播放啸叫测试录音',
        whistleStop: '停止播放啸叫录音',
        whistleAudioUnavailable: '暂无可播放的啸叫测试录音',
        whistleAudioError: '啸叫录音播放失败',
        whistleNoData: '暂无数据',
        whistleTooltipTitle: '进气遮挡啸叫等级：',
        whistleTooltipTotal: '总：',
        whistleTooltipSeparator: '；',
        whistleTooltipIndex: '总体吵闹指数：',
        whistleTooltipExperimental: '（试验版）',
        whistleTooltipSpeed: '测试转速：{rpm} RPM（{airflow} CFM）',
        whistleTooltipDisclaimer: '注：啸叫等级指标为啸叫测试工况相对正常进气工况同转速拟合值的噪音增加量。数据仅供参考，实际表现会因机箱环境而变化。',
        copiedModel: '已复制型号文字',
        like: '点赞',
        unlike: '取消点赞',
        addToRadar: '添加到雷达对比',
        compositeRadarChart: '综合评分雷达图',
        compositeScore: '综合评分',
        rgbLighting: 'RGB灯光：{value}',
        lightingLikes: '灯效获赞数: {count}',
        conditionLikes: '工况好评',
        conditionHeat: '工况热度',
      },
    },
    en: {
      common: {
        loading: 'Loading...',
        loadingEllipsis: 'Loading…',
        noData: 'No data available',
        none: 'None',
        yes: 'Yes',
        no: 'No',
        notAvailable: '—',
        retryLater: 'Please try again later.',
      },
      site: {
        name: 'FanCool DB',
        title: 'FanCool DB',
        subtitle: 'A self-service site for fan performance lookup and comparison',
        pageTitle: 'FanCool DB',
        legalTitle: 'Copyright & Disclaimer - FanCool DB',
        switchLabel: 'Switch language',
        home: 'Back to home',
      },
      game: {
        title: 'Fan Spin Challenge', description: 'Spin a 120 mm fan, earn airflow and upgrade your equipment.',
        status: 'Condition & wear', workshop: 'Workshop', close: 'Close',
        airflow: 'Airflow', turns: 'Total turns', todayBest: 'Today’s best', best: 'Personal best',
        todayAirflow: 'Today’s airflow', totalAirflow: 'Total airflow', modelNote: 'This model approximates interaction feel, not measured hardware performance.',
        todayPeakAirflow: 'Today’s peak airflow', weekPeakAirflow: 'This week’s peak airflow', bestAirflow: 'All-time peak airflow',
        hint: 'Open here to check wear and quickly replace the impeller or equipment.',
      },
      sidebar: {
        toggleLabel: 'Expand or collapse sidebar',
        dataManager: 'Data Manager',
        resources: 'Downloads',
        history: 'History',
        likes: 'Recent Likes',
        loading: 'Loading...',
        noHistory: 'No browsing history yet',
        likesLazy: 'Loads when you open this tab for the first time...',
        noRecentLikes: 'No recent likes yet',
      },
      home: {
        sourceInfo: 'Data sources & notes',
        toggleTheme: 'Toggle light/dark theme',
        addByModel: 'Add by Model',
        advancedSearch: 'Advanced Search',
        searchByModel: 'Search directly by model',
        modelKeywordPlaceholder: 'Type model keywords...',
        brand: 'Brand',
        selectBrand: '-- Select a brand --',
        selectBrandFirst: '-- Select a brand first --',
        modelComposite: 'Model (composite score)',
        selectModel: '-- Select a model --',
        selectModelFirst: '-- Select a model first --',
        conditionScore: 'Condition scores',
        addToCompare: 'Add to radar compare',
        conditionFilter: 'Condition filter',
        selectCondition: '-- Select a test condition --',
        compositePerformance: 'Composite performance',
        sortBy: 'Sort by',
        sortByHelp: 'Available only when Condition filter is not set to Composite performance.\nCondition score: sort by the selected condition score.\nMax-speed airflow: airflow at full speed.\nSame-RPM airflow: highest airflow under the RPM limit.\nSame-dBA airflow: highest airflow under the noise limit.',
        sortByHelpLabel: 'Sort rule help',
        sortValue: 'Limit',
        size: 'Size (mm)',
        thickness: 'Thickness (mm)',
        rgb: 'RGB',
        colors: 'Colors',
        others: 'Other features',
        price: 'Ref. price (CNY)',
        maxSpeed: 'Max RPM',
        searchFan: 'Search fans',
        scoreRule: 'Scoring guide',
        fanChallenge: 'Fan Spin Challenge',
        ladder: 'Tier list',
        queryCount: '{count} searches run',
        rankings: 'Boards',
        searchResults: 'Search',
        recentUpdates: 'Updates',
        boardSwitch: 'Ranking switcher',
        lightingBoard: 'RGB',
        heatBoard: 'Heat',
        performanceBoard: 'Perf',
        searchHint: 'Use Advanced Search to see results',
        recentUpdatesLazy: 'Loads when you open Recent Updates for the first time...',
        viewAllModels: 'View all models ›',
        collapseRadar: 'Collapse radar panel',
        expandRadar: 'Expand radar panel',
        resetRadar: 'Reset radar',
        reset: 'Reset',
        clearModels: 'Clear models',
        clearConditions: 'Clear active conditions',
        resetColors: 'Reset colors',
        reviewSubtitle: 'Review notes',
        feedback: 'Report an issue',
        gallery: 'Gallery & Video',
        playBilibili: 'Play Bilibili video',
        bilibiliTitle: 'Bilibili video player',
        noImages: 'No images yet',
        toggleExtraPanel: 'Switch to review/gallery panel',
        galleryPreview: 'Image preview',
        close: 'Close',
        previous: 'Previous',
        next: 'Next',
        switchImages: 'Image navigation',
        radarOverviewTitle: 'Condition Scores',
        radarOverviewHint: 'Tap a condition to check curves and recordings',
      },
      chart: {
        axisSwitch: 'X-axis switch',
        rpmShort: 'Speed',
        noiseShort: 'Noise',
        rpmAxis: 'RPM',
        noiseDbAxis: 'Noise (dBA)',
        noiseSoneAxis: 'Noise (sone)',
        airflowAxis: 'Airflow (CFM)',
        airflowCurve: 'Airflow curve',
        spectrumToggle: 'Expand or collapse spectrum',
        expandSpectrum: 'Expand spectrum',
        collapseSpectrum: 'Collapse spectrum',
        spectrumTitle: 'OCT A-weighted SPL spectrum',
        noiseUnitSwitch: 'Noise unit switch',
        switchBackToMain: 'Switch back to main chart',
      },
      scoreRule: {
        title: 'Scoring guide',
        intro: 'The scoring core focuses on each fan’s airflow-to-noise ratio under specific test conditions, rather than raw maximum airflow or static pressure.<br>Below is the current scoring flow:',
        empty: 'No explanation is available right now. Please try again later.',
        compositeWeight: 'Composite score weights',
        condition: 'Condition',
        weight: 'Weight',
        requestFailed: 'Request failed',
        loadFailed: 'Failed to load. Please try again later.',
      },
      ladder: {
        title: 'Tier list',
        tabs: {
          composite: 'Composite',
          intakeExhaust: 'Intake / Exhaust',
          radiator: 'Radiator',
        },
        tablist: 'Tier-list categories',
        note: '<sup>* </sup>Air-cooler tower ladders are not shown for now because tower designs vary widely and would easily cause disputes; the web page instead keeps effective-airflow rankings as a reference.<br><sup>* </sup>Heavily modded, non-stock, or long-discontinued models are excluded.',
        download: 'Download image',
      },
      feedback: {
        title: 'Report an issue',
        tabs: 'Feedback tabs',
        submitTab: 'Report an issue',
        myTab: 'My reports',
        disclaimer: 'This page is only for collecting website feedback and is not an official brand support or warranty channel.<br>Please report issues based on actual usage experience and avoid exaggeration, false reports, or unsupported claims about a specific brand or model.',
        otherPlaceholder: 'Describe the other issue (max {max} chars)',
        submit: 'Submit feedback',
        noRecords: 'No feedback records yet',
      },
      footer: {
        publicSecurity: '沪公网安备31011402021508号',
        icp: '沪ICP备2025145529号-1',
        group: 'QQ feedback group: 1070155380',
        github: 'GitHub frontend repository',
        legal: 'Copyright & Disclaimer',
        beianIconAlt: 'Registration icon',
      },
      legal: {
        title: 'Copyright & Disclaimer',
        intro: 'Welcome to FanCool DB (“the Site”). Please read this page carefully before using the data, charts, and tools provided here. By continuing to use the Site, you acknowledge and accept the following terms.',
        sections: {
          copyright: '1. Copyright notice',
          data: '2. Data sources & usage limits',
          disclaimer: '3. Disclaimer',
          fairUse: '4. Fair use & citation',
          privacy: '5. Privacy & cookies',
          updates: '6. Terms updates',
          contact: '7. Contact',
        },
        copyrightHtml: '1. All content on this Site, including but not limited to fan test data, charts, written descriptions, images, site design, and source code, is either independently created by the operator or used with lawful rights. Unless otherwise required by law, the Site retains the related copyright, database rights, and other intellectual-property rights. No organization or individual may copy, republish, mirror, adapt, redistribute, or use Site content for commercial purposes without prior written permission. Non-commercial quotations must clearly credit “FanCool DB” and keep this notice intact.<br>2. Brand names, model names, and trademarks referenced on the Site remain the property of their respective owners and are used only for objective description and comparison. Their appearance does not imply partnership, sponsorship, or endorsement.<br>3. If you believe any content on the Site infringes your rights, please contact us using the address below and we will review it promptly.',
        dataHtml: '1. Most data comes from hands-on testing conducted by the Site maintainer (see <a class="inline-link" href="/source-info">Data sources & notes</a>), with some details cross-checked against public materials or vendor documentation.<br>2. Because of instrument precision, environmental conditions, and sample variance, the published numbers may contain measurement error. They are shared for enthusiast discussion and non-commercial reference only.<br>3. Without permission, do not use Site test results directly in commercial marketing, official public statements, or other formal materials that may materially influence purchasing decisions.',
        disclaimerHtml: '1. The Site aims to keep its data and commentary objective and useful, but provides them on an “as is” and “as available” basis without warranties of completeness, timeliness, fitness for a particular purpose, merchantability, or non-infringement.<br>2. You are responsible for verifying compatibility, safety, availability, price, and suitability before making any purchasing, installation, or engineering decision. The Site is not legal, technical, or safety advice, and we are not liable for direct, indirect, incidental, consequential, or special losses arising from reliance on the Site where such limits are permitted by applicable law.<br>3. The Site may be interrupted by maintenance, server issues, third-party failures, or force majeure. External links, stores, and embedded services are provided for convenience only, and the Site does not guarantee or continuously monitor their legality, accuracy, uptime, or policies.',
        fairUseHtml: '1. You may quote limited data points or screenshots for non-commercial discussion, research, or forum sharing, provided that you clearly cite the source as “FanCool DB”.<br>2. Bulk scraping, aggressive automation, or other high-frequency access that harms service quality may be rate-limited or blocked.',
        privacyHtml: 'The Site may use necessary cookies or similar local storage to maintain sessions, remember interface preferences, and collect high-level visit statistics. We do not intentionally collect sensitive personal information that directly identifies you through this page. If the policy changes materially, this page will be updated.',
        updatesHtml: 'We may revise this page whenever needed. Updated terms take effect once posted, so please review this page periodically.',
        contactHtml: 'For copyright matters, cooperation requests, data corrections, or infringement complaints, please email: <a class="inline-link" href="mailto:541112159@qq.com">541112159@qq.com</a>.',
        updateDate: 'Last updated: ',
        rights: '© {year} FanCool DB. Some rights reserved.',
      },
      sourceInfo: {
        pageTitle: 'Data Sources & Test Environment - FanCool DB',
        title: 'Data Sources and Test Environment',
        airflowEquipment: 'Airflow test apparatus',
        airflowCaption: '↑ Airflow test apparatus shown above ↑',
        tunnel: 'The wind tunnel design partly follows the AMCA 210 standard and comprises a flow straightening section, contraction, measurement section and diffuser. The fan outlet faces the tunnel inlet.',
        airflowMethod: 'A waveform generator controls fan speed with a square wave at a specified duty cycle, and an oscilloscope measures speed via the fan tachometer wire. A Testo 425 thermal anemometer measures air velocity, which is converted to airflow. Calibration uses radial multi-point equal-area velocity measurements to obtain calibrated airflow.',
        soundEnvironment: 'Noise test environment',
        soundCaption: '↑ Noise test environment shown above ↑',
        soundSetup: 'The fan is mounted on a stand on an isolation platform. There is more than 60 cm of clearance in front of the outlet and more than 40 cm in front of the inlet.',
        soundMethod: 'Background noise is below 8 dBA. An Aihua AWA5662 professional sound level meter records noise, with its probe positioned 30 cm above the top of the fan frame.',
        faq: 'Frequently asked questions',
        probeQuestion: 'Q: Why is the sound probe parallel to the fan rotation plane rather than facing it?',
        probeAnswer: 'A: Fans and loads can be installed in different orientations, with either the inlet or outlet facing outward. Positioning the probe parallel to the rotation plane captures sound from both sides across different setups. Results may differ slightly from the conventional face-on arrangement.',
        specQuestion: 'Q: Why do some free-air results differ from manufacturer specifications?',
        specAnswer: 'A: Manufacturers may use different test standards and environments. Our passive wind tunnel also incurs some airflow loss. Refer to manufacturer specifications for free-air airflow; our results are for reference only.',
        tests: 'Test setups',
        intro: 'The following shows the test loads and six test conditions. Airflow and noise are measured separately for each condition. The example fan is the Noctua A12x25 G2.',
        equipment: 'Test loads',
        panelsAlt: 'Exhaust panel on the left and intake panel on the right',
        panels: 'Exhaust and intake panels',
        panelsDescription: 'The exhaust panel has a 70% open area. The intake panel uses 34-mesh PET and has a 10 mm spacer.',
        towerAlt: 'Arctic Freezer 36 air-cooler tower',
        tower: 'Air-cooler tower',
        towerDescription: 'The test uses an Arctic Freezer 36 air-cooler tower.',
        radiatorAlt: 'Test radiator',
        radiator: 'Radiator',
        radiatorDescription: 'Overall thickness: 27 mm; core thickness: 20 mm; fin density: 20 FPI.',
        conditions: 'Six test conditions',
        pairDescription: 'Each pair shows the airflow test on the left and the noise test on the right.',
        cases: {
          free: { name: 'Free air', description: 'The fan is tested without an added load.', airflow: 'Free air · Airflow test', noise: 'Free air · Noise test' },
          intake: { name: 'Intake', description: 'Simulates a fan drawing air through a case intake panel.', airflow: 'Intake · Airflow test', noise: 'Intake · Noise test' },
          exhaust: { name: 'Exhaust', description: 'Simulates a fan exhausting air through a case exhaust panel.', airflow: 'Exhaust · Airflow test', noise: 'Exhaust · Noise test' },
          tower: { name: 'Air cooler', description: 'The fan is tested with an air-cooler tower.', airflow: 'Air cooler · Airflow test', noise: 'Air cooler · Noise test' },
          push: { name: 'Radiator push', description: 'The fan pushes air through the radiator.', airflow: 'Radiator push · Airflow test', noise: 'Radiator push · Noise test' },
          pull: { name: 'Radiator pull', description: 'The fan pulls air through the radiator.', airflow: 'Radiator pull · Airflow test', noise: 'Radiator pull · Noise test' },
        },
        radiatorPanel: 'Intake panel installed on the opposite side of the radiator',
        radiatorSetup: 'Radiator test setup',
        radiatorSetupDescription: 'Both radiator push and pull tests use an intake panel on the opposite side of the radiator to simulate a radiator installed in a case.',
        whistle: 'Whistle test',
        whistleIntro: 'The reference is the speed and noise at an effective airflow of 40 CFM in the intake test. The exhaust and intake panels are then swapped to compare the noise caused by changing the panel nearest the fan inlet.',
        whistleReference: 'Intake reference: exhaust panel → intake panel → fan inlet',
        whistleTest: 'Whistle test: intake panel → exhaust panel → fan inlet',
        whistleMethod: 'In the intake test, the intake panel has a 10 mm spacer. In the whistle test, the exhaust panel is placed directly against the fan inlet, which may trigger whistling. Noise is recorded at the reference speed and compared with the intake test at the same speed. The increase in noise and the tonality measured at the whistle-test point are used to assess whistle severity.',
        notes: 'Notes',
        disclaimer: 'The images and descriptions on this page come from actual tests and records. As the equipment and environment are not standard laboratory conditions, results are suitable for relative comparison rather than strict calibration.',
      },
      dynamic: {
        purchaseLinks: 'Purchase links',
        noPurchaseLinks: 'No purchase links yet',
        noImage: 'No image available',
        fanPhoto: 'Fan photo',
        download: 'Download',
        viewDetails: 'Expand details',
        hideDetails: 'Collapse details',
        caution: 'Caution',
        whistle: 'Whistle',
        whistlePlay: 'Play whistle test recording',
        whistleStop: 'Stop whistle recording',
        whistleAudioUnavailable: 'No whistle test recording is available',
        whistleAudioError: 'Whistle audio playback failed',
        whistleNoData: 'No data',
        whistleTooltipTitle: 'Intake-obstruction whistle level: ',
        whistleTooltipTotal: 'Total: ',
        whistleTooltipSeparator: '; ',
        whistleTooltipIndex: 'Overall noise index: ',
        whistleTooltipExperimental: ' (experimental)',
        whistleTooltipSpeed: 'Test speed: {rpm} RPM ({airflow} CFM)',
        whistleTooltipDisclaimer: 'Note: the whistle level is the noise increase in the whistle test relative to the fitted normal-intake value at the same RPM. Data is for reference only; actual results vary with the PC case environment.',
        copiedModel: 'Model text copied',
        like: 'Like',
        unlike: 'Unlike',
        addToRadar: 'Add to radar',
        compositeRadarChart: 'Composite radar chart',
        compositeScore: 'Composite score',
        rgbLighting: 'RGB lighting: {value}',
        lightingLikes: 'RGB likes: {count}',
        conditionLikes: 'Condition likes',
        conditionHeat: 'Condition heat',
      },
    },
  };

  function normalizeLang(lang) {
    const raw = String(lang || '').trim().toLowerCase();
    if (raw.startsWith('en')) return 'en';
    if (raw.startsWith('zh')) return 'zh';
    return DEFAULT_LANG;
  }

  function detectSystemLang() {
    try {
      const langs = navigator.languages;
      const preferred = String((langs && langs[0]) || '').trim()
        || String(navigator.language || '').trim()
        || String(navigator.userLanguage || '').trim();
      if (preferred) return /^zh(?:-|$)/i.test(preferred) ? 'zh' : 'en';
    } catch (_) {}
    return DEFAULT_LANG;
  }

  function readStoredLang() {
    try {
      const raw = String(localStorage.getItem(STORAGE_KEY) || '').trim().toLowerCase();
      if (raw === 'en' || raw === 'zh') return raw;
      return null;
    } catch (_) {
      return null;
    }
  }

  function resolveInitialLang() {
    if (!isMultiLangEnabled()) return DEFAULT_LANG;
    const saved = readStoredLang();
    if (SUPPORTED.has(saved)) return saved;
    return detectSystemLang();
  }

  function setDocumentLang(lang) {
    const normalized = normalizeLang(lang);
    document.documentElement.lang = normalized === 'en' ? 'en' : 'zh-CN';
    document.documentElement.dataset.fcLang = normalized;
  }

  let currentLang = resolveInitialLang();
  setDocumentLang(currentLang);

  function getPath(obj, key) {
    return String(key || '').split('.').reduce((acc, part) => {
      if (!acc || typeof acc !== 'object') return undefined;
      return acc[part];
    }, obj);
  }

  function formatMessage(message, params) {
    if (!params || typeof message !== 'string') return message;
    return message.replace(/\{(\w+)\}/g, function (_, name) {
      return Object.prototype.hasOwnProperty.call(params, name) ? String(params[name]) : '';
    });
  }

  function t(key, params) {
    const msg = getPath(DICT[currentLang], key);
    const fallback = msg == null ? getPath(DICT[DEFAULT_LANG], key) : msg;
    return formatMessage(fallback == null ? key : fallback, params);
  }

  function isBlank(value) {
    return value == null || String(value).trim() === '';
  }

  function pickLocalizedField(obj, baseKey) {
    if (!obj || typeof obj !== 'object') return '';
    const lang = currentLang;
    const keys = lang === 'en'
      ? [`${baseKey}_en`, baseKey, `${baseKey}_zh`]
      : [`${baseKey}_zh`, baseKey, `${baseKey}_en`];
    for (let i = 0; i < keys.length; i += 1) {
      const value = obj[keys[i]];
      if (!isBlank(value)) return value;
    }
    return '';
  }

  function pickText(zhText, enText, fallback) {
    if (currentLang === 'en') return !isBlank(enText) ? enText : (!isBlank(zhText) ? zhText : (fallback || ''));
    return !isBlank(zhText) ? zhText : (!isBlank(enText) ? enText : (fallback || ''));
  }

  function applySafeHtml(el, html) {
    if (!el) return;
    const source = String(html || '');
    const frag = document.createDocumentFragment();
    const stack = [frag];
    const tokenRe = /<(br\s*\/?|\/?sup|a\b[^>]*|\/a)>/ig;
    let lastIndex = 0;
    let match;
    const appendText = (text) => {
      if (!text) return;
      stack[stack.length - 1].appendChild(document.createTextNode(text));
    };
    while ((match = tokenRe.exec(source))) {
      appendText(source.slice(lastIndex, match.index));
      const token = match[1] || '';
      const lower = token.toLowerCase();
      if (lower.startsWith('br')) {
        stack[stack.length - 1].appendChild(document.createElement('br'));
      } else if (lower === 'sup') {
        const sup = document.createElement('sup');
        stack[stack.length - 1].appendChild(sup);
        stack.push(sup);
      } else if (lower === '/sup') {
        if (stack.length > 1) stack.pop();
      } else if (lower.startsWith('a')) {
        const hrefMatch = token.match(/\bhref\s*=\s*"([^"]+)"/i);
        const classMatch = token.match(/\bclass\s*=\s*"([^"]+)"/i);
        const href = hrefMatch ? hrefMatch[1] : '';
        if (/^(https?:|mailto:|\/)/i.test(href)) {
          const a = document.createElement('a');
          a.setAttribute('href', href);
          if (classMatch && classMatch[1] === 'inline-link') a.className = 'inline-link';
          stack[stack.length - 1].appendChild(a);
          stack.push(a);
        }
      } else if (lower === '/a') {
        if (stack.length > 1) stack.pop();
      }
      lastIndex = tokenRe.lastIndex;
    }
    appendText(source.slice(lastIndex));
    el.replaceChildren(frag);
  }

  function translateElement(root) {
    if (!root || typeof root.querySelectorAll !== 'function') return;
    root.querySelectorAll('[data-i18n]').forEach((el) => {
      if (el.hasAttribute('data-i18n-html')) return;
      el.textContent = t(el.getAttribute('data-i18n'));
    });
    root.querySelectorAll('[data-i18n-html]').forEach((el) => {
      applySafeHtml(el, t(el.getAttribute('data-i18n-html')));
    });
    [
      ['data-i18n-title', 'title'],
      ['data-i18n-aria-label', 'aria-label'],
      ['data-i18n-placeholder', 'placeholder'],
      ['data-i18n-data-tooltip', 'data-tooltip'],
      ['data-i18n-alt', 'alt'],
    ].forEach(([dataAttr, attr]) => {
      root.querySelectorAll('[' + dataAttr + ']').forEach((el) => {
        el.setAttribute(attr, t(el.getAttribute(dataAttr)));
      });
    });
    root.querySelectorAll('[data-i18n-with-vars]').forEach((el) => {
      const key = el.getAttribute('data-i18n-with-vars');
      const vars = {};
      Array.from(el.attributes).forEach((attr) => {
        if (attr.name.startsWith('data-i18n-var-')) {
          vars[attr.name.replace('data-i18n-var-', '')] = attr.value;
        }
      });
      el.textContent = t(key, vars);
    });
  }

  function syncSwitches() {
    document.querySelectorAll('[data-fc-lang-switch]').forEach((wrap) => {
      wrap.hidden = !isMultiLangEnabled();
      wrap.querySelectorAll('[data-lang]').forEach((btn) => {
        const active = normalizeLang(btn.getAttribute('data-lang')) === currentLang;
        btn.classList.toggle('is-active', active);
        btn.setAttribute('aria-pressed', active ? 'true' : 'false');
      });
    });
  }

  function bindSwitches() {
    document.querySelectorAll('[data-fc-lang-switch]').forEach((wrap) => {
      if (wrap.dataset.fcLangBound === '1') return;
      wrap.dataset.fcLangBound = '1';
      wrap.addEventListener('click', function (event) {
        if (!isMultiLangEnabled()) return;
        const btn = event.target.closest('[data-lang]');
        if (!btn) return;
        event.preventDefault();
        setLang(btn.getAttribute('data-lang'));
      });
    });
  }

  function apply(root) {
    translateElement(root || document);
    syncSwitches();
  }

  function emitChange() {
    apply(document);
    const detail = { lang: currentLang };
    document.dispatchEvent(new CustomEvent('fc:languagechange', { detail }));
    listeners.forEach((fn) => {
      try { fn(currentLang); } catch (err) { console.error('[fancool-i18n] listener failed', err); }
    });
  }

  function setLang(lang, options) {
    const normalized = isMultiLangEnabled() ? normalizeLang(lang) : DEFAULT_LANG;
    const opts = options || {};
    if (!SUPPORTED.has(normalized)) return currentLang;
    if (normalized === currentLang && opts.force !== true) return currentLang;
    currentLang = normalized;
    setDocumentLang(currentLang);
    if (opts.persist !== false) {
      try { localStorage.setItem(STORAGE_KEY, currentLang); } catch (_) {}
    }
    emitChange();
    return currentLang;
  }

  function onChange(handler) {
    if (typeof handler !== 'function') return function () {};
    listeners.add(handler);
    return function () { listeners.delete(handler); };
  }

  function localizeScenario(item) {
    if (!item || typeof item !== 'object') return { name: '', type: '', location: '' };
    return {
      name: pickLocalizedField(item, 'condition_name') || item.condition || '',
      type: pickLocalizedField(item, 'resistance_type') || item.resistance_type || '',
      location: pickLocalizedField(item, 'resistance_location') || item.resistance_location || '',
    };
  }

  function localizeModelLabel(item) {
    if (!item || typeof item !== 'object') return '';
    const brand = pickLocalizedField(item, 'brand_name') || item.brand || '';
    const model = pickLocalizedField(item, 'model_name') || item.model || '';
    return [brand, model].filter(Boolean).join(' ');
  }

  window.FcI18n = {
    STORAGE_KEY,
    getLang: function () { return currentLang; },
    isEnglish: function () { return currentLang === 'en'; },
    isMultiLangEnabled,
    detectSystemLang,
    readStoredLang,
    resolveInitialLang,
    setLang,
    onChange,
    t,
    apply,
    pickLocalizedField,
    pickText,
    localizeScenario,
    localizeModelLabel,
    bindSwitches,
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      bindSwitches();
      apply(document);
    }, { once: true });
  } else {
    bindSwitches();
    apply(document);
  }
})(window, document);
