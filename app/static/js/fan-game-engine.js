(() => {
  "use strict";

  const serverClient = window.fanGameClient || null;
  const remoteGame = serverClient && !serverClient.sandbox ? serverClient : null;
  const gameNow = () => remoteGame ? remoteGame.now() : Date.now();
  const canvas = document.getElementById("fan");
  const ctx = canvas.getContext("2d");
  const rpmText = document.getElementById("rpm");
  const turnsText = document.getElementById("turns");
  const speedFill = document.getElementById("speedFill");
  const speedProgress = document.getElementById("speedProgress");
  const rpmValue = document.getElementById("rpmValue");
  const liveIndicator = document.getElementById("liveIndicator");
  const message = document.getElementById("message");
  const resetButton = document.getElementById("reset");
  const shopStatus = document.getElementById("shopStatus");
  const shopSections = document.getElementById("shopSections");
  function renderServerRecords(records) {
    if (!records || !document.getElementById("serverTodayRpm")) return;
    for (const [id, key, decimals] of [
      ["serverTodayRpm", "today_rpm", 0], ["serverWeekRpm", "week_rpm", 0],
      ["serverTodayTurns", "today_turns", 2], ["serverWeekTurns", "week_turns", 2],
      ["serverTodayAirflow", "today_airflow", 1], ["serverWeekAirflow", "week_airflow", 1],
    ]) {
      document.getElementById(id).textContent = Number(records[key]).toLocaleString("zh-CN", {
        minimumFractionDigits: decimals, maximumFractionDigits: decimals,
      });
    }
  }
  const conditionGrid = document.getElementById("conditionGrid");
  const coastRpm = document.getElementById("coastRpm");
  const coastStatus = document.getElementById("coastStatus");
  const themeButton = document.getElementById("themeToggle");
  function applyTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    document.getElementById("themeColor").setAttribute("content", theme === "dark" ? "#111827" : "#f9fafb");
    themeButton.textContent = theme === "dark" ? "浅色主题" : "暗色主题";
    themeButton.setAttribute("aria-label", theme === "dark" ? "切换到浅色主题" : "切换到暗色主题");
  }
  themeButton.addEventListener("click", () => {
    const theme = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
    applyTheme(theme);
    draw();
    try {
      window.localStorage.setItem("theme", theme);
    } catch (error) {
      console.warn("无法保存主题偏好，本次切换仍然有效。", error);
    }
  });
  applyTheme(document.documentElement.getAttribute("data-theme"));
  const tuning = Object.fromEntries([
    "rootStrength", "hubFriction", "bearingInitialResistance", "bearingResistanceSlope",
    "ccwFingerForce", "cwFingerForce", "fingerCompression", "coggingStrength", "rotorInertia",
    "windNoiseGain", "bladeToneGain", "ballRustleGain", "airflowSoundExponent",
  ].map(key => [key, document.getElementById(key) || { value: "" }]));

  // Only the standalone demo uses this catalog; production receives the server's version.
  const DEMO_CATALOG = {
    version: "local-2026.11-5",
    tuning: {
      rootStrength: .3, hubFriction: .5, bearingInitialResistance: .00015,
      bearingResistanceSlope: .000001, ccwFingerForce: 2, cwFingerForce: .3,
      fingerCompression: 5, coggingStrength: .002, rotorInertia: .00006,
      windNoiseGain: 50, bladeToneGain: 100, ballRustleGain: .2, airflowSoundExponent: 2.5,
    },
    airflow: {
      referenceRpm: 3000,
      referenceCfm: 100,
      earningRpmCap: 3000,
      missingBladeExponent: 1.5,
      imbalancePenalty: .15,
    },
    sweat: { fullPressSeconds: 120, recoverSeconds: 60, referenceSpeed: .7, peakAt: .4, peakBoost: .2, soakedFriction: .65 },
    gearImpact: { thresholdMs: 1, durabilityPerMs2: 2 },
    materials: {
      print3d: {
        name: "3D 打印 PETG", price: 0, printSeconds: 30,
        rootStrength: .4, friction: 1.2, inertia: 0.8, airflow: .9,
        note: "层间结合弱、表面粗糙：好拨但易断，叶型精度差，风量较低。",
      },
      pbt: {
        name: "PBT（玻纤增强）", price: 30,
        rootStrength: 1, friction: 1, inertia: 1, airflow: .97,
        note: "常见量产风扇材质，各项均衡；本页调试参数以它为基准。",
      },
      lcp: {
        name: "LCP 液晶聚合物", price: 100,
        rootStrength: 1.4, friction: 0.9, inertia: 1.2, airflow: 1,
        note: "刚性高，叶片可做薄、叶尖间隙更小；表面光滑，较难拨快。",
      },
    },
    bearings: {
      sleeve: {
        name: "含油轴承", price: 0, resistance: 1, lifeTurns: 5000, wearA: 3, wearP: 2,
        service: { label: "补油", cost: 2, keepWearFraction: .3 },
        note: "便宜常见；润滑油逐渐流失后阻力上升较早，补油可部分恢复。",
      },
      fdb: {
        name: "液压轴承（FDB）", price: 50, resistance: .7, lifeTurns: 7000, wearA: 2, wearP: 4,
        service: { label: "更换", cost: 25, keepWearFraction: 0 },
        note: "油膜承载、阻力低且安静；前期几乎不衰减，寿命末期快速变差。",
      },
      ball: {
        name: "双滚珠轴承", price: 80, resistance: .85, lifeTurns: 20000, wearA: 1.5, wearP: 1.5,
        service: { label: "更换", cost: 40, keepWearFraction: 0 },
        note: "寿命最长、耐高温；阻力略高于 FDB，磨损过程最平缓。",
      },
    },
    hands: {
      hand: {
        name: "裸手", price: 0, friction: 1, sweatEffect: 1, compressionBonusMm: 0,
        note: "按住操作时会出汗：微湿时摩擦略升，汗多后明显打滑。松手休息或用止滑粉。",
      },
      cots: {
        name: "防滑指套", price: 20, replaceCost: 8, contactMinutes: 30,
        friction: 1.3, sweatEffect: .3, compressionBonusMm: 1,
        note: "橡胶指套增加摩擦并隔开大部分汗液；按接触时间和冲击磨损。",
      },
      gloves: {
        name: "橡胶手套", price: 60, replaceCost: 25, contactMinutes: 120,
        friction: 1.2, sweatEffect: .05, compressionBonusMm: 3,
        note: "几乎不受汗影响，更厚的缓冲更护叶片；手感变钝。",
      },
    },
    consumables: {
      powder: { name: "止滑粉", price: 1, pressSeconds: 90 },
    },
  };
  let GAME_CATALOG = serverClient ? serverClient.catalog : DEMO_CATALOG;
  const MATERIAL_ORDER = ["print3d", "pbt", "lcp"];
  const BEARING_ORDER = ["sleeve", "fdb", "ball"];
  const HAND_ORDER = ["hand", "cots", "gloves"];
  const SAVE_KEY = "fanSpinnerSave.v2";
  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

  const BLADE_COUNT = 7;
  const MAX_RPM = 3000;
  const REVERSE_MAX_RPM = 300;
  // Geometry in millimetres: 120 mm frame, R36 edge arcs through the axis offset by 40°, Ø45 hub.
  const FRAME_MM = 120;
  const DRAWING_SCALE = .98;
  const FRAME_CORNER_MM = 6;
  const PAD_HALF_MM = 59;
  const PAD_INNER_MM = 70;
  const PAD_OUTER_CORNER_MM = 5;
  const PAD_INNER_CORNER_MM = 1;
  const HOLE_PITCH_MM = 105;
  const HOLE_RADIUS_MM = 2.5;
  const MM = DRAWING_SCALE / FRAME_MM;
  const HUB_MM = 22.5;
  const TIP_MM = 57.5;
  const EDGE_ARC_MM = 36;
  // World-space [x, y, radius, strength] in mm (canvas y down), forming a blurred
  // reflection locus that sweeps tip -> mid-span -> root down the left side.
  const SPECULAR_SPOTS = [
    [-30, 0, 60, .35],
  ];
  const EDGE_OFFSET = 40 * Math.PI / 180;
  const BORE_MM = 58;
  const STRUT_COUNT = 4;
  const STRUT_WIDTH_MM = 6;
  const STRUT_INNER_MM = 18;
  const STRUT_ANGLE = -Math.PI * 300 / 360;
  const STRUT_SKEW = 60 * Math.PI / 180;
  const BORE_EDGE_MM = 1;
  const LEAD_TIP_FILLET_MM = 1;
  const TRAIL_TIP_FILLET_MM = 5;
  const HUB_RADIUS = HUB_MM * MM;
  const FINGER_RADIUS_MM = 2;
  const CONTACT_RELEASE_MS = 30;
  // Reference: Noctua NF-A12x25 PWM spec, 2000 rpm at 1.2 W typical input.
  // Motor/driver efficiency is an assumption, so this is an order-of-magnitude calibration.
  const AIR_DRAG_REFERENCE = { rpm: 2000, typicalInputW: 1.2, assumedEfficiency: .6 };
  const AIR_DRAG = AIR_DRAG_REFERENCE.typicalInputW * AIR_DRAG_REFERENCE.assumedEfficiency
    / Math.pow(AIR_DRAG_REFERENCE.rpm * Math.PI / 30, 3);
  const PHYSICS_STEP = .001;
  const IMPACT_PEAK_FACTOR = 2;
  const MIN_POINTER_SEGMENT_MS = 8;
  const FINGER_DAMPING_RATIO = .35;

  function pointerEventTime(event) {
    const now = performance.now();
    const stamp = Number(event && event.timeStamp);
    return Number.isFinite(stamp) && stamp > 0 && stamp <= now + 1 && now - stamp < 1000 ? stamp : now;
  }
  const LEADING_EDGE_END_SEGMENT = 48;
  const BLADE = buildBladeGeometry();
  const BLADE_WINDING = Math.sign(BLADE.points.reduce((sum, point, index) => {
    const next = BLADE.points[(index + 1) % BLADE.points.length];
    return sum + point[0] * next[1] - next[0] * point[1];
  }, 0)) || 1;
  const BLADE_BOUNDS = BLADE.points.reduce((bounds, point) => ({
    minX: Math.min(bounds.minX, point[0]),
    maxX: Math.max(bounds.maxX, point[0]),
    minY: Math.min(bounds.minY, point[1]),
    maxY: Math.max(bounds.maxY, point[1]),
  }), { minX: Infinity, maxX: -Infinity, minY: Infinity, maxY: -Infinity });
  const BLADE_SEGMENTS = BLADE.points.map((end, index) => {
    const start = BLADE.points[(index + BLADE.points.length - 1) % BLADE.points.length];
    const dx = end[0] - start[0];
    const dy = end[1] - start[1];
    const lengthSquared = dx * dx + dy * dy;
    const projection = lengthSquared
      ? Math.min(1, Math.max(0, -(start[0] * dx + start[1] * dy) / lengthSquared))
      : 0;
    const closestX = start[0] + projection * dx;
    const closestY = start[1] + projection * dy;
    return {
      startX: start[0],
      startY: start[1],
      endX: end[0],
      endY: end[1],
      dx,
      dy,
      lengthSquared,
      minRadius: Math.hypot(closestX, closestY),
      maxRadius: Math.max(Math.hypot(start[0], start[1]), Math.hypot(end[0], end[1])),
    };
  });
  const BLADE_PATH = new Path2D(BLADE.path);
  const MATERIAL_TEXTURES = buildMaterialTextures();
  const MATERIAL_TEXTURE_BITMAPS = { pixelsPerMm: 0, layers: new Map() };
  const DUST = makeDust();

  // Blade-local paths are rasterized once per display resolution, not on every rotation frame.
  function buildMaterialTextures() {
    const pathData = lines => lines.map(points => points
      .map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(2)} ${y.toFixed(2)}`).join("")).join("");
    const angles = BLADE.points.map(([x, y]) => Math.atan2(y, x));
    const meanAngle = Math.atan2(
      angles.reduce((sum, angle) => sum + Math.sin(angle), 0),
      angles.reduce((sum, angle) => sum + Math.cos(angle), 0),
    );
    const relative = angles.map(angle => Math.atan2(Math.sin(angle - meanAngle), Math.cos(angle - meanAngle)));
    const minAngle = Math.min(...relative) - .05;
    const maxAngle = Math.max(...relative) + .05;

    // Circumferential print layers: concentric arcs around the axis, clipped by the blade outline.
    const layerArc = radius => {
      const points = [];
      const segments = 24;
      for (let s = 0; s <= segments; s += 1) {
        const angle = meanAngle + minAngle + (maxAngle - minAngle) * s / segments;
        points.push([radius * Math.cos(angle), radius * Math.sin(angle)]);
      }
      return points;
    };
    const layerStep = .55;
    const ridges = [];
    const grooves = [];
    for (let radius = HUB_MM - 2; radius <= TIP_MM + 1.5; radius += layerStep) {
      ridges.push(layerArc(radius));
      grooves.push(layerArc(radius + layerStep * .45));
    }

    // Each blade has a stable, slightly different flow origin near its root.
    let seed = 7;
    const random = () => {
      seed = (seed * 16807) % 2147483647;
      return seed / 2147483647;
    };
    const insideBlade = (x, y) => {
      let inside = false;
      for (let i = 0, j = BLADE.points.length - 1; i < BLADE.points.length; j = i, i += 1) {
        const [xi, yi] = BLADE.points[i];
        const [xj, yj] = BLADE.points[j];
        if ((yi > y) !== (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi) inside = !inside;
      }
      return inside;
    };
    const lcp = Array.from({ length: BLADE_COUNT }, () => {
      const brightStreaks = [];
      const darkStreaks = [];
      // The flow source sits under the hub cap, near this blade's root, so only its outward fan shows.
      const rootPoints = BLADE.points.filter(([x, y]) => Math.hypot(x, y) < HUB_MM + 1);
      const rootAngle = Math.atan2(
        rootPoints.reduce((sum, [x, y]) => sum + y / Math.hypot(x, y), 0),
        rootPoints.reduce((sum, [x, y]) => sum + x / Math.hypot(x, y), 0),
      );
      const originRadius = HUB_MM - 7 - random() * 5;
      const originAngle = rootAngle + (random() - .5) * .35;
      const origin = [originRadius * Math.cos(originAngle), originRadius * Math.sin(originAngle)];
      const [cx, cy] = origin;
      const direction = Math.atan2(cy, cx);
      const phaseA = random() * Math.PI * 2;
      const phaseB = random() * Math.PI * 2;
      const bend = (theta, r) => .035 * Math.sin(r * .13 + theta * 3 + phaseA)
        + .015 * Math.sin(r * .24 - theta * 5 + phaseB);
      const pointAt = (theta, r) => {
        const heading = theta + bend(theta, r) - bend(theta, 0);
        return [cx + r * Math.cos(heading), cy + r * Math.sin(heading)];
      };
      // Blade span along each direction from the hidden origin.
      const spanOf = theta => {
        let enter = 0;
        let reach = 0;
        for (let r = .5; r < 2 * TIP_MM; r += .5) {
          if (!insideBlade(...pointAt(theta, r))) continue;
          if (!enter) enter = r;
          reach = r;
        }
        return { enter, reach };
      };
      const angleSteps = 240;
      const spreadLimit = Math.PI;
      const directions = Array.from({ length: angleSteps }, (_, i) => {
        const theta = direction - spreadLimit + 2 * spreadLimit * (i + .5) / angleSteps;
        return { theta, ...spanOf(theta) };
      }).filter(item => item.reach - item.enter > 1.5);
      // Rays per direction ∝ visible span and starts staggered along each ray, so the diverging fan
      // keeps a roughly constant line density across the whole blade instead of thinning out.
      const cumulative = [];
      let totalReach = 0;
      for (const item of directions) cumulative.push(totalReach += item.reach - item.enter);
      const rays = 260;
      for (let i = 0; i < rays; i += 1) {
        const target = totalReach * (i + random()) / rays;
        const k = cumulative.findIndex(value => value >= target);
        const { theta: baseTheta, enter, reach } = directions[Math.max(0, k)];
        const theta = baseTheta + (random() - .5) * 2 * spreadLimit / angleSteps;
        const end = reach + 1.5;
        const start = i % 4 === 0 ? Math.max(.5, enter - 3) : enter + random() * (reach - enter) * .9;
        const line = [];
        const segments = Math.max(2, Math.ceil((end - start) / 1.2));
        for (let s = 0; s <= segments; s += 1) {
          const r = start + (end - start) * s / segments;
          line.push(pointAt(theta, r));
        }
        (random() < .55 ? brightStreaks : darkStreaks).push(line);
      }
      return { origin, bright: new Path2D(pathData(brightStreaks)), dark: new Path2D(pathData(darkStreaks)) };
    });
    return {
      print3d: { ridges: new Path2D(pathData(ridges)), grooves: new Path2D(pathData(grooves)) },
      lcp,
    };
  }

  function materialTextureBitmaps(material, index, mmScale) {
    const pixelsPerMm = Math.ceil(mmScale * state.dpr);
    if (MATERIAL_TEXTURE_BITMAPS.pixelsPerMm !== pixelsPerMm) {
      MATERIAL_TEXTURE_BITMAPS.layers.clear();
      MATERIAL_TEXTURE_BITMAPS.pixelsPerMm = pixelsPerMm;
    }
    const key = material === "lcp" ? `${material}:${index}` : material;
    if (MATERIAL_TEXTURE_BITMAPS.layers.has(key)) return MATERIAL_TEXTURE_BITMAPS.layers.get(key);
    const texture = material === "lcp" ? MATERIAL_TEXTURES.lcp[index] : MATERIAL_TEXTURES.print3d;
    const definitions = material === "lcp"
      ? [[texture.dark, .18, "rgba(0, 0, 0, .25)"], [texture.bright, .18, "rgb(235, 235, 235)"]]
      : [[texture.grooves, .3, "rgba(0, 0, 0, .28)"], [texture.ridges, .2, "rgb(210, 210, 210)"]];
    const x = BLADE_BOUNDS.minX - .5;
    const y = BLADE_BOUNDS.minY - .5;
    const layers = definitions.map(([path, width, color]) => {
      const image = document.createElement("canvas");
      image.width = Math.ceil((BLADE_BOUNDS.maxX - x + .5) * pixelsPerMm);
      image.height = Math.ceil((BLADE_BOUNDS.maxY - y + .5) * pixelsPerMm);
      const context = image.getContext("2d");
      context.setTransform(pixelsPerMm, 0, 0, pixelsPerMm, -x * pixelsPerMm, -y * pixelsPerMm);
      context.clip(BLADE_PATH);
      context.lineWidth = width;
      context.strokeStyle = color;
      context.stroke(path);
      return image;
    });
    const bitmap = { layers, x, y, pixelsPerMm };
    MATERIAL_TEXTURE_BITMAPS.layers.set(key, bitmap);
    return bitmap;
  }

  function drawMaterialTexture(material, directionalLight, index, mmScale) {
    if (material !== "print3d" && material !== "lcp") return;
    const bitmap = materialTextureBitmaps(material, index, mmScale);
    let highlightAlpha;
    if (material === "print3d") {
      const ridgeLight = smoothstep(-1, 1, directionalLight);
      highlightAlpha = .03 + ridgeLight * .05;
    } else {
      // Anisotropic sheen: the radial streaks flash as they turn through the fixed light.
      const sheen = smoothstep(-.2, .9, directionalLight);
      highlightAlpha = .035 + sheen * .065;
    }
    const paint = image => ctx.drawImage(image, bitmap.x, bitmap.y,
      image.width / bitmap.pixelsPerMm, image.height / bitmap.pixelsPerMm);
    if (material === "print3d") paint(bitmap.layers[0]);
    ctx.save();
    ctx.globalAlpha *= highlightAlpha;
    paint(bitmap.layers[1]);
    ctx.restore();
    if (material === "lcp") paint(bitmap.layers[0]);
  }

  function buildBladeGeometry() {
    const R = EDGE_ARC_MM;
    const rootRadius = HUB_MM - 1.5;
    const leadAngle = 150 * Math.PI / 180;
    const lead = [R * Math.cos(leadAngle), R * Math.sin(leadAngle)];
    const trail = [R * Math.cos(leadAngle - EDGE_OFFSET), R * Math.sin(leadAngle - EDGE_OFFSET)];
    const polar = (radius, angle) => [radius * Math.cos(angle), radius * Math.sin(angle)];
    const angleOf = (p, c = [0, 0]) => Math.atan2(p[1] - c[1], p[0] - c[0]);
    const dist = (p, c = [0, 0]) => Math.hypot(p[0] - c[0], p[1] - c[1]);
    // A point at distance d from the axis on an edge circle lies 2R·cosφ away, at circle angle − φ.
    const onEdge = (center, radius) => polar(radius, angleOf(center) - Math.acos(radius / (2 * R)));
    // Fillet centre: on the tip-side circle and at the given distance from the edge-circle centre.
    const filletCenter = (center, filletRadius, edgeDistance) => {
      const a = TIP_MM - filletRadius;
      const alpha = Math.acos((a * a + R * R - edgeDistance * edgeDistance) / (2 * a * R));
      return polar(a, angleOf(center) - alpha);
    };
    const towards = (from, to, radius) => {
      const angle = angleOf(to, from);
      return [from[0] + radius * Math.cos(angle), from[1] + radius * Math.sin(angle)];
    };
    const points = [];
    const arc = (center, radius, from, to, steps) => {
      const a0 = angleOf(from, center);
      let delta = angleOf(to, center) - a0;
      delta = Math.atan2(Math.sin(delta), Math.cos(delta));
      for (let i = 0; i <= steps; i += 1) points.push(polar(radius, a0 + delta * i / steps).map((v, k) => v + center[k]));
    };

    // The blade sits outside the leading circle and inside the trailing circle.
    const leadFillet = filletCenter(lead, LEAD_TIP_FILLET_MM, R + LEAD_TIP_FILLET_MM);
    const trailFillet = filletCenter(trail, TRAIL_TIP_FILLET_MM, R - TRAIL_TIP_FILLET_MM);
    const leadRoot = onEdge(lead, rootRadius);
    const leadEdgeTangent = towards(lead, leadFillet, R);
    const leadTipTangent = towards([0, 0], leadFillet, TIP_MM);
    const trailTipTangent = towards([0, 0], trailFillet, TIP_MM);
    const trailEdgeTangent = towards(trail, trailFillet, R);
    const trailRoot = onEdge(trail, rootRadius);

    arc(lead, R, leadRoot, leadEdgeTangent, 48);
    arc(leadFillet, LEAD_TIP_FILLET_MM, leadEdgeTangent, leadTipTangent, 8);
    arc([0, 0], TIP_MM, leadTipTangent, trailTipTangent, 32);
    arc(trailFillet, TRAIL_TIP_FILLET_MM, trailTipTangent, trailEdgeTangent, 16);
    arc(trail, R, trailEdgeTangent, trailRoot, 48);
    arc([0, 0], rootRadius, trailRoot, leadRoot, 12);

    // Canvas y points down; mirroring keeps the drawing's visual CCW forward sweep.
    const canvasPoints = points.map(([x, y]) => [x, -y]);
    const shadeReach = Math.max(...points.map((p) => dist(p, lead)));
    return {
      path: canvasPoints.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(3)} ${y.toFixed(3)}`).join(" ") + " Z",
      points: canvasPoints,
      leadCenter: [lead[0], -lead[1]],
      shadeReach,
    };
  }
  function makeDust() {
    let seed = 1207;
    const random = () => {
      seed = (seed * 1664525 + 1013904223) >>> 0;
      return seed / 0x100000000;
    };
    return Array.from({ length: 28 }, () => {
      const angle = random() * Math.PI * 2;
      const radius = Math.sqrt(.06 + random() * .78);
      return {
        x: Math.cos(angle) * radius,
        y: Math.sin(angle) * radius,
        size: .002 + random() * .006,
        alpha: .12 + random() * .25,
      };
    });
  }
  let pendingFeedbackNotice = null;
  let messageReady = false;
  const state = {
    omega: 0,
    angle: 0,
    totalRadians: serverClient ? serverClient.snapshot.total_turns * Math.PI * 2 : 0,
    brokenBlades: Array(BLADE_COUNT).fill(null),
    flyouts: [],
    pointer: null,
    lastFrame: 0,
    lastReadout: 0,
    animationTime: 0,
    size: 0,
    dpr: 1,
    framePending: false,
    campaign: loadCampaign(),
    lastSaveAt: 0,
    lastWorkshopReadout: 0,
    breakNotice: null,
    feedbackNotice: pendingFeedbackNotice,
    controlPresses: new Set(),
    controlsNeedRefresh: false,
    sessionStartAirflow: 0,
    sessionMaxRpm: 0,
    impactWear: 0,
  };
  messageReady = true;
  if (state.feedbackNotice) renderMessage();
  function restoreBrokenBlades(indices) {
    state.brokenBlades.fill(null);
    for (const index of indices) state.brokenBlades[index] = { stubRadiusMm: HUB_MM + 2, seed: 0 };
  }
  function currentBrokenIndices() {
    return state.brokenBlades.map((damage, index) => damage ? index : null).filter(index => index !== null);
  }
  // Owned impellers are kept in inventory with their own broken-blade state; fresh=true installs a new set.
  function installImpeller(id, fresh) {
    const campaign = state.campaign;
    if (id !== campaign.material) campaign.storedBrokenBlades[campaign.material] = currentBrokenIndices();
    if (!campaign.ownedMaterials.includes(id)) campaign.ownedMaterials.push(id);
    campaign.material = id;
    if (id === "print3d" && !campaign.printReadyAt) {
      campaign.printReadyAt = gameNow() + GAME_CATALOG.materials.print3d.printSeconds * 1000;
    }
    restoreBrokenBlades(fresh ? [] : campaign.storedBrokenBlades[id] || []);
    delete campaign.storedBrokenBlades[id];
  }
  state.sessionStartAirflow = state.campaign.totalAirflow;
  restoreBrokenBlades(state.campaign.brokenBlades);

  const rpm = () => Math.abs(state.omega) * 60 / (Math.PI * 2);
  // Canvas angles increase clockwise; the fan's forward direction is counterclockwise.
  const signedRpm = () => -state.omega * 60 / (Math.PI * 2);
  function currentDateKey() {
    const now = new Date();
    return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
  }
  function createCampaign() {
    return {
      balance: 0,
      totalAirflow: 0,
      todayAirflow: 0,
      todayMaxRpm: 0,
      bestRpm: 0,
      todayMaxAirflow: 0,
      bestAirflow: 0,
      date: currentDateKey(),
      material: "print3d",
      ownedMaterials: ["print3d"],
      bearing: "sleeve",
      ownedBearings: ["sleeve"],
      hand: "hand",
      ownedHands: ["hand"],
      bearingTurns: { sleeve: 0, fdb: 0, ball: 0 },
      handDurability: { cots: 100, gloves: 100 },
      handContactSeconds: { cots: 0, gloves: 0 },
      sweat: 0,
      sweatRestAt: gameNow(),
      powderCount: 0,
      powderSeconds: 0,
      printReadyAt: gameNow() + GAME_CATALOG.materials.print3d.printSeconds * 1000,
      brokenBlades: [],
      storedBrokenBlades: {},
    };
  }
  function sanitizeBladeIndices(values) {
    return Array.isArray(values)
      ? [...new Set(values.filter(index => Number.isInteger(index) && index >= 0 && index < BLADE_COUNT))]
      : [];
  }
  function loadCampaign() {
    const initial = createCampaign();
    if (serverClient) return JSON.parse(JSON.stringify(serverClient.snapshot.campaign));
    try {
      const raw = window.localStorage.getItem(SAVE_KEY);
      if (!raw) return initial;
      const saved = JSON.parse(raw);
      if (!saved || typeof saved !== "object" || saved.version !== 1) return initial;
      const nonnegative = value => Number.isFinite(Number(value)) ? Math.max(0, Number(value)) : 0;
      const known = (value, catalog) => Object.prototype.hasOwnProperty.call(catalog, value) ? value : null;
      const uniqueKnown = (values, catalog, fallback) => Array.isArray(values)
        ? [...new Set(values.filter(value => Object.prototype.hasOwnProperty.call(catalog, value)).concat(fallback))]
        : [...fallback];
      const result = {
        ...initial,
        balance: nonnegative(saved.balance),
        totalAirflow: nonnegative(saved.totalAirflow),
        todayAirflow: nonnegative(saved.todayAirflow),
        todayMaxRpm: nonnegative(saved.todayMaxRpm),
        bestRpm: nonnegative(saved.bestRpm),
        todayMaxAirflow: nonnegative(saved.todayMaxAirflow),
        bestAirflow: nonnegative(saved.bestAirflow),
        date: typeof saved.date === "string" ? saved.date : initial.date,
        material: known(saved.material, GAME_CATALOG.materials) || initial.material,
        ownedMaterials: uniqueKnown(saved.ownedMaterials, GAME_CATALOG.materials, initial.ownedMaterials),
        bearing: known(saved.bearing, GAME_CATALOG.bearings) || initial.bearing,
        ownedBearings: uniqueKnown(saved.ownedBearings, GAME_CATALOG.bearings, initial.ownedBearings),
        hand: known(saved.hand, GAME_CATALOG.hands) || initial.hand,
        ownedHands: uniqueKnown(saved.ownedHands, GAME_CATALOG.hands, initial.ownedHands),
        bearingTurns: { ...initial.bearingTurns },
        handDurability: { ...initial.handDurability },
        handContactSeconds: { ...initial.handContactSeconds },
        sweat: clamp(nonnegative(saved.sweat), 0, 1),
        sweatRestAt: nonnegative(saved.sweatRestAt) || gameNow(),
        powderCount: Math.floor(nonnegative(saved.powderCount)),
        powderSeconds: nonnegative(saved.powderSeconds),
        printReadyAt: nonnegative(saved.printReadyAt),
        brokenBlades: sanitizeBladeIndices(saved.brokenBlades),
        storedBrokenBlades: {},
      };
      if (result.material === "print3d" && !result.printReadyAt) {
        result.printReadyAt = gameNow() + GAME_CATALOG.materials.print3d.printSeconds * 1000;
      }
      if (!result.ownedMaterials.includes(result.material)) result.ownedMaterials.push(result.material);
      for (const id of result.ownedMaterials) {
        if (id !== result.material) {
          result.storedBrokenBlades[id] = sanitizeBladeIndices(saved.storedBrokenBlades && saved.storedBrokenBlades[id]);
        }
      }
      for (const id of Object.keys(result.bearingTurns)) result.bearingTurns[id] = nonnegative(saved.bearingTurns && saved.bearingTurns[id]);
      for (const id of Object.keys(result.handDurability)) result.handDurability[id] = clamp(
        nonnegative(saved.handDurability && saved.handDurability[id]), 0, 100,
      );
      for (const id of Object.keys(result.handContactSeconds)) {
        result.handContactSeconds[id] = nonnegative(saved.handContactSeconds && saved.handContactSeconds[id]);
      }
      return result;
    } catch (error) {
      console.warn("无法读取风扇工坊存档，将使用新存档。", error);
      setShopStatus("无法读取本地存档，本次进度仅保留在当前页面。", true);
      return initial;
    }
  }
  function setShopStatus(text, isError = false) {
    shopStatus.textContent = text;
    shopStatus.classList.toggle("is-error", isError);
    pendingFeedbackNotice = { text, isError };
    if (messageReady) {
      state.feedbackNotice = pendingFeedbackNotice;
      renderMessage();
    }
  }
  function renderMessage() {
    const notice = state.breakNotice && performance.now() < state.breakNotice.until
      ? state.breakNotice.text
      : state.feedbackNotice ? state.feedbackNotice.text : "";
    const brokenCount = state.brokenBlades.filter(Boolean).length;
    const brokenText = brokenCount ? `已有 ${brokenCount} 片叶片断裂` : "";
    message.textContent = notice && brokenText ? `${notice}${brokenText}` : notice || brokenText;
    message.classList.toggle("is-error", Boolean(
      state.breakNotice && performance.now() < state.breakNotice.until
        || state.feedbackNotice && state.feedbackNotice.isError,
    ));
  }
  function persistCampaign(force = false) {
    if (serverClient) return;
    const now = performance.now();
    if (!force && now - state.lastSaveAt < 5000) return;
    state.campaign.brokenBlades = state.brokenBlades
      .map((damage, index) => damage ? index : null)
      .filter(index => index !== null);
    const data = { version: 1, ...state.campaign };
    try {
      window.localStorage.setItem(SAVE_KEY, JSON.stringify(data));
      state.lastSaveAt = now;
    } catch (error) {
      console.warn("无法保存风扇工坊存档，本次变化仅保留在当前页面。", error);
      setShopStatus("浏览器阻止了本地存档；当前页面关闭后，进度可能无法保留。", true);
    }
  }
  let maintenanceTimer = null;
  function syncSweatRecovery() {
    const campaign = state.campaign;
    if (state.pointer || !campaign.sweatRestAt) return;
    const now = gameNow();
    const elapsed = Math.max(0, now - campaign.sweatRestAt) / 1000;
    campaign.sweat = Math.max(0, campaign.sweat - elapsed / GAME_CATALOG.sweat.recoverSeconds);
    campaign.sweatRestAt = now;
  }
  function scheduleMaintenance() {
    if (maintenanceTimer !== null) window.clearTimeout(maintenanceTimer);
    maintenanceTimer = null;
    const campaign = state.campaign;
    const printPending = campaign.printReadyAt > gameNow();
    if (!printPending && (state.pointer || campaign.sweat <= 0)) return;
    maintenanceTimer = window.setTimeout(() => {
      maintenanceTimer = null;
      syncSweatRecovery();
      renderWorkshop();
      persistCampaign(true);
      scheduleMaintenance();
    }, printPending ? Math.min(1000, Math.max(1, campaign.printReadyAt - gameNow())) : 1000);
  }
  function refreshDailyStats() {
    if (remoteGame) return;
    const today = currentDateKey();
    if (state.campaign.date !== today) {
      state.campaign.date = today;
      state.campaign.todayAirflow = 0;
      state.campaign.todayMaxRpm = 0;
      state.campaign.todayMaxAirflow = 0;
    }
  }
  function brokenBladeFactor() {
    const broken = state.brokenBlades
      .map((damage, index) => damage ? index * Math.PI * 2 / BLADE_COUNT : null)
      .filter(angle => angle !== null);
    if (!broken.length) return 1;
    const imbalance = Math.hypot(
      broken.reduce((sum, angle) => sum + Math.cos(angle), 0),
      broken.reduce((sum, angle) => sum + Math.sin(angle), 0),
    ) / BLADE_COUNT;
    return ((BLADE_COUNT - broken.length) / BLADE_COUNT) ** GAME_CATALOG.airflow.missingBladeExponent
      * (1 - GAME_CATALOG.airflow.imbalancePenalty * imbalance);
  }
  function intactBladeFraction() {
    return state.brokenBlades.filter(damage => !damage).length / BLADE_COUNT;
  }
  function airflowFactor() {
    return GAME_CATALOG.materials[state.campaign.material].airflow * brokenBladeFactor();
  }
  function bearingWearMultiplier(id = state.campaign.bearing) {
    const bearing = GAME_CATALOG.bearings[id];
    const progress = state.campaign.bearingTurns[id] / bearing.lifeTurns;
    return Math.min(1 + bearing.wearA * progress ** bearing.wearP, 1 + 2 * bearing.wearA);
  }
  function handFrictionFactor() {
    const gear = effectiveHandGear();
    return gear.friction * handTractionFactor();
  }
  function handTractionFactor() {
    const gear = effectiveHandGear();
    const sweat = state.campaign.sweat * gear.sweatEffect;
    const { peakAt, peakBoost, soakedFriction } = GAME_CATALOG.sweat;
    const sweatFactor = sweat < peakAt
      ? 1 + peakBoost * Math.sin(Math.PI * sweat / peakAt)
      : 1 + (soakedFriction - 1) * (sweat - peakAt) / (1 - peakAt);
    const powderFactor = state.campaign.powderSeconds > 0 ? 1.3 : 1;
    return sweatFactor * powderFactor;
  }
  function effectiveHandGear() {
    const hand = state.campaign.hand;
    if (hand !== "hand" && state.campaign.handDurability[hand] <= 0) return GAME_CATALOG.hands.hand;
    return GAME_CATALOG.hands[hand];
  }
  function currentAirflowCfm() {
    const forwardRpm = Math.max(0, signedRpm());
    return forwardRpm / GAME_CATALOG.airflow.referenceRpm * GAME_CATALOG.airflow.referenceCfm
      * airflowFactor();
  }
  function updateEconomy(dt) {
    if (!(dt > 0) || !Number.isFinite(dt)) return;
    refreshDailyStats();
    const campaign = state.campaign;
    const cfm = currentAirflowCfm();
    const earned = Math.min(forwardAirflowCap(), cfm) * dt / 60;
    if (!remoteGame) campaign.balance += earned;
    campaign.totalAirflow += earned;
    campaign.todayAirflow += earned;
    const forwardRpm = Math.max(0, signedRpm());
    const currentRpm = Math.min(forwardRpm, GAME_CATALOG.airflow.earningRpmCap);
    campaign.todayMaxRpm = Math.max(campaign.todayMaxRpm, forwardRpm);
    campaign.bestRpm = Math.max(campaign.bestRpm, forwardRpm);
    campaign.todayMaxAirflow = Math.max(campaign.todayMaxAirflow, cfm);
    campaign.bestAirflow = Math.max(campaign.bestAirflow, cfm);
    state.sessionMaxRpm = Math.max(state.sessionMaxRpm, forwardRpm);
    const pointer = state.pointer;
    let effort = 0;
    if (pointer) {
      const age = Math.max(0, performance.now() - (pointer.gestureMotionAt || 0));
      const speed = (pointer.gestureSpeed || 0) * Math.exp(-age / 100);
      effort = .6 + .4 * Math.min(speed / GAME_CATALOG.sweat.referenceSpeed, 1);
      campaign.sweat = clamp(campaign.sweat + dt * effort / GAME_CATALOG.sweat.fullPressSeconds, 0, 1);
      if (campaign.powderSeconds > 0) campaign.powderSeconds = Math.max(0, campaign.powderSeconds - dt);
      const hand = campaign.hand;
      if (pointer.physicalContact && hand !== "hand" && campaign.handDurability[hand] > 0) {
        campaign.handContactSeconds[hand] += dt;
        const lifetimeSeconds = GAME_CATALOG.hands[hand].contactMinutes * 60;
        campaign.handDurability[hand] = Math.max(
          0,
          campaign.handDurability[hand] - dt / lifetimeSeconds * 100,
        );
      }
    }
    if (remoteGame) {
      remoteGame.record({
        duration_ms: dt * 1000,
        turns: Math.abs(state.omega) / (Math.PI * 2) * dt,
        forward_turns: currentRpm / 60 * dt,
        peak_rpm: rpm(),
        peak_forward_rpm: Math.max(0, signedRpm()),
        peak_airflow: cfm,
        end_rpm: signedRpm(),
        press_ms: pointer ? dt * 1000 : 0,
        contact_ms: pointer && pointer.physicalContact ? dt * 1000 : 0,
        effort_ms: effort * dt * 1000,
        impact_wear: state.impactWear,
        broken_blades: currentBrokenIndices(),
      });
      state.impactWear = 0;
    }
    if (performance.now() - state.lastWorkshopReadout >= 500) {
      renderWorkshop();
      state.lastWorkshopReadout = performance.now();
    }
    persistCampaign();
  }
  function forwardAirflowCap() {
    return GAME_CATALOG.airflow.earningRpmCap / GAME_CATALOG.airflow.referenceRpm
      * GAME_CATALOG.airflow.referenceCfm * airflowFactor();
  }
  function updateCampaignReadout() {
    refreshDailyStats();
    const campaign = state.campaign;
    const money = value => value.toLocaleString("zh-CN", { maximumFractionDigits: 1 });
    document.getElementById("balanceValue").textContent = money(campaign.balance);
    document.getElementById("workshopBalance").textContent = money(campaign.balance);
    document.getElementById("todayMaxRpm").textContent = Math.floor(campaign.todayMaxRpm).toLocaleString("zh-CN");
    document.getElementById("todayAirflow").textContent = money(campaign.todayAirflow);
    document.getElementById("bestRpm").textContent = Math.floor(campaign.bestRpm).toLocaleString("zh-CN");
    document.getElementById("totalAirflow").textContent = money(campaign.totalAirflow);
    document.getElementById("todayMaxAirflow").textContent = campaign.todayMaxAirflow.toFixed(1);
    document.getElementById("bestAirflow").textContent = campaign.bestAirflow.toFixed(1);
    renderConditions();
  }
  function impellerAction(id) {
    const campaign = state.campaign;
    const item = GAME_CATALOG.materials[id];
    if (id !== "print3d") {
      return { action: "buyMaterial", label: `换新叶轮 · ${item.price} ft³`, disabled: campaign.balance < item.price };
    }
    const remaining = Math.max(0, campaign.printReadyAt - gameNow());
    return {
      action: "installPrint",
      label: remaining ? `打印中 · ${Math.ceil(remaining / 1000)} 秒` : campaign.printReadyAt ? "安装免费叶轮" : "装备后自动打印",
      disabled: remaining > 0 || !campaign.printReadyAt,
    };
  }
  function itemCard(id, item, category, owned, equipped) {
    const campaign = state.campaign;
    let action = owned ? "equip" : "buy";
    let label = equipped ? "已装备" : owned ? "装备" : `购买 · ${item.price.toLocaleString("zh-CN")} ft³`;
    let disabled = equipped || (!owned && campaign.balance < item.price);
    let stats = "";
    if (category === "material") {
      if (!owned) {
        action = "buyMaterial";
        label = `购买并安装 · ${item.price} ft³`;
      }
      const stored = equipped ? 0 : (campaign.storedBrokenBlades[id] || []).length;
      stats = `<li>叶根强度 ×${item.rootStrength} · 手指摩擦 ×${item.friction}</li><li>转动惯量 ×${item.inertia} · 风量 ×${item.airflow}</li><li>${owned && !equipped ? `库存叶轮 · ${stored}/${BLADE_COUNT} 片损坏` : id === "print3d" ? `自动免费打印，每套需等待 ${item.printSeconds} 秒` : `整套 ${item.price} ft³，换新请在状态与耐久中操作`}</li>`;
    } else if (category === "bearing") {
      const turns = campaign.bearingTurns[id];
      const wear = bearingWearMultiplier(id);
      stats = `<li>阻力基准 ×${item.resistance} · 使用 ${Math.floor(turns).toLocaleString("zh-CN")} / ${item.lifeTurns.toLocaleString("zh-CN")} 圈</li><li>当前磨损倍率 ×${wear.toFixed(2)} · 阻力随磨损增加</li>`;
    } else {
      const durability = id === "hand" ? 100 : campaign.handDurability[id];
      stats = `<li>手部摩擦 ×${item.friction} · 出汗影响 ×${item.sweatEffect}</li><li>缓冲量 +${item.compressionBonusMm} mm</li><li>${id === "hand" ? "无装备耐久" : `耐久 ${durability.toFixed(0)}% · 更换 ${item.replaceCost} ft³`}</li>`;
    }
    const actionName = category === "material" ? (owned ? "equipMaterial" : action) : `${action}${category[0].toUpperCase()}${category.slice(1)}`;
    const printProgress = id === "print3d" && campaign.printReadyAt
      ? `<div class="meter" aria-label="打印进度"><span style="width:${clamp((1 - Math.max(0, campaign.printReadyAt - gameNow()) / (item.printSeconds * 1000)) * 100, 0, 100)}%"></span></div>`
      : "";
    let printed = "";
    if (id === "print3d" && category === "material" && !equipped && campaign.printReadyAt) {
      const replacement = impellerAction(id);
      printed = `<button type="button" data-action="${replacement.action}" data-id="${id}"${replacement.disabled ? " disabled" : ""}>${replacement.label}</button>`;
    }
    return `<article class="item-card${equipped ? " is-equipped" : ""}"><div class="item-name">${item.name}<span>${equipped ? "当前" : owned ? "已拥有" : `售价 ${item.price}`}</span></div><ul class="item-stats">${stats}</ul><p class="item-note">${item.note}</p>${printProgress}<button type="button" data-action="${actionName}" data-id="${id}"${disabled ? " disabled" : ""}>${label}</button>${printed}</article>`;
  }
  function renderConditions() {
    if (state.controlPresses.size) {
      state.controlsNeedRefresh = true;
      return;
    }
    const campaign = state.campaign;
    const material = GAME_CATALOG.materials[campaign.material];
    const bearing = GAME_CATALOG.bearings[campaign.bearing];
    const gear = GAME_CATALOG.hands[campaign.hand];
    const bearingM = bearingWearMultiplier();
    const wearPct = clamp(campaign.bearingTurns[campaign.bearing] / bearing.lifeTurns * 100, 0, 100);
    const sweatPct = campaign.sweat * 100;
    const brokenCount = state.brokenBlades.filter(Boolean).length;
    const handDurability = campaign.hand === "hand" ? 100 : campaign.handDurability[campaign.hand];
    const handPct = campaign.hand === "hand" ? 100 : handDurability;
    const leafPct = (BLADE_COUNT - brokenCount) / BLADE_COUNT * 100;
    const overview = document.getElementById("statusOverview");
    overview.innerHTML = `<span>叶轮 ${Math.round(leafPct)}%</span><span>轴承 ${Math.round(100 - wearPct)}%</span><span>手汗 ${Math.round(sweatPct)}%</span><span>手部 ${Math.round(handPct)}%</span>`;
    document.getElementById("statusPanel").classList.toggle("is-warning",
      brokenCount > 0 || Math.min(100 - wearPct, handPct, 100 - sweatPct) < 30);
    const barClass = value => value < 20 ? "is-bad" : value < 45 ? "is-warn" : "";
    const wearLabel = campaign.bearing === "sleeve" ? "轴承使用进度" : "轴承寿命进度";
    const handAction = campaign.hand !== "hand"
      ? `<button type="button" data-action="replaceHand" data-id="${campaign.hand}"${campaign.balance < gear.replaceCost || handDurability >= 100 ? " disabled" : ""}>更换同款装备 · ${gear.replaceCost} ft³</button>`
      : `<button type="button" disabled>裸手无需更换</button>`;
    const replacement = impellerAction(campaign.material);
    const repairButton = `<button type="button" data-action="${replacement.action}" data-id="${campaign.material}"${replacement.disabled ? " disabled" : ""}>${replacement.label}</button>`;
    const serviceButton = `<button type="button" data-action="serviceBearing"${campaign.balance < bearing.service.cost ? " disabled" : ""}>${bearing.service.label} · ${bearing.service.cost} ft³</button>`;
    const powder = GAME_CATALOG.consumables.powder;
    const powderButton = campaign.powderSeconds > 0
      ? `<button type="button" disabled>止滑粉生效中 · ${Math.ceil(campaign.powderSeconds)} 秒</button>`
      : campaign.powderCount > 0
        ? `<button type="button" data-action="usePowder">使用止滑粉（剩余 ${campaign.powderCount}）</button>`
        : `<button type="button" data-action="buyPowder"${campaign.balance < powder.price ? " disabled" : ""}>购买止滑粉 · ${powder.price} ft³</button>`;
    conditionGrid.innerHTML = `
      <div class="condition"><div class="condition-title"><span>扇叶状态</span><span>${brokenCount}/${BLADE_COUNT} 片损坏</span></div><div class="condition-value">${material.name} · 风量系数 ×${(material.airflow * brokenBladeFactor()).toFixed(2)}</div><div class="meter"><span class="${barClass((BLADE_COUNT - brokenCount) / BLADE_COUNT * 100)}" style="width:${(brokenCount / BLADE_COUNT * 100)}%"></span></div>${repairButton}</div>
      <div class="condition"><div class="condition-title"><span>${wearLabel}</span><span>${Math.floor(campaign.bearingTurns[campaign.bearing]).toLocaleString("zh-CN")} 圈</span></div><div class="condition-value">${bearing.name} · 阻力 ×${(bearing.resistance * bearingM).toFixed(2)}</div><div class="meter"><span class="${barClass(100 - wearPct)}" style="width:${wearPct}%"></span></div>${serviceButton}</div>
      <div class="condition"><div class="condition-title"><span>手汗</span><span>${sweatPct.toFixed(0)}%</span></div><div class="condition-value">${campaign.powderSeconds > 0 ? `止滑粉剩余 ${Math.ceil(campaign.powderSeconds)} 秒` : campaign.sweat < .4 ? "干爽 / 微湿，摩擦略增" : "汗液增多，摩擦下降"}</div><div class="meter"><span class="${barClass(100 - sweatPct)}" style="width:${sweatPct}%"></span></div>${powderButton}</div>
      <div class="condition"><div class="condition-title"><span>当前手部装备</span><span>${handPct.toFixed(0)}%</span></div><div class="condition-value">${gear.name} · 接触摩擦 ×${handFrictionFactor().toFixed(2)}</div><div class="meter"><span class="${barClass(handPct)}" style="width:${handPct}%"></span></div>${handAction}</div>`;
  }
  function renderWorkshop() {
    if (state.controlPresses.size) {
      state.controlsNeedRefresh = true;
      return;
    }
    const campaign = state.campaign;
    updateCampaignReadout();
    shopSections.innerHTML = `
      <div class="shop-section"><h3>扇叶材质</h3><div class="item-grid">${MATERIAL_ORDER.map(id => itemCard(id, GAME_CATALOG.materials[id], "material", campaign.ownedMaterials.includes(id), campaign.material === id)).join("")}</div></div>
      <div class="shop-section"><h3>轴承</h3><div class="item-grid">${BEARING_ORDER.map(id => itemCard(id, GAME_CATALOG.bearings[id], "bearing", campaign.ownedBearings.includes(id), campaign.bearing === id)).join("")}</div></div>
      <div class="shop-section"><h3>手部装备</h3><div class="item-grid">${HAND_ORDER.map(id => itemCard(id, GAME_CATALOG.hands[id], "hand", campaign.ownedHands.includes(id), campaign.hand === id)).join("")}</div></div>`;
  }
  function purchaseFromShop(action, id) {
    if (remoteGame) {
      void remoteGame.perform(action, id);
      return;
    }
    const campaign = state.campaign;
    const purchase = (item, ownedList, equipKey, turnsToReset) => {
      if (campaign[ownedList].includes(id)) {
        campaign[equipKey] = id;
      } else {
        if (campaign.balance < item.price) {
          setShopStatus("ft³ 不足，继续拨动风扇积累风量。", true);
          return false;
        }
        campaign.balance -= item.price;
        campaign[ownedList].push(id);
        campaign[equipKey] = id;
        if (turnsToReset) campaign.bearingTurns[id] = 0;
      }
      return true;
    };
    if (action === "installPrint") {
      if (!campaign.printReadyAt || campaign.printReadyAt > gameNow()) return;
      campaign.printReadyAt = 0;
      installImpeller("print3d", true);
      setShopStatus("已安装全新 3D 打印叶轮，下一套正在打印。");
      scheduleMaintenance();
    } else if (action === "equipMaterial") {
      const item = GAME_CATALOG.materials[id];
      if (!item || !campaign.ownedMaterials.includes(id) || campaign.material === id) return;
      installImpeller(id, false);
      setShopStatus(`已换上库存的 ${item.name} 叶轮。`);
      scheduleMaintenance();
    } else if (action === "buyMaterial") {
      const item = GAME_CATALOG.materials[id];
      if (!item || id === "print3d") return;
      if (campaign.balance < item.price) {
        setShopStatus("ft³ 不足，无法购买叶轮。", true);
        return;
      }
      campaign.balance -= item.price;
      installImpeller(id, true);
      setShopStatus(`已安装全新 ${item.name} 叶轮。`);
    } else if (action === "buyBearing" || action === "equipBearing") {
      if (!purchase(GAME_CATALOG.bearings[id], "ownedBearings", "bearing", true)) return;
      setShopStatus(`已装备 ${GAME_CATALOG.bearings[id].name}。`);
    } else if (action === "buyHand" || action === "equipHand") {
      if (!purchase(GAME_CATALOG.hands[id], "ownedHands", "hand", false)) return;
      setShopStatus(`已装备 ${GAME_CATALOG.hands[id].name}。`);
    } else if (action === "serviceBearing") {
      const item = GAME_CATALOG.bearings[campaign.bearing];
      if (campaign.balance < item.service.cost) {
        setShopStatus("ft³ 不足，无法维护轴承。", true);
        return;
      }
      campaign.balance -= item.service.cost;
      campaign.bearingTurns[campaign.bearing] *= item.service.keepWearFraction ** (1 / item.wearP);
      setShopStatus(`${item.name}${item.service.label}完成。`);
    } else if (action === "replaceHand") {
      const item = GAME_CATALOG.hands[id];
      if (!item || campaign.balance < item.replaceCost) {
        setShopStatus("ft³ 不足，无法更换手部装备。", true);
        return;
      }
      campaign.balance -= item.replaceCost;
      campaign.handDurability[id] = 100;
      campaign.handContactSeconds[id] = 0;
      setShopStatus(`${item.name}已更换，耐久恢复至 100%。`);
    } else if (action === "buyPowder") {
      const item = GAME_CATALOG.consumables.powder;
      if (campaign.balance < item.price) {
        setShopStatus("ft³ 不足，无法购买止滑粉。", true);
        return;
      }
      campaign.balance -= item.price;
      campaign.powderCount += 1;
      setShopStatus("已购买止滑粉。");
    } else if (action === "usePowder") {
      if (!campaign.powderCount || campaign.powderSeconds > 0) return;
      campaign.powderCount -= 1;
      campaign.powderSeconds = GAME_CATALOG.consumables.powder.pressSeconds;
      campaign.sweat = 0;
      setShopStatus("已使用止滑粉，按住计时 90 秒。");
    } else {
      return;
    }
    renderWorkshop();
    updateReadout();
    draw();
    persistCampaign(true);
  }
  shopSections.addEventListener("click", event => {
    const button = event.target.closest("button[data-action]");
    if (button && !button.disabled) purchaseFromShop(button.dataset.action, button.dataset.id);
  });
  conditionGrid.addEventListener("click", event => {
    const button = event.target.closest("button[data-action]");
    if (button && !button.disabled) purchaseFromShop(button.dataset.action, button.dataset.id);
  });
  function flushControlRefresh() {
    if (state.controlPresses.size || !state.controlsNeedRefresh) return;
    state.controlsNeedRefresh = false;
    renderWorkshop();
  }
  for (const container of [shopSections, conditionGrid]) {
    container.addEventListener("pointerdown", event => {
      const button = event.target.closest("button[data-action]");
      if (button && !button.disabled) state.controlPresses.add(event.pointerId);
    }, true);
  }
  function finishControlPress(event) {
    if (!state.controlPresses.has(event.pointerId)) return;
    // The browser dispatches click after pointerup; keep its target alive until then.
    window.setTimeout(() => {
      state.controlPresses.delete(event.pointerId);
      flushControlRefresh();
    }, 0);
  }
  document.getElementById("debugGrant")?.addEventListener("click", () => {
    if (remoteGame) return;
    state.campaign.balance += 1000;
    setShopStatus("调试：增加 1000 ft³。");
    renderWorkshop();
    persistCampaign(true);
  });
  document.getElementById("debugClearSave")?.addEventListener("click", () => {
    if (serverClient) return;
    try {
      window.localStorage.removeItem(SAVE_KEY);
      window.location.reload();
    } catch (error) {
      console.warn("无法清空风扇工坊存档。", error);
      setShopStatus("无法清空本地存档，请检查浏览器存储权限。", true);
    }
  });
  window.addEventListener("beforeunload", () => persistCampaign(true));
  function smoothstep(edge0, edge1, value) {
    const t = clamp((value - edge0) / (edge1 - edge0), 0, 1);
    return t * t * (3 - 2 * t);
  }

  const soundToggle = document.getElementById("soundToggle");
  const audio = { enabled: true, context: null, nodes: null, idleTimer: null, failed: false };
  function soundError(error) {
    if (audio.failed) return;
    audio.failed = true;
    console.warn("无法启用风扇音效。", error);
    setShopStatus("音效不可用，游戏仍可正常进行。", true);
  }
  function initializeSound() {
    if (!audio.enabled || audio.failed) return;
    if (audio.idleTimer !== null) {
      window.clearTimeout(audio.idleTimer);
      audio.idleTimer = null;
    }
    try {
      if (!audio.context) {
        const AudioContext = window.AudioContext || window.webkitAudioContext;
        if (!AudioContext) {
          soundError(new Error("浏览器不支持 Web Audio"));
          return;
        }
        const context = new AudioContext();
        audio.context = context;
        const master = context.createGain();
        master.gain.value = .22;
        master.connect(context.destination);
        const buffer = context.createBuffer(1, context.sampleRate * 2, context.sampleRate);
        const samples = buffer.getChannelData(0);
        for (let i = 0; i < samples.length; i += 1) samples[i] = Math.random() * 2 - 1;
        const noise = context.createBufferSource();
        noise.buffer = buffer;
        noise.loop = true;
        const windFilter = context.createBiquadFilter();
        windFilter.type = "bandpass";
        windFilter.frequency.value = 900;
        windFilter.Q.value = .6;
        const windGain = context.createGain();
        windGain.gain.value = 0;
        noise.connect(windFilter);
        windFilter.connect(windGain);
        windGain.connect(master);
        const flutter = context.createOscillator();
        const flutterDepth = context.createGain();
        flutterDepth.gain.value = 0;
        flutter.connect(flutterDepth);
        flutterDepth.connect(windGain.gain);
        flutter.start();
        const bearingFilter = context.createBiquadFilter();
        bearingFilter.type = "bandpass";
        bearingFilter.frequency.value = 1800;
        const bearingGain = context.createGain();
        bearingGain.gain.value = 0;
        noise.connect(bearingFilter);
        bearingFilter.connect(bearingGain);
        bearingGain.connect(master);
        // Fidget-spinner-like metallic hiss: two narrow high bands above a steep highpass.
        const ballRustleFilter = context.createBiquadFilter();
        ballRustleFilter.type = "bandpass";
        ballRustleFilter.frequency.value = 6800;
        ballRustleFilter.Q.value = 4;
        const ballRustleShimmer = context.createBiquadFilter();
        ballRustleShimmer.type = "bandpass";
        ballRustleShimmer.frequency.value = 10200;
        ballRustleShimmer.Q.value = 7;
        const ballRustleHighpass = context.createBiquadFilter();
        ballRustleHighpass.type = "highpass";
        ballRustleHighpass.frequency.value = 4500;
        const ballRustleGain = context.createGain();
        ballRustleGain.gain.value = 0;
        noise.connect(ballRustleFilter);
        noise.connect(ballRustleShimmer);
        ballRustleFilter.connect(ballRustleHighpass);
        ballRustleShimmer.connect(ballRustleHighpass);
        ballRustleHighpass.connect(ballRustleGain);
        ballRustleGain.connect(master);
        const hubFilter = context.createBiquadFilter();
        hubFilter.type = "bandpass";
        hubFilter.frequency.value = 1400;
        const hubGain = context.createGain();
        hubGain.gain.value = 0;
        noise.connect(hubFilter);
        hubFilter.connect(hubGain);
        hubGain.connect(master);
        noise.start();
        const makeHarmonic = order => {
          const oscillator = context.createOscillator();
          const gain = context.createGain();
          gain.gain.value = 0;
          oscillator.connect(gain);
          gain.connect(master);
          oscillator.start();
          return { oscillator, gain, order };
        };
        const harmonics = [1, 2, 3, 4].map(makeHarmonic);
        const shaftHarmonics = Array.from({ length: BLADE_COUNT - 1 }, (_, i) => makeHarmonic(i + 1));
        const cogRate = context.createOscillator();
        const pulse = context.createWaveShaper();
        pulse.curve = Float32Array.from({ length: 1024 }, (_, i) => {
          const value = i / 1023 * 2 - 1;
          return ((value + 1) / 2) ** 4;
        });
        const cogDepth = context.createGain();
        cogDepth.gain.value = 0;
        const cogFilter = context.createBiquadFilter();
        cogFilter.type = "bandpass";
        cogFilter.frequency.value = 220;
        cogFilter.Q.value = .8;
        const cogLowpass = context.createBiquadFilter();
        cogLowpass.type = "lowpass";
        cogLowpass.frequency.value = 480;
        cogLowpass.Q.value = .7;
        const cogGain = context.createGain();
        cogGain.gain.value = 0;
        cogRate.connect(pulse);
        pulse.connect(cogDepth);
        cogDepth.connect(cogGain.gain);
        noise.connect(cogFilter);
        cogFilter.connect(cogLowpass);
        cogLowpass.connect(cogGain);
        cogGain.connect(master);
        cogRate.start();
        const ballRate = context.createOscillator();
        const ballPulse = context.createWaveShaper();
        ballPulse.curve = Float32Array.from({ length: 1024 }, (_, i) => {
          const value = i / 1023 * 2 - 1;
          return value > .985 ? ((value - .985) / .015) ** 2 : 0;
        });
        const ballDepth = context.createGain();
        ballDepth.gain.value = 0;
        const ballFilter = context.createBiquadFilter();
        ballFilter.type = "bandpass";
        ballFilter.frequency.value = 2450;
        ballFilter.Q.value = 24;
        const ballOvertone = context.createBiquadFilter();
        ballOvertone.type = "bandpass";
        ballOvertone.frequency.value = 4150;
        ballOvertone.Q.value = 36;
        const ballHighpass = context.createBiquadFilter();
        ballHighpass.type = "highpass";
        ballHighpass.frequency.value = 1500;
        const ballGain = context.createGain();
        ballGain.gain.value = 0;
        ballRate.connect(ballPulse);
        ballPulse.connect(ballDepth);
        ballDepth.connect(ballGain.gain);
        // Gate the excitation before the resonators so each click has a short hard-surface ring.
        noise.connect(ballGain);
        ballGain.connect(ballFilter);
        ballGain.connect(ballOvertone);
        ballFilter.connect(ballHighpass);
        ballOvertone.connect(ballHighpass);
        ballHighpass.connect(master);
        ballRate.start();
        const imbalance = context.createOscillator();
        const imbalanceGain = context.createGain();
        imbalanceGain.gain.value = 0;
        imbalance.connect(imbalanceGain);
        imbalanceGain.connect(master);
        imbalance.start();
        audio.nodes = { master, noiseBuffer: buffer, windFilter, windGain, flutter, flutterDepth, bearingGain, ballRustleFilter, ballRustleShimmer, ballRustleHighpass, ballRustleGain, hubGain, harmonics, shaftHarmonics, cogRate, cogFilter, cogLowpass, cogDepth, ballRate, ballFilter, ballOvertone, ballHighpass, ballDepth, imbalance, imbalanceGain };
      }
      if (audio.context.state === "suspended") {
        audio.context.resume().catch(soundError);
      }
    } catch (error) {
      soundError(error);
    }
  }
  function updateSound() {
    if (!audio.context || !audio.nodes || audio.failed) return;
    const now = audio.context.currentTime;
    const set = (param, value) => param.setTargetAtTime(value, now, .05);
    const speed = rpm();
    const level = Math.min(speed / 3000, 1);
    const soundExponent = Number(tuning.airflowSoundExponent.value);
    const airflowSoundLevel = speed > 0 ? level ** soundExponent : 0;
    const normalizedAirflow = Math.min(speed / GAME_CATALOG.airflow.referenceRpm * airflowFactor(), 1);
    const windSoundLevel = normalizedAirflow > 0 ? normalizedAirflow ** soundExponent : 0;
    const bladeFraction = intactBladeFraction();
    const bladeFrequency = speed / 60 * BLADE_COUNT;
    const nodes = audio.nodes;
    const ballBearing = state.campaign.bearing === "ball";
    const bearingWear = bearingWearMultiplier();
    set(nodes.master.gain, audio.enabled && !document.hidden
      && (!remoteGame || remoteGame.ready && !remoteGame.busy) ? .22 : 0);
    set(nodes.windFilter.frequency, 700 + 1900 * level);
    const windNoiseGain = Number(tuning.windNoiseGain.value);
    const bladeToneGain = Number(tuning.bladeToneGain.value);
    set(nodes.windGain.gain, .09 * windSoundLevel * windNoiseGain);
    set(nodes.flutter.frequency, Math.max(.1, bladeFrequency));
    set(nodes.flutterDepth.gain, .035 * windSoundLevel * windNoiseGain);
    set(nodes.bearingGain.gain, ballBearing ? 0 : Math.min(.035, .003 + (bearingWear - 1) * .006) * Math.min(speed / 250, 1));
    // Rises quickly to ~75% by 200 RPM, then creeps up slowly toward full level at 3000 RPM.
    const rustleLevel = speed > 1 ? (1 - Math.exp(-speed / 94)) * (.8 + .2 * Math.sqrt(level)) : 0;
    set(nodes.ballRustleGain.gain, ballBearing ? .04 * rustleLevel * Number(tuning.ballRustleGain.value) : 0);
    set(nodes.ballRustleFilter.frequency, 6800 + 800 * level);
    set(nodes.ballRustleShimmer.frequency, 10200 + 1200 * level);
    const pointer = state.pointer;
    const motion = pointer ? pointerMotionAt(pointer, performance.now()) : null;
    const fingerOmega = motion
      ? fingerAngularVelocity(motion.point, motion.velocityX, motion.velocityY)
      : 0;
    const hubSlip = pointer && pointer.physicalContact && pointer.contact === "hub"
      ? Math.abs(fingerOmega - state.omega) : 0;
    set(nodes.hubGain.gain, .035 * Math.min(hubSlip / 25, 1));
    nodes.harmonics.forEach(node => {
      set(node.oscillator.frequency, Math.max(1, bladeFrequency * node.order));
      set(node.gain.gain, .016 * airflowSoundLevel * bladeToneGain * bladeFraction / 2 ** (node.order - 1));
    });
    // Missing slots break sevenfold symmetry, adding shaft-order components without retuning BPF.
    nodes.shaftHarmonics.forEach(node => {
      let real = 0;
      let imaginary = 0;
      state.brokenBlades.forEach((damage, index) => {
        if (damage) return;
        const angle = node.order * index * Math.PI * 2 / BLADE_COUNT;
        real += Math.cos(angle);
        imaginary += Math.sin(angle);
      });
      const amplitude = Math.hypot(real, imaginary) / BLADE_COUNT;
      set(node.oscillator.frequency, Math.max(1, speed / 60 * node.order));
      set(node.gain.gain, amplitude < 1e-9 ? 0
        : .008 * airflowSoundLevel * bladeToneGain * amplitude / Math.sqrt(node.order));
    });
    set(nodes.cogRate.frequency, Math.max(.1, speed / 60 * 12));
    set(nodes.cogFilter.frequency, 200 + 100 * level);
    set(nodes.cogLowpass.frequency, 450 + 150 * level);
    set(nodes.cogDepth.gain, speed > 1 ? .16 * Math.min(speed / 40, 1) * (1 + .25 * level) * (ballBearing ? .45 : 1) : 0);
    // A stylized rolling-bearing rattle, not a measured ball-pass frequency.
    set(nodes.ballRate.frequency, Math.max(.1, speed / 60 * 8));
    set(nodes.ballDepth.gain, ballBearing ? (.4 + Math.min(.18, (bearingWear - 1) * .036)) * Math.min(speed / 80, 1) : 0);
    set(nodes.imbalance.frequency, Math.max(1, speed / 60));
    set(nodes.imbalanceGain.gain, .012 * state.brokenBlades.filter(Boolean).length / BLADE_COUNT * Math.min(speed / 300, 1));
    if (audio.idleTimer === null && (!audio.enabled || document.hidden || (speed < 1 && !state.pointer))) {
      audio.idleTimer = window.setTimeout(() => {
        audio.idleTimer = null;
        if (audio.context && (!audio.enabled || document.hidden || (rpm() < 1 && !state.pointer))) {
          audio.context.suspend().catch(soundError);
        }
      }, 300);
    }
  }
  function playBreakSound() {
    if (!audio.enabled || !audio.context || !audio.nodes || audio.context.state !== "running") return;
    const now = audio.context.currentTime;
    try {
      const duration = .045;
      const noise = audio.context.createBufferSource();
      const filter = audio.context.createBiquadFilter();
      const lowpass = audio.context.createBiquadFilter();
      const crackGain = audio.context.createGain();
      noise.buffer = audio.nodes.noiseBuffer;
      filter.type = "highpass";
      filter.frequency.value = 650;
      lowpass.type = "lowpass";
      lowpass.frequency.value = 7500;
      crackGain.gain.setValueAtTime(.0001, now);
      crackGain.gain.linearRampToValueAtTime(.4, now + .0005);
      crackGain.gain.exponentialRampToValueAtTime(.02, now + .009);
      crackGain.gain.exponentialRampToValueAtTime(.0001, now + duration);
      noise.connect(filter);
      filter.connect(lowpass);
      lowpass.connect(crackGain);
      crackGain.connect(audio.nodes.master);
      noise.start(now);
      noise.stop(now + duration);
    } catch (error) {
      soundError(error);
    }
  }
  soundToggle.addEventListener("click", () => {
    audio.enabled = !audio.enabled;
    soundToggle.textContent = `音效：${audio.enabled ? "开启" : "关闭"}`;
    soundToggle.setAttribute("aria-pressed", String(!audio.enabled));
    if (audio.enabled) initializeSound();
    updateSound();
  });

  function rotateRotor(delta) {
    state.angle = (state.angle + delta) % (Math.PI * 2);
    state.totalRadians += Math.abs(delta);
    state.campaign.bearingTurns[state.campaign.bearing] += Math.abs(delta) / (Math.PI * 2);
  }

  function roundedRectPath(context, x, y, width, height, radius) {
    const r = Math.min(radius, width / 2, height / 2);
    context.beginPath();
    context.moveTo(x + r, y);
    context.arcTo(x + width, y, x + width, y + height, r);
    context.arcTo(x + width, y + height, x, y + height, r);
    context.arcTo(x, y + height, x, y, r);
    context.arcTo(x, y, x + width, y, r);
    context.closePath();
  }

  function drawMount(size, mirrorX, mirrorY) {
    const outer = PAD_HALF_MM;
    const r = PAD_INNER_CORNER_MM;
    const filletY = outer - r;
    // The R1 centres are outside the concentric R70 circle for internal tangency.
    const filletX = Math.sqrt((PAD_INNER_MM + r) ** 2 - filletY ** 2);
    const topAngle = Math.atan2(-filletY, -filletX);
    const leftAngle = Math.atan2(-filletX, -filletY);
    const corner = PAD_OUTER_CORNER_MM;
    ctx.save();
    ctx.translate(size / 2, size / 2);
    ctx.scale(size * MM * mirrorX, size * MM * mirrorY);
    ctx.beginPath();
    ctx.moveTo(-outer + corner, -outer);
    ctx.lineTo(-filletX, -outer);
    ctx.arc(-filletX, -filletY, r, -Math.PI / 2, topAngle + Math.PI);
    ctx.arc(0, 0, PAD_INNER_MM, topAngle, leftAngle, true);
    ctx.arc(-filletY, -filletX, r, leftAngle + Math.PI, Math.PI);
    ctx.lineTo(-outer, -outer + corner);
    ctx.arc(-outer + corner, -outer + corner, corner, Math.PI, Math.PI * 1.5);
    ctx.closePath();
    ctx.fillStyle = "#414141";
    ctx.fill();

    ctx.globalCompositeOperation = "destination-out";
    ctx.beginPath();
    ctx.arc(-HOLE_PITCH_MM / 2, -HOLE_PITCH_MM / 2, HOLE_RADIUS_MM, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  }

  function fractureCrossings(radius) {
    const crossings = [];
    for (let i = 0; i < BLADE.points.length; i += 1) {
      const a = BLADE.points[i];
      const b = BLADE.points[(i + 1) % BLADE.points.length];
      const ra = Math.hypot(a[0], a[1]) - radius;
      const rb = Math.hypot(b[0], b[1]) - radius;
      if (ra * rb > 0 || Math.abs(ra - rb) < 1e-9) continue;
      const t = clamp(ra / (ra - rb), 0, 1);
      crossings.push([a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]);
    }
    return crossings;
  }

  function drawBlade(mmScale, index) {
    const spread = Math.PI * 2 / BLADE_COUNT;
    const damage = state.brokenBlades[index];
    ctx.save();
    ctx.rotate(index * spread);
    ctx.scale(mmScale, mmScale);
    if (damage) {
      ctx.beginPath();
      ctx.arc(0, 0, damage.stubRadiusMm, 0, Math.PI * 2);
      ctx.clip();
    }

    // Shade outward from the leading-edge arc centre so the leading edge is brightest.
    const [cx, cy] = BLADE.leadCenter;
    const material = ctx.createRadialGradient(cx, cy, EDGE_ARC_MM, cx, cy, BLADE.shadeReach);
    material.addColorStop(0, "#242424");
    material.addColorStop(.45, "#1f1f1f");
    material.addColorStop(1, "#181818");
    ctx.fillStyle = material;
    ctx.fill(BLADE_PATH);

    ctx.strokeStyle = "rgba(0, 0, 0, .25)";
    ctx.lineWidth = .25;
    ctx.stroke(BLADE_PATH);

    const worldAngle = state.angle + index * Math.PI * 2 / BLADE_COUNT;
    const directionalLight = Math.cos(worldAngle + 2.2);
    const broadLight = smoothstep(-.45, .65, directionalLight);
    const edgeLight = smoothstep(.55, -.45, directionalLight);
    ctx.save();
    ctx.clip(BLADE_PATH);
    const materialId = state.campaign.material;
    drawMaterialTexture(materialId, directionalLight, index, mmScale);
    const gloss = materialId === "print3d" ? .6 : materialId === "lcp" ? 1.2 : 1;
    // Fixed world-space blurred light reflection on the left: upper-left blades catch it
    // near the tip, left blades mid-span, lower-left blades near the root.
    const cosA = Math.cos(worldAngle);
    const sinA = Math.sin(worldAngle);
    for (const [wx, wy, radius, baseStrength] of SPECULAR_SPOTS) {
      const strength = baseStrength * gloss;
      const lx = wx * cosA + wy * sinA;
      const ly = -wx * sinA + wy * cosA;
      const spot = ctx.createRadialGradient(lx, ly, 0, lx, ly, radius);
      spot.addColorStop(0, `rgba(235, 235, 235, ${(strength * .52).toFixed(3)})`);
      spot.addColorStop(.25, `rgba(235, 235, 235, ${(strength * .42).toFixed(3)})`);
      spot.addColorStop(.5, `rgba(235, 235, 235, ${(strength * .22).toFixed(3)})`);
      spot.addColorStop(.75, `rgba(235, 235, 235, ${(strength * .06).toFixed(3)})`);
      spot.addColorStop(1, "rgba(235, 235, 235, 0)");
      ctx.fillStyle = spot;
      ctx.fillRect(lx - radius, ly - radius, radius * 2, radius * 2);
    }
    // The rounded leading edge itself faces away from that light, so keep it dark.
    const [leadX, leadY] = BLADE.leadCenter;
    const edgeShade = ctx.createRadialGradient(leadX, leadY, EDGE_ARC_MM, leadX, leadY, EDGE_ARC_MM + 3.5);
    edgeShade.addColorStop(0, `rgba(22, 22, 22, ${(.25 + broadLight * .7).toFixed(3)})`);
    edgeShade.addColorStop(.45, `rgba(22, 22, 22, ${(broadLight * .35).toFixed(3)})`);
    edgeShade.addColorStop(1, "rgba(22, 22, 22, 0)");
    ctx.fillStyle = edgeShade;
    ctx.fillRect(-TIP_MM, -TIP_MM, TIP_MM * 2, TIP_MM * 2);
    ctx.restore();

    ctx.beginPath();
    BLADE.points.slice(0, LEADING_EDGE_END_SEGMENT + 1).forEach(([x, y], pointIndex) => {
      if (pointIndex === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = `rgba(235, 235, 235, ${(0.03 + edgeLight * .48).toFixed(3)})`;
    ctx.lineWidth = .48;
    ctx.lineJoin = "round";
    ctx.stroke();
    if (damage) {
      const crossings = fractureCrossings(damage.stubRadiusMm);
      if (crossings.length >= 2) {
        const [start, end] = [crossings[0], crossings[crossings.length - 1]];
        const dx = end[0] - start[0];
        const dy = end[1] - start[1];
        const length = Math.hypot(dx, dy) || 1;
        const nx = -dy / length;
        const ny = dx / length;
        const segments = 8;
        ctx.beginPath();
        ctx.moveTo(start[0], start[1]);
        for (let i = 1; i < segments; i += 1) {
          const t = i / segments;
          const jag = Math.sin(i * 2.37 + damage.seed * 13) * (.35 + damage.seed * .8);
          ctx.lineTo(start[0] + dx * t + nx * jag, start[1] + dy * t + ny * jag);
        }
        ctx.lineTo(end[0], end[1]);
        ctx.strokeStyle = "rgba(190, 190, 190, .8)";
        ctx.lineWidth = .55;
        ctx.stroke();
      }
    }
    ctx.restore();
  }

  function drawHub(size, offsetX = 0, offsetY = 0) {
    const radius = size * HUB_RADIUS / 1.12;
    const wobble = 0.1*Math.sin(state.angle);
    const glintAngle = -2.2 + wobble * .1;
    const glintStrength = .42 + Math.cos(state.angle) * .03;
    const lobeWidth = .075 + wobble * .006;
    ctx.save();
    ctx.translate(size / 2 + offsetX, size / 2 + offsetY);

    const rim = ctx.createRadialGradient(-radius * .28, -radius * .32, radius * .08, 0, 0, radius * 1.13);
    rim.addColorStop(0, "#363636");
    rim.addColorStop(.48, "#161616");
    rim.addColorStop(.83, "#090909");
    rim.addColorStop(1, "#545454");
    ctx.beginPath();
    ctx.arc(0, 0, radius * 1.12, 0, Math.PI * 2);
    ctx.fillStyle = rim;
    ctx.fill();

    ctx.beginPath();
    ctx.arc(0, 0, radius, 0, Math.PI * 2);
    ctx.fillStyle = "#050505";
    ctx.fill();
    ctx.strokeStyle = "rgba(214, 214, 214, .25)";
    ctx.lineWidth = Math.max(.8, size * .002);
    ctx.stroke();

    ctx.save();
    ctx.beginPath();
    ctx.arc(0, 0, radius * .965, 0, Math.PI * 2);
    ctx.clip();

    const reflection = ctx.createConicGradient(glintAngle, 0, 0);
    reflection.addColorStop(0, `rgba(240, 240, 240, ${glintStrength.toFixed(3)})`);
    reflection.addColorStop(lobeWidth, "rgba(201, 201, 201, .08)");
    reflection.addColorStop(.22, "rgba(255, 255, 255, 0)");
    reflection.addColorStop(.42 + wobble * .005, "rgba(201, 201, 201, .08)");
    reflection.addColorStop(.5, `rgba(240, 240, 240, ${(glintStrength * .78).toFixed(3)})`);
    reflection.addColorStop(.575 + wobble * .004, "rgba(201, 201, 201, .06)");
    reflection.addColorStop(.78, "rgba(255, 255, 255, 0)");
    reflection.addColorStop(.925, "rgba(201, 201, 201, .08)");
    reflection.addColorStop(1, `rgba(240, 240, 240, ${glintStrength.toFixed(3)})`);
    ctx.fillStyle = reflection;
    ctx.fillRect(-radius, -radius, radius * 2, radius * 2);

    for (let i = 0; i < 64; i += 1) {
      const grooveRadius = radius * (.035 + i * .0145);
      ctx.beginPath();
      ctx.arc(0, 0, grooveRadius, 0, Math.PI * 2);
      ctx.strokeStyle = i % 2 === 0
        ? "rgba(219, 219, 219, .13)"
        : "rgba(0, 0, 0, .38)";
      ctx.lineWidth = Math.max(.35, size * .0008);
      ctx.stroke();
    }

    ctx.save();
    ctx.rotate(state.angle);
    for (const particle of DUST) {
      ctx.beginPath();
      ctx.arc(particle.x * radius, particle.y * radius, particle.size * radius, 0, Math.PI * 2);
      ctx.fillStyle = `rgba(185, 185, 185, ${particle.alpha})`;
      ctx.fill();
    }
    ctx.restore();

    ctx.restore();

    ctx.restore();
  }

  // Four static frame struts under the rotor: start under the hub, meet the bore with a tangential skew.
  function drawStruts(size) {
    const center = size / 2;
    const scale = size * MM;
    ctx.save();
    ctx.translate(center, center);
    ctx.scale(scale, scale);
    ctx.beginPath();
    ctx.arc(0, 0, BORE_MM + .2, 0, Math.PI * 2);
    ctx.clip();
    for (let i = 0; i < STRUT_COUNT; i += 1) {
      const base = STRUT_ANGLE + i * Math.PI * 2 / STRUT_COUNT;
      const inner = base - STRUT_SKEW;
      const x0 = Math.cos(inner) * STRUT_INNER_MM;
      const y0 = Math.sin(inner) * STRUT_INNER_MM;
      const x1 = Math.cos(base) * (BORE_MM + 1);
      const y1 = Math.sin(base) * (BORE_MM + 1);
      const length = Math.hypot(x1 - x0, y1 - y0);
      const nx = -(y1 - y0) / length * STRUT_WIDTH_MM / 2;
      const ny = (x1 - x0) / length * STRUT_WIDTH_MM / 2;
      const shade = ctx.createLinearGradient(x0 + nx, y0 + ny, x0 - nx, y0 - ny);
      shade.addColorStop(0, "#3a3a3a");
      shade.addColorStop(.3, "#242424");
      shade.addColorStop(1, "#141414");
      ctx.beginPath();
      ctx.moveTo(x0 + nx, y0 + ny);
      ctx.lineTo(x1 + nx, y1 + ny);
      ctx.lineTo(x1 - nx, y1 - ny);
      ctx.lineTo(x0 - nx, y0 - ny);
      ctx.closePath();
      ctx.fillStyle = shade;
      ctx.fill();
    }
    ctx.restore();
  }

  function draw() {
    const size = state.size;
    if (!size) return;
    ctx.clearRect(0, 0, size, size);

    const frameSize = size * FRAME_MM * MM;
    const margin = (size - frameSize) / 2;
    const corner = size * MM * FRAME_CORNER_MM;
    roundedRectPath(ctx, margin, margin, frameSize, frameSize, corner);
    ctx.fillStyle = "#1a1a1a";
    ctx.fill();

    const center = size / 2;
    const boreRadius = size * BORE_MM * MM;
    const edgeRadius = size * (BORE_MM + BORE_EDGE_MM) * MM;
    ctx.save();
    ctx.globalCompositeOperation = "destination-out";
    ctx.beginPath();
    ctx.arc(center, center, boreRadius, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
    // 1 mm rounded transition from the bore into the frame face.
    const boreEdge = ctx.createRadialGradient(center, center, boreRadius, center, center, edgeRadius);
    boreEdge.addColorStop(0, "#616161");
    boreEdge.addColorStop(.35, "#3d3d3d");
    boreEdge.addColorStop(.7, "#262626");
    boreEdge.addColorStop(1, "#1a1a1a");
    ctx.beginPath();
    ctx.arc(center, center, edgeRadius, 0, Math.PI * 2);
    ctx.moveTo(center + boreRadius, center);
    ctx.arc(center, center, boreRadius, Math.PI * 2, 0, true);
    ctx.fillStyle = boreEdge;
    ctx.fill();

    drawMount(size, 1, 1);
    drawMount(size, -1, 1);
    drawMount(size, 1, -1);
    drawMount(size, -1, -1);
    drawStruts(size);

    const brokenCount = state.brokenBlades.filter(Boolean).length;
    const vibration = brokenCount
      ? Math.min(3, rpm() / 3000 * (0.8 + brokenCount * .35))
      : 0;
    const vibrationPhase = state.animationTime * .045 + state.angle * 1.7;
    const vibrationX = Math.sin(vibrationPhase) * vibration;
    const vibrationY = Math.cos(vibrationPhase * 1.13) * vibration * .72;
    ctx.save();
    ctx.translate(center + vibrationX, center + vibrationY);
    ctx.rotate(state.angle);
    for (let i = 0; i < BLADE_COUNT; i += 1) drawBlade(size * MM, i);
    ctx.restore();

    drawHub(size, vibrationX, vibrationY);
    drawFlyouts(size);
  }

  function drawFlyouts(size) {
    const duration = 900;
    for (const flyout of state.flyouts) {
      const progress = clamp((state.animationTime - flyout.startTime) / duration, 0, 1);
      if (progress >= 1) continue;
      const distance = 105 * progress;
      const tangent = flyout.angle + Math.PI / 2;
      ctx.save();
      ctx.globalAlpha = 1 - progress;
      ctx.translate(
        size / 2 + Math.cos(tangent) * distance * size * MM,
        size / 2 + Math.sin(tangent) * distance * size * MM,
      );
      ctx.rotate(flyout.angle + progress * flyout.spin);
      ctx.scale(size * MM * (1 + progress * .2), size * MM * (1 - progress * .28));
      ctx.fillStyle = "#242424";
      ctx.fill(BLADE_PATH);
      ctx.strokeStyle = "rgba(190, 190, 190, .55)";
      ctx.lineWidth = .3;
      ctx.stroke(BLADE_PATH);
      ctx.restore();
    }
    state.flyouts = state.flyouts.filter(flyout => state.animationTime - flyout.startTime < duration);
  }

  function resize() {
    const rect = canvas.getBoundingClientRect();
    if (!rect.width) return;
    state.size = rect.width;
    state.dpr = Math.min(window.devicePixelRatio || 1, 2);
    canvas.width = Math.round(rect.width * state.dpr);
    canvas.height = Math.round(rect.height * state.dpr);
    ctx.setTransform(state.dpr, 0, 0, state.dpr, 0, 0);
    draw();
    if (hasActiveAnimation()) wakeAnimation();
  }

  function localPoint(event) {
    const rect = canvas.getBoundingClientRect();
    return { x: event.clientX - rect.left, y: event.clientY - rect.top };
  }

  function angleAt(point) {
    const center = state.size / 2;
    return Math.atan2(point.y - center, point.x - center);
  }

  function radiusAt(point) {
    const center = state.size / 2;
    return Math.hypot(point.x - center, point.y - center) / state.size;
  }

  function contactAt(point) {
    const radiusMm = radiusAt(point) * FRAME_MM / .98;
    if (radiusMm <= HUB_MM + FINGER_RADIUS_MM) return "hub";
    return radiusMm <= TIP_MM + FINGER_RADIUS_MM ? "blade" : "none";
  }

  function bladeContact(pointer, {
    onlyBlade = null,
    excludedBlade = null,
    point = pointer.point,
  } = {}) {
    const center = state.size / 2;
    const worldX = (point.x - center) / (state.size * MM);
    const worldY = (point.y - center) / (state.size * MM);
    const radiusMm = Math.hypot(worldX, worldY);
    if (radiusMm < HUB_MM - FINGER_RADIUS_MM || radiusMm > TIP_MM + FINGER_RADIUS_MM) return null;
    const pointAngle = Math.atan2(worldY, worldX) - state.angle;
    const spread = Math.PI * 2 / BLADE_COUNT;
    let nearestContact = null;
    for (let blade = 0; blade < BLADE_COUNT; blade += 1) {
      if ((onlyBlade !== null && blade !== onlyBlade)
        || blade === excludedBlade
        || state.brokenBlades[blade]) continue;
      const angle = pointAngle - blade * spread;
      const cos = Math.cos(angle);
      const sin = Math.sin(angle);
      const x = radiusMm * cos;
      const y = radiusMm * sin;
      if (x < BLADE_BOUNDS.minX - FINGER_RADIUS_MM
        || x > BLADE_BOUNDS.maxX + FINGER_RADIUS_MM
        || y < BLADE_BOUNDS.minY - FINGER_RADIUS_MM
        || y > BLADE_BOUNDS.maxY + FINGER_RADIUS_MM) continue;
      let inside = false;
      for (const segment of BLADE_SEGMENTS) {
        if ((segment.startY > y) !== (segment.endY > y)
          && x < (segment.endX - segment.startX) * (y - segment.startY)
            / (segment.endY - segment.startY) + segment.startX) inside = !inside;
      }
      let nearestDistanceSquared = Infinity;
      let nearestX = 0;
      let nearestY = 0;
      let nearestDx = 0;
      let nearestDy = 0;
      let nearestEdgeIndex = -1;
      for (let i = 0; i < BLADE_SEGMENTS.length; i += 1) {
        const segment = BLADE_SEGMENTS[i];
        const radialGap = radiusMm < segment.minRadius
          ? segment.minRadius - radiusMm
          : radiusMm > segment.maxRadius ? radiusMm - segment.maxRadius : 0;
        const distanceBoundSquared = inside ? nearestDistanceSquared : FINGER_RADIUS_MM ** 2;
        if (radialGap * radialGap > distanceBoundSquared + 1e-9) continue;
        const t = segment.lengthSquared
          ? clamp(((x - segment.startX) * segment.dx + (y - segment.startY) * segment.dy)
            / segment.lengthSquared, 0, 1)
          : 0;
        const candidateX = segment.startX + t * segment.dx;
        const candidateY = segment.startY + t * segment.dy;
        const deltaX = x - candidateX;
        const deltaY = y - candidateY;
        const distanceSquared = deltaX * deltaX + deltaY * deltaY;
        if (distanceSquared < nearestDistanceSquared) {
          nearestDistanceSquared = distanceSquared;
          nearestX = candidateX;
          nearestY = candidateY;
          nearestDx = segment.dx;
          nearestDy = segment.dy;
          nearestEdgeIndex = i;
        }
      }
      if (nearestEdgeIndex < 0) continue;
      const nearestDistance = Math.sqrt(nearestDistanceSquared);
      const penetration = FINGER_RADIUS_MM - (inside ? -nearestDistance : nearestDistance);
      if (penetration <= 0 || (nearestContact && penetration <= nearestContact.penetration)) continue;
      let normalX;
      let normalY;
      if (nearestDistance > 1e-6) {
        const direction = inside ? -1 : 1;
        normalX = (x - nearestX) / nearestDistance * direction;
        normalY = (y - nearestY) / nearestDistance * direction;
      } else {
        const edgeLength = Math.hypot(nearestDx, nearestDy) || 1;
        normalX = BLADE_WINDING * nearestDy / edgeLength;
        normalY = -BLADE_WINDING * nearestDx / edgeLength;
      }
      const worldAngle = state.angle + blade * spread;
      const worldCos = Math.cos(worldAngle);
      const worldSin = Math.sin(worldAngle);
      nearestContact = {
        bladeIndex: blade,
        edgeIndex: nearestEdgeIndex,
        leadingEdge: nearestEdgeIndex >= 1 && nearestEdgeIndex <= LEADING_EDGE_END_SEGMENT,
        radiusMm,
        penetration,
        surfaceX: nearestX * worldCos - nearestY * worldSin,
        surfaceY: nearestX * worldSin + nearestY * worldCos,
        normalX: normalX * worldCos - normalY * worldSin,
        normalY: normalX * worldSin + normalY * worldCos,
      };
    }
    return nearestContact;
  }

  function rotorInertia() {
    const initial = Number(tuning.rotorInertia.value);
    const brokenCount = state.brokenBlades.filter(Boolean).length;
    return Math.max(1e-6, initial * GAME_CATALOG.materials[state.campaign.material].inertia
      * Math.max(.2, 1 - brokenCount * .8 / BLADE_COUNT));
  }

  function breakBlade(index, reason, directionBeforeImpact) {
    if (state.brokenBlades[index]) return;
    playBreakSound();
    const direction = Math.sign(directionBeforeImpact) || Math.sign(state.omega);
    if (direction && state.omega * direction < 0) {
      state.omega = 0;
    } else {
      state.omega *= .82;
    }
    const damage = {
      stubRadiusMm: HUB_MM + 1 + Math.random() * 2,
      seed: Math.random(),
    };
    state.brokenBlades[index] = damage;
    state.flyouts.push({
      angle: state.angle + index * Math.PI * 2 / BLADE_COUNT,
      startTime: state.animationTime,
      spin: (Math.random() * 2 - 1) * 7,
    });
    if (state.pointer && state.pointer.contactBlade === index) state.pointer.contactBlade = null;
    liveIndicator.classList.add("is-broken");
    state.breakNotice = { text: reason + "。", until: performance.now() + 2500 };
    renderMessage();
    persistCampaign(true);
    renderWorkshop();
  }

  function wearHandGear(relativeSpeed) {
    const hand = state.campaign.hand;
    if (hand === "hand" || state.campaign.handDurability[hand] <= 0) return;
    const excessSpeed = Math.max(0, relativeSpeed - GAME_CATALOG.gearImpact.thresholdMs);
    if (!excessSpeed) return;
    const previous = state.campaign.handDurability[hand];
    state.campaign.handDurability[hand] = Math.max(
      0,
      state.campaign.handDurability[hand] - excessSpeed ** 2 * GAME_CATALOG.gearImpact.durabilityPerMs2,
    );
    state.impactWear += previous - state.campaign.handDurability[hand];
    renderWorkshop();
    persistCampaign(true);
  }

  function updateReadout() {
    const currentRpm = signedRpm();
    const shownRpm = Math.round(currentRpm) || 0;
    rpmText.textContent = shownRpm.toLocaleString("zh-CN");
    document.getElementById("cfmNow").textContent = currentAirflowCfm().toFixed(1);
    turnsText.textContent = (state.totalRadians / (Math.PI * 2)).toFixed(2);
    const reverse = currentRpm < 0;
    speedFill.classList.toggle("is-reverse", reverse);
    rpmValue.classList.toggle("is-reverse", reverse);
    const fill = reverse
      ? clamp(-currentRpm / REVERSE_MAX_RPM, 0, 1) * 9.09
      : clamp(currentRpm / MAX_RPM, 0, 1) * 90.91;
    speedFill.style.width = fill.toFixed(2) + "%";
    speedProgress.setAttribute("aria-valuenow", String(Math.round(clamp(currentRpm, -REVERSE_MAX_RPM, MAX_RPM))));
    const spinning = Math.abs(state.omega) > .01;
    const holding = state.pointer && state.pointer.contact !== "none";
    const brokenCount = state.brokenBlades.filter(Boolean).length;
    liveIndicator.textContent = spinning
      ? (state.omega > 0 ? "顺时针" : "逆时针")
      : (holding ? "已挡停" : "待机");
    liveIndicator.classList.toggle("is-spinning", spinning);
    liveIndicator.classList.toggle("is-broken", brokenCount > 0);
    renderMessage();
    if (performance.now() - state.lastWorkshopReadout >= 500) {
      updateCampaignReadout();
      state.lastWorkshopReadout = performance.now();
    }
  }

  function hasActiveAnimation() {
    return state.omega !== 0 || Boolean(state.pointer)
      || state.flyouts.length > 0;
  }

  function scheduleFrame() {
    if (state.framePending || document.hidden) return;
    state.framePending = true;
    window.requestAnimationFrame(frame);
  }

  function wakeAnimation() {
    if (!state.framePending) state.lastFrame = performance.now();
    scheduleFrame();
  }

  function pointerMotionAt(pointer, now) {
    const segments = pointer.motionSegments;
    while (segments.length > 1 && segments[1].startTime < now) segments.shift();
    const segment = segments[0];
    if (!segment || now < segment.startTime) {
      return { point: segment ? segment.startPoint : pointer.point, velocityX: 0, velocityY: 0 };
    }
    const duration = Math.max(segment.endTime - segment.startTime, MIN_POINTER_SEGMENT_MS);
    const progress = segment.endTime > segment.startTime
      ? clamp((now - segment.startTime) / (segment.endTime - segment.startTime), 0, 1)
      : 1;
    const decay = Math.exp(-Math.max(0, now - segment.endTime) / CONTACT_RELEASE_MS);
    const metersPerPixel = FRAME_MM / state.size / 1000;
    return {
      point: {
        x: segment.startPoint.x + (segment.endPoint.x - segment.startPoint.x) * progress,
        y: segment.startPoint.y + (segment.endPoint.y - segment.startPoint.y) * progress,
      },
      velocityX: (segment.endPoint.x - segment.startPoint.x) / duration * 1000 * metersPerPixel * decay,
      velocityY: (segment.endPoint.y - segment.startPoint.y) / duration * 1000 * metersPerPixel * decay,
    };
  }

  function fingerAngularVelocity(point, velocityX, velocityY) {
    const fingerX = (point.x - state.size / 2) / state.size / MM / 1000;
    const fingerY = (point.y - state.size / 2) / state.size / MM / 1000;
    const radiusSquared = fingerX * fingerX + fingerY * fingerY;
    return radiusSquared > 1e-12
      ? (fingerX * velocityY - fingerY * velocityX) / radiusSquared
      : 0;
  }

  function physicsStep(dt, now) {
    const pointer = state.pointer;
    let driveTorque = 0;
    let angularImpulse = 0;
    let pendingBreak = null;
    const directionBeforeImpact = Math.sign(state.omega);
    if (pointer) {
      pointer.physicalContact = false;
      const motion = pointerMotionAt(pointer, now);
      const fingerPoint = motion.point;
      const contactMode = contactAt(fingerPoint);
      const fingerX = (fingerPoint.x - state.size / 2) / state.size / MM / 1000;
      const fingerY = (fingerPoint.y - state.size / 2) / state.size / MM / 1000;
      const fingerOmega = fingerAngularVelocity(fingerPoint, motion.velocityX, motion.velocityY);
      const inertia = rotorInertia();
      if (contactMode === "hub") {
        pointer.grip = null;
        pointer.physicalContact = true;
        const requestedTorque = inertia * (fingerOmega - state.omega) / dt;
        const radius = Math.hypot(
          fingerPoint.x - state.size / 2,
          fingerPoint.y - state.size / 2,
        ) / state.size * FRAME_MM / 1000;
        const frictionLimit = Number(tuning.hubFriction.value) * radius * handFrictionFactor();
        driveTorque = clamp(requestedTorque, -frictionLimit, frictionLimit);
      } else {
        let contact = null;
        if (contactMode === "blade") {
          if (pointer.contactBlade !== null) {
            contact = bladeContact(pointer, { onlyBlade: pointer.contactBlade, point: fingerPoint });
            if (!contact) {
              pointer.contactBlade = null;
            }
          }
          if (!contact) {
            contact = bladeContact(pointer, { point: fingerPoint });
          }
        }
        if (contact) {
          pointer.physicalContact = true;
          pointer.contactBlade = contact.bladeIndex;
          const fingerAngle = angleAt(fingerPoint);
          if (!pointer.grip || pointer.grip.blade !== contact.bladeIndex) {
            pointer.grip = { blade: contact.bladeIndex, fingerAngle, rotorAngle: state.angle };
          } else {
            let delta = fingerAngle - pointer.grip.fingerAngle;
            if (delta > Math.PI) delta -= Math.PI * 2;
            if (delta < -Math.PI) delta += Math.PI * 2;
            pointer.grip.rotorAngle += delta;
            pointer.grip.fingerAngle = fingerAngle;
          }
          const surfaceX = contact.surfaceX / 1000;
          const surfaceY = contact.surfaceY / 1000;
          const bladeVelocityX = -state.omega * surfaceY;
          const bladeVelocityY = state.omega * surfaceX;
          // A compliant fingertip slides along the swept edge, so only its
          // circumferential motion can push a blade; radial motion must not wedge it backward.
          const fingerVelocityX = -fingerOmega * fingerY;
          const fingerVelocityY = fingerOmega * fingerX;
          const relativeNormalVelocity = (fingerVelocityX - bladeVelocityX) * contact.normalX
            + (fingerVelocityY - bladeVelocityY) * contact.normalY;
          let hardCollision = false;
          if (contact.leadingEdge) {
            const momentArm = surfaceX * contact.normalY - surfaceY * contact.normalX;
            if (relativeNormalVelocity < 0 && Math.abs(momentArm) > 1e-6) {
              hardCollision = true;
              const effectiveMass = inertia / (momentArm * momentArm);
              const gear = effectiveHandGear();
              const compression = Math.max(
                (Number(tuning.fingerCompression.value) + gear.compressionBonusMm) / 1000,
                1e-6,
              );
              const averageImpactForce = .5 * effectiveMass
                * relativeNormalVelocity * relativeNormalVelocity / compression;
              const peakImpactForce = IMPACT_PEAK_FACTOR * averageImpactForce;
              const rootX = contact.surfaceX * HUB_MM / contact.radiusMm / 1000;
              const rootY = contact.surfaceY * HUB_MM / contact.radiusMm / 1000;
              const rootMomentArm = Math.abs(
                (surfaceX - rootX) * contact.normalY
                  - (surfaceY - rootY) * contact.normalX,
              );
              const bladeBroken = rootMomentArm * peakImpactForce
                >= Number(tuning.rootStrength.value)
                  * GAME_CATALOG.materials[state.campaign.material].rootStrength;
              if (bladeBroken) {
                pendingBreak = { index: contact.bladeIndex, reason: "前缘冲击过大，叶根断裂" };
              }
              wearHandGear(Math.abs(relativeNormalVelocity));
              angularImpulse = -momentArm * (-relativeNormalVelocity * effectiveMass)
                * (bladeBroken ? .35 : 1);
            }
          }
          if (!hardCollision || !pendingBreak) {
            const radius = Math.hypot(surfaceX, surfaceY);
            const gear = effectiveHandGear();
            const handForce = handFrictionFactor()
              * GAME_CATALOG.materials[state.campaign.material].friction;
            const counterclockwiseLimit = Number(tuning.ccwFingerForce.value) * radius * handForce;
            const clockwiseLimit = Number(tuning.cwFingerForce.value) * radius * handForce;
            const compression = Math.max(
              (Number(tuning.fingerCompression.value) + gear.compressionBonusMm) / 1000,
              1e-6,
            );
            const relativeOmega = fingerOmega - (state.omega + angularImpulse / inertia);
            const gripError = pointer.grip.rotorAngle - state.angle;
            const slipDirection = Math.abs(relativeOmega) > 1e-3
              ? Math.sign(relativeOmega)
              : Math.sign(gripError);
            if (!slipDirection) {
              driveTorque = 0;
            } else {
              const tractionLimit = slipDirection > 0 ? clockwiseLimit : counterclockwiseLimit;
              const angularCompression = compression / Math.max(radius, 1e-6);
              const stiffness = tractionLimit / angularCompression;
              const damping = 2 * FINGER_DAMPING_RATIO * Math.sqrt(stiffness * inertia);
              const requestedTorque = stiffness * gripError + damping * relativeOmega;
              driveTorque = slipDirection > 0
                ? clamp(requestedTorque, 0, clockwiseLimit)
                : clamp(requestedTorque, -counterclockwiseLimit, 0);
            }
            const tangentialForce = driveTorque / Math.max(radius, 1e-6);
            const rootRadius = HUB_MM / 1000;
            const rootMoment = Math.abs(tangentialForce) * Math.max(0, radius - rootRadius);
            const bladeBroken = rootMoment >= Number(tuning.rootStrength.value)
              * GAME_CATALOG.materials[state.campaign.material].rootStrength;
            if (bladeBroken) {
              pendingBreak = { index: contact.bladeIndex, reason: "叶根弯矩过大，叶片断裂" };
            }
          }
        } else {
          pointer.grip = null;
        }
      }
    }

    const coggingTorque = -Number(tuning.coggingStrength.value) * Math.sin(12 * state.angle);
    const aerodynamicTorque = -AIR_DRAG * intactBladeFraction() * state.omega * Math.abs(state.omega);
    const nonBearingTorque = driveTorque + coggingTorque + aerodynamicTorque;
    const bearing = GAME_CATALOG.bearings[state.campaign.bearing];
    const bearingScale = bearing.resistance * bearingWearMultiplier();
    let bearingTorque;
    if (Math.abs(state.omega) < .02) {
      const initialResistance = Number(tuning.bearingInitialResistance.value) * bearingScale;
      if (Math.abs(nonBearingTorque) <= initialResistance) {
        state.omega = 0;
        bearingTorque = -nonBearingTorque;
      } else {
        bearingTorque = -Math.sign(nonBearingTorque) * initialResistance
          - Number(tuning.bearingResistanceSlope.value) * bearingScale * state.omega;
      }
    } else {
      bearingTorque = -Math.sign(state.omega) * (
        Number(tuning.bearingInitialResistance.value)
        + Number(tuning.bearingResistanceSlope.value) * Math.abs(state.omega)
      );
      bearingTorque *= bearingScale;
    }
    const inertia = rotorInertia();
    state.omega += angularImpulse / inertia;
    state.omega += (nonBearingTorque + bearingTorque) / inertia * dt;
    if (pendingBreak) {
      breakBlade(
        pendingBreak.index,
        pendingBreak.reason,
        directionBeforeImpact || Math.sign(angularImpulse) || Math.sign(driveTorque),
      );
    }
    rotateRotor(state.omega * dt);
    updateEconomy(dt);
  }

  function frame(now) {
    state.framePending = false;
    if (document.hidden || remoteGame && (!remoteGame.ready || remoteGame.busy)) {
      state.lastFrame = now;
      updateSound();
      return;
    }
    if (!state.lastFrame) state.lastFrame = now;
    const dt = clamp((now - state.lastFrame) / 1000, 0, .25);
    const stepStart = now - dt * 1000;
    state.lastFrame = now;
    state.animationTime = now;

    const activeBeforeStep = hasActiveAnimation();
    if (dt > 0 && activeBeforeStep) {
      const pointerSpeed = state.pointer ? state.pointer.motionSegments.reduce((speed, segment) => {
        if (segment.endTime < stepStart || segment.startTime > now) return speed;
        return Math.max(speed, Math.hypot(
          segment.endPoint.x - segment.startPoint.x,
          segment.endPoint.y - segment.startPoint.y,
        ) / Math.max(segment.endTime - segment.startTime, 1) * 1000);
      }, 0) : 0;
      const steps = Math.max(
        1,
        Math.ceil(dt / PHYSICS_STEP),
        Math.ceil(dt * Math.abs(state.omega) / .03),
        Math.ceil(dt * pointerSpeed / (FINGER_RADIUS_MM * state.size * MM / 2)),
      );
      const step = dt / steps;
      for (let i = 0; i < steps; i += 1) {
        physicsStep(step, stepStart + (i + 1) * step * 1000);
      }
    }
    if (activeBeforeStep) draw();
    const keepAnimating = hasActiveAnimation();
    updateSound();
    if (now - state.lastReadout > 55 || !keepAnimating) {
      updateReadout();
      state.lastReadout = now;
    }
    if (keepAnimating) scheduleFrame();
  }

  function startPointer(event, startedOutside = false) {
    if (remoteGame && (!event.isTrusted || !remoteGame.ready || remoteGame.busy)) return;
    if (state.pointer || event.button !== 0) return;
    if (!startedOutside || event.pointerType === "touch") event.preventDefault();
    initializeSound();
    syncSweatRecovery();
    const point = localPoint(event);
    const now = pointerEventTime(event);
    state.pointer = {
      id: event.pointerId,
      point,
      time: now,
      contact: contactAt(point),
      contactBlade: null,
      physicalContact: false,
      motionSegments: [],
      gestureSpeed: 0,
      gestureMotionAt: now,
      startedOutside,
    };
    state.campaign.sweatRestAt = 0;
    scheduleMaintenance();
    if (state.pointer.contact === "blade") {
      const contact = bladeContact(state.pointer);
      if (contact) {
        state.pointer.contactBlade = contact.bladeIndex;
        state.pointer.grip = { blade: contact.bladeIndex, fingerAngle: angleAt(point), rotorAngle: state.angle };
      }
    }
    if (!startedOutside || event.pointerType === "touch") canvas.setPointerCapture(event.pointerId);
    updateReadout();
    wakeAnimation();
  }
  canvas.addEventListener("pointerdown", event => startPointer(event));

  function movePointer(event) {
    if (remoteGame && !event.isTrusted) return;
    const pointer = state.pointer;
    if (!pointer || pointer.id !== event.pointerId) return;
    const point = localPoint(event);
    const now = Math.max(pointerEventTime(event), pointer.time);
    const elapsed = Math.max((now - pointer.time) / 1000, .001);
    pointer.motionSegments.push({
      startPoint: pointer.point,
      endPoint: point,
      startTime: pointer.time,
      endTime: now,
    });
    pointer.gestureSpeed = Math.hypot(point.x - pointer.point.x, point.y - pointer.point.y)
      / state.size * FRAME_MM / 1000 / elapsed;
    pointer.gestureMotionAt = now;
    const contact = contactAt(point);
    if (contact !== pointer.contact) {
      pointer.contactBlade = null;
    }
    pointer.point = point;
    pointer.time = now;
    pointer.contact = contact;
    wakeAnimation();
  }
  canvas.addEventListener("pointermove", event => {
    if (state.pointer && !state.pointer.startedOutside) movePointer(event);
  });
  document.addEventListener("pointerdown", event => {
    if ((event.pointerType !== "mouse" && event.pointerType !== "touch") || event.target === canvas
      || (event.target.closest && event.target.closest("button, input, summary, a, .workshop, .status-content, .status-panel, .status-hint"))) return;
    startPointer(event, true);
  });
  document.addEventListener("pointermove", event => {
    if (state.pointer && state.pointer.startedOutside) movePointer(event);
  });

  function releasePointer(event) {
    const pointer = state.pointer;
    if (!pointer || (event && pointer.id !== event.pointerId)) return;
    state.pointer = null;
    state.campaign.sweatRestAt = gameNow();
    persistCampaign(true);
    scheduleMaintenance();
    state.lastFrame = performance.now();
    wakeAnimation();
  }
  canvas.addEventListener("pointerup", releasePointer);
  canvas.addEventListener("pointercancel", releasePointer);
  canvas.addEventListener("lostpointercapture", releasePointer);
  document.addEventListener("pointerup", event => {
    finishControlPress(event);
    if (state.pointer && state.pointer.startedOutside) releasePointer(event);
  });
  document.addEventListener("pointercancel", event => {
    finishControlPress(event);
    if (state.pointer && state.pointer.startedOutside) releasePointer(event);
  });
  window.addEventListener("blur", () => {
    releasePointer();
    state.controlPresses.clear();
    flushControlRefresh();
  });

  document.getElementById("startCoast")?.addEventListener("click", () => {
    if (remoteGame) return;
    initializeSound();
    const rpm = Number(coastRpm.value);
    const omega = -rpm * Math.PI / 30;
    if (coastRpm.value.trim() === "" || !Number.isFinite(rpm) || !Number.isFinite(omega)) {
      coastStatus.textContent = "请输入有效的有限转速（RPM）。";
      return;
    }
    releasePointer();
    state.omega = omega;
    state.lastFrame = performance.now();
    coastStatus.textContent = `已从 ${rpm} RPM 开始自由减速；再次接触风扇会影响减速过程。`;
    updateReadout();
    draw();
    wakeAnimation();
  });

  resetButton?.addEventListener("click", () => {
    if (remoteGame) return;
    state.omega = 0;
    state.angle = 0;
    state.totalRadians = 0;
    state.brokenBlades.fill(null);
    state.flyouts = [];
    state.pointer = null;
    state.campaign.sweatRestAt = gameNow();
    scheduleMaintenance();
    state.animationTime = performance.now();
    liveIndicator.classList.remove("is-broken", "is-spinning");
    state.breakNotice = null;
    message.textContent = "";
    updateReadout();
    draw();
    renderWorkshop();
    persistCampaign(true);
    wakeAnimation();
  });

  const tuningLabels = {
    rootStrength: "rootStrengthValue",
    hubFriction: "hubFrictionValue",
    bearingInitialResistance: "bearingInitialResistanceValue",
    bearingResistanceSlope: "bearingResistanceSlopeValue",
    ccwFingerForce: "ccwFingerForceValue",
    cwFingerForce: "cwFingerForceValue",
    fingerCompression: "fingerCompressionValue",
    coggingStrength: "coggingStrengthValue",
    rotorInertia: "rotorInertiaValue",
    windNoiseGain: "windNoiseGainValue",
    bladeToneGain: "bladeToneGainValue",
    ballRustleGain: "ballRustleGainValue",
    airflowSoundExponent: "airflowSoundExponentValue",
  };
  const tuningPrecision = {
    rootStrength: 2,
    hubFriction: 2,
    bearingInitialResistance: 6,
    bearingResistanceSlope: 6,
    ccwFingerForce: 2,
    cwFingerForce: 2,
    fingerCompression: 1,
    coggingStrength: 4,
    rotorInertia: 6,
    windNoiseGain: 2,
    bladeToneGain: 2,
    ballRustleGain: 2,
    airflowSoundExponent: 2,
  };
  const tuningDefaults = {};
  const updateTuning = {};
  for (const key of Object.keys(tuningLabels)) tuningDefaults[key] = String(GAME_CATALOG.tuning[key]);
  for (const [key, outputId] of Object.entries(tuningLabels)) {
    let lastValid = tuningDefaults[key];
    const updateValue = () => {
      if (remoteGame) tuning[key].value = tuningDefaults[key];
      const value = Number(tuning[key].value);
      if (tuning[key].value.trim() === "" || !Number.isFinite(value) || value < 0) {
        tuning[key].value = lastValid;
        return;
      }
      lastValid = tuning[key].value;
      const output = document.getElementById(outputId);
      if (output) output.textContent = value.toFixed(tuningPrecision[key]);
      if (key === "ballRustleGain") updateSound();
    };
    tuning[key].addEventListener?.("input", updateValue);
    document.getElementById(key + "Reset")?.addEventListener("click", () => {
      tuning[key].value = tuningDefaults[key];
      lastValid = tuningDefaults[key];
      updateValue();
    });
    updateTuning[key] = updateValue;
    tuning[key].value = tuningDefaults[key];
    updateValue();
  }

  if ("ResizeObserver" in window) {
    new ResizeObserver(resize).observe(canvas);
  } else {
    window.addEventListener("resize", resize);
  }
  document.addEventListener("visibilitychange", () => {
    if (document.hidden && state.pointer) releasePointer();
    state.lastFrame = performance.now();
    syncSweatRecovery();
    renderWorkshop();
    scheduleMaintenance();
    if (!document.hidden && Math.abs(state.omega) > .01) initializeSound();
    updateSound();
    if (!document.hidden && hasActiveAnimation()) scheduleFrame();
  });
  const debugPanel = document.getElementById("debugPanel");
  if (debugPanel) debugPanel.hidden = serverClient
    ? !serverClient.sandbox
    : new URLSearchParams(window.location.search || "").get("debug") !== "1";
  // Narrow screens show status as a dropdown in row 2; wide screens tile it under the readout metrics.
  const narrowQuery = window.matchMedia("(max-width: 760px)");
  const placeStatusContent = () => {
    const statusPanel = document.getElementById("statusPanel");
    const target = narrowQuery.matches ? statusPanel : document.getElementById("statusDock");
    target.appendChild(document.getElementById("statusContent"));
    if (!narrowQuery.matches) statusPanel.open = false;
    document.getElementById("statusHint").hidden = !narrowQuery.matches || statusPanel.open;
  };
  if (narrowQuery.addEventListener) narrowQuery.addEventListener("change", placeStatusContent);
  placeStatusContent();
  const dismissStatusHint = () => {
    document.getElementById("statusHint").hidden = true;
    try {
      window.localStorage.setItem("fanGameStatusHintSeen", "1");
    } catch (error) {
      console.warn("无法保存耐久提示偏好。", error);
    }
  };
  try {
    if (window.localStorage.getItem("fanGameStatusHintSeen") === "1") {
      document.getElementById("statusHint").hidden = true;
    }
  } catch (error) {
    console.warn("无法读取耐久提示偏好。", error);
  }
  document.getElementById("statusPanel").addEventListener("toggle", () => {
    if (document.getElementById("statusPanel").open) {
      dismissStatusHint();
      if (state.pointer) releasePointer();
    }
  });
  document.getElementById("statusClose").addEventListener("click", () => {
    document.getElementById("statusPanel").open = false;
  });
  document.addEventListener("click", event => {
    const panel = document.getElementById("statusPanel");
    if (narrowQuery.matches && panel.open && event.target.closest
      && !event.target.closest("summary, .status-content")) panel.open = false;
  });
  const setWorkshopOpen = open => {
    document.documentElement.classList.toggle("is-workshop-open", open);
    if (open) document.getElementById("statusPanel").open = false;
    if (open && state.pointer) releasePointer();
  };
  document.getElementById("workshopOpen").addEventListener("click", () => setWorkshopOpen(true));
  document.getElementById("workshopClose").addEventListener("click", () => setWorkshopOpen(false));
  document.addEventListener("keydown", event => {
    if (event.key === "Escape") {
      setWorkshopOpen(false);
      document.getElementById("statusPanel").open = false;
    }
  });
  if (remoteGame) {
    remoteGame.connect({
      apply(data, { replace, pending }) {
        const catalogChanged = GAME_CATALOG.version !== data.catalog.version;
        GAME_CATALOG = data.catalog;
        if (catalogChanged) {
          for (const key of Object.keys(tuningDefaults)) {
            tuningDefaults[key] = String(GAME_CATALOG.tuning[key]);
            tuning[key].value = tuningDefaults[key];
            updateTuning[key]();
          }
        }
        renderServerRecords(data.server_records);
        const campaign = JSON.parse(JSON.stringify(data.campaign));
        if (replace) {
          state.campaign = campaign;
          restoreBrokenBlades(campaign.brokenBlades);
          state.totalRadians = data.total_turns * Math.PI * 2;
          state.impactWear = 0;
          state.flyouts = [];
        } else {
          const pendingTurns = pending.reduce((sum, slice) => sum + slice.turns, 0);
          const pendingContact = pending.reduce((sum, slice) => sum + slice.contact_ms, 0) / 1000;
          const pendingImpact = pending.reduce((sum, slice) => sum + slice.impact_wear, 0);
          campaign.bearingTurns[campaign.bearing] += pendingTurns;
          if (campaign.hand !== "hand") {
            campaign.handDurability[campaign.hand] = Math.max(0,
              campaign.handDurability[campaign.hand] - pendingImpact
              - pendingContact / (GAME_CATALOG.hands[campaign.hand].contactMinutes * 60) * 100);
            campaign.handContactSeconds[campaign.hand] += pendingContact;
          }
          campaign.sweat = state.campaign.sweat;
          campaign.sweatRestAt = state.campaign.sweatRestAt;
          campaign.powderSeconds = state.campaign.powderSeconds;
          campaign.brokenBlades = currentBrokenIndices();
          const pendingPeakAirflow = pending.reduce((max, slice) => Math.max(max, slice.peak_airflow), 0);
          campaign.todayMaxAirflow = Math.max(campaign.todayMaxAirflow, pendingPeakAirflow);
          campaign.bestAirflow = Math.max(campaign.bestAirflow, pendingPeakAirflow);
          state.campaign = campaign;
        }
        renderWorkshop();
        updateReadout();
        draw();
        scheduleMaintenance();
        wakeAnimation();
      },
      pause() {
        releasePointer();
      },
      status(text, isError, feedback = true) {
        const sync = document.getElementById("gameSyncStatus");
        sync.textContent = text;
        sync.classList.toggle("is-error", isError);
        document.getElementById("gameRetry").hidden = remoteGame.ready;
        if (feedback) setShopStatus(text, isError);
        wakeAnimation();
      },
    });
  }
  syncSweatRecovery();
  if (serverClient) renderServerRecords(serverClient.snapshot.server_records);
  renderMessage();
  renderWorkshop();
  updateReadout();
  resize();
  persistCampaign(true);
  scheduleMaintenance();
  scheduleFrame();
})();
