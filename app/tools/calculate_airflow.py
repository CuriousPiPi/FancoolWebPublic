import numpy as np
from scipy.optimize import curve_fit
import matplotlib.pyplot as plt

# ---------- 单位换算工具 ----------

def cfm_to_m3s(Q_cfm):
    """立方英尺每分钟 (cfm) -> 立方米每秒 (m^3/s)."""
    # 1 ft^3 = 0.028316846592 m^3
    return Q_cfm * 0.028316846592 / 60.0

def m3s_to_cfm(Q_m3s):
    """立方米每秒 (m^3/s) -> 立方英尺每分钟 (cfm)."""
    return Q_m3s * 60.0 / 0.028316846592


# ---------- 幂律剖面与拟合 ----------

def power_law_profile(r, v_max, n, R):
    """
    幂律速度剖面: v(r) = v_max * (1 - r/R)^n
    r: 半径 (m), 可以是 numpy 数组
    v_max: 拟合得到的中心线最大速度 (m/s)
    n: 幂指数 (>0)
    R: 管道半径 (m)
    """
    x = np.clip(1.0 - r / R, 0.0, 1.0)
    return v_max * x**n


def fit_power_law(D, r_list, v_list):
    """
    拟合幂律剖面参数 v_max 和 n

    参数:
    - D: 管道内径 (m)
    - r_list: 测点半径列表 (m)
    - v_list: 对应风速列表 (m/s)

    返回:
    - v_max_fit: 拟合得到的中心最大速度 (m/s)
    - n_fit: 拟合得到的幂指数
    """
    R = D / 2.0
    r = np.asarray(r_list, dtype=float)
    v = np.asarray(v_list, dtype=float)

    if r.shape != v.shape:
        raise ValueError("r_list 和 v_list 长度不一致")

    if np.any(r < 0):
        raise ValueError("半径 r_list 中存在小于 0 的值")

    if np.any(r > R * 1.0001):
        raise ValueError("半径 r_list 中存在大于管道半径 R 的值")

    # 按半径升序排序
    sort_idx = np.argsort(r)
    r = r[sort_idx]
    v = v[sort_idx]

    # 初始猜测
    v_max0 = float(np.max(v))  # 最大测得速度作为 v_max 初值
    n0 = 7.0                   # 经验初值

    def model(r, v_max, n):
        return power_law_profile(r, v_max, n, R)

    # 给参数设置边界, 防止拟合发散
    bounds_lower = [0.1 * max(v_max0, 0.01), 0.1]
    bounds_upper = [10.0 * max(v_max0, 0.01), 50.0]

    popt, pcov = curve_fit(
        model,
        r,
        v,
        p0=[v_max0, n0],
        bounds=(bounds_lower, bounds_upper),
        maxfev=10000
    )

    v_max_fit, n_fit = popt
    return float(v_max_fit), float(n_fit), r, v  # 返回排序后的 r, v 方便画图


# ---------- 主接口：从仪器显示风量 -> 真实风量 + 画图 ----------

def compute_true_flow_from_cfm(
    D,
    r_list,
    Q_cfm_list,
    num_points_integral=1000,
    make_plot=True
):
    """
    已知：仪器在不同半径 r_i 位置显示的风量 Q_display_i (cfm)，
    仪器是用 v_local * A 直接算的，所以：
      v_local(r_i) = Q_display_i(m^3/s) / A

    步骤：
      1. 把 Q_display_i (cfm) 转成 m^3/s
      2. 用 A = π (D/2)^2 求出每个点的等效局部风速 v_i (m/s)
      3. 用 (r_i, v_i) 做幂律拟合，得到 v_max, n
      4. 在 [0, R] 上积分 v(r) 计算真实风量 Q_true (m^3/s)
      5. 可选：画出测点与拟合的幂律曲线

    参数:
    - D: 管道内径 (m)
    - r_list: 测点半径列表 (m)
    - Q_cfm_list: 对应半径处仪器显示的风量 (cfm)
    - num_points_integral: 数值积分在 [0, R] 上的细分点数
    - make_plot: 是否画图 (Jupyter 中推荐设为 True)

    返回:
    - Q_true_m3s: 拟合剖面积分得到的真实风量 (m^3/s)
    - Q_true_cfm: 同上, 转成 cfm
    - v_max_fit: 拟合出的中心线最大风速 (m/s)
    - n_fit: 拟合出的幂指数
    """
    R = D / 2.0
    A = np.pi * R**2  # 截面积 (m^2)

    r = np.asarray(r_list, dtype=float)
    Q_cfm = np.asarray(Q_cfm_list, dtype=float)

    if r.shape != Q_cfm.shape:
        raise ValueError("r_list 和 Q_cfm_list 长度不一致")

    # 1. cfm -> m^3/s
    Q_m3s = cfm_to_m3s(Q_cfm)

    # 2. 等效局部风速 v_i = Q_i / A
    v_local = Q_m3s / A  # m/s

    # 3. 拟合幂律参数 (顺带拿到排序后的 r_sorted, v_sorted)
    v_max_fit, n_fit, r_sorted, v_sorted = fit_power_law(D, r, v_local)

    # 4. 积分计算真实风量
    r_fine = np.linspace(0.0, R, num_points_integral)
    v_fine = power_law_profile(r_fine, v_max_fit, n_fit, R)
    integrand = v_fine * r_fine
    Q_true_m3s = 2.0 * np.pi * np.trapz(integrand, r_fine)
    Q_true_cfm = m3s_to_cfm(Q_true_m3s)

    # 5. 画图（在 Jupyter 中会自动 inline 显示）
    if make_plot:
        plt.figure(figsize=(6, 4))
        # 实测等效局部风速散点
        plt.scatter(r_sorted, v_sorted, color='C0', label='Measured (eq. local v)', zorder=3)
        # 拟合曲线
        plt.plot(r_fine, v_fine, color='C1', label='Fitted power-law profile')
        plt.xlabel('Radius r (m)')
        plt.ylabel('Axial velocity v (m/s)')
        plt.title('Radial velocity profile (power-law fit)')
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        plt.show()

    return Q_true_m3s, Q_true_cfm, v_max_fit, n_fit


# ---------- 示例：在 Jupyter 里直接运行 ----------

if __name__ == "__main__":
    # 圆形风管直径 0.30 m
    D = 0.30  # m

    # 测点半径 (m)
    r_list = [0.00, 0.015, 0.03, 0.06, 0.08, 0.1, 0.12, 0.14]

    # 仪器在这些点显示的风量 (cfm) 
    Q_cfm_list = [116.4, 115.44, 113.36, 108.48, 107.36, 106.68, 101.03, 87.75]

    Q_true_m3s, Q_true_cfm, v_max_fit, n_fit = compute_true_flow_from_cfm(
        D,
        r_list,
        Q_cfm_list,
        num_points_integral=2000,
        make_plot=True
    )

    print("拟合得到的速度剖面参数：")
    print(f"  v_max_fit = {v_max_fit:.3f} m/s")
    print(f"  n_fit     = {n_fit:.3f}")

    print("\n幂律拟合 + 积分得到的真实风量：")
    print(f"  Q_true    = {Q_true_m3s:.6f} m^3/s")
    print(f"  Q_true    = {Q_true_cfm:.3f} cfm")