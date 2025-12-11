import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
from matplotlib import rcParams

# 设置中文字体支持
rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Arial Unicode MS']
rcParams['axes.unicode_minus'] = False

# ==========================================
# 1. 配置与数据加载
# ==========================================
BASE_DIR = "./data"
DISORDER_LEVELS = [0, 3, 6]  # 乱序等级
WINDOW_TYPES = ['RollingWindow', 'SlidingWindow']
DELAYS = [0, 2000, 4000, 5000, 8000, 10000, 15000, 20000]

SUMMARY_COLS = ['delay_setting', 'window_start', 'window_end', 'count', 'window_avg', 'trigger_time', 'trigger_lag']
DETAIL_COLS = ['event_time', 'proc_time', 'value', 'delay_setting', 'win_start', 'win_end', 'trig_time', 'lag',
               'win_avg', 'late_flag', 'dropped_flag']

def load_all_data():
    """加载所有乱序等级和窗口类型的数据"""
    all_summary = []
    all_detail = []
    
    for disorder in DISORDER_LEVELS:
        for window_type in WINDOW_TYPES:
            data_dir = os.path.join(BASE_DIR, f"乱序{disorder}_flink_csv", window_type)
            
            if not os.path.exists(data_dir):
                print(f"警告: 目录不存在 - {data_dir}")
                continue
            
            print(f"正在加载: 乱序{disorder}/{window_type}...")
            
            for d in DELAYS:
                # 加载 Summary
                sum_path = os.path.join(data_dir, f"summary_{d}.csv")
                if os.path.exists(sum_path):
                    try:
                        df_s = pd.read_csv(sum_path, names=SUMMARY_COLS, header=None)
                        df_s['disorder_level'] = disorder
                        df_s['window_type'] = window_type
                        all_summary.append(df_s)
                    except (pd.errors.EmptyDataError, Exception) as e:
                        print(f"  跳过 {sum_path}: {e}")
                
                # 加载 Detail
                det_path = os.path.join(data_dir, f"detail_{d}.csv")
                if os.path.exists(det_path):
                    try:
                        df_d = pd.read_csv(det_path, names=DETAIL_COLS, header=None)
                        df_d['disorder_level'] = disorder
                        df_d['window_type'] = window_type
                        all_detail.append(df_d)
                    except (pd.errors.EmptyDataError, Exception) as e:
                        print(f"  跳过 {det_path}: {e}")
    
    if not all_summary or not all_detail:
        print("错误: 未能加载任何数据文件！")
        return None, None
    
    df_summary = pd.concat(all_summary, ignore_index=True)
    df_detail = pd.concat(all_detail, ignore_index=True)
    
    # 转换单位
    df_summary['delay_sec'] = df_summary['delay_setting'] / 1000.0
    df_detail['delay_sec'] = df_detail['delay_setting'] / 1000.0
    
    print(f"\n数据加载完成:")
    print(f"  Summary 总行数: {len(df_summary)}")
    print(f"  Detail 总行数: {len(df_detail)}")
    print(f"  乱序等级: {sorted(df_summary['disorder_level'].unique())}")
    print(f"  窗口类型: {sorted(df_summary['window_type'].unique())}")
    
    return df_summary, df_detail

# ==========================================
# 2. 统计指标计算
# ==========================================

def calculate_metrics(df_summary, df_detail):
    """计算延迟和准确性指标"""
    
    results = []
    
    for disorder in DISORDER_LEVELS:
        for window_type in WINDOW_TYPES:
            # 筛选数据
            summary_subset = df_summary[
                (df_summary['disorder_level'] == disorder) & 
                (df_summary['window_type'] == window_type)
            ]
            detail_subset = df_detail[
                (df_detail['disorder_level'] == disorder) & 
                (df_detail['window_type'] == window_type)
            ]
            
            if summary_subset.empty or detail_subset.empty:
                continue
            
            # 按延迟分组计算指标
            for delay in DELAYS:
                delay_sec = delay / 1000.0
                
                # 延迟指标
                summary_delay = summary_subset[summary_subset['delay_sec'] == delay_sec]
                if not summary_delay.empty:
                    avg_trigger_latency = summary_delay['trigger_lag'].mean()
                else:
                    avg_trigger_latency = np.nan
                
                # 准确性指标
                detail_delay = detail_subset[detail_subset['delay_sec'] == delay_sec]
                if not detail_delay.empty:
                    total_events = len(detail_delay)
                    dropped_events = detail_delay['dropped_flag'].sum()
                    late_events = detail_delay['late_flag'].sum()
                    drop_rate = (dropped_events / total_events) * 100.0
                    late_rate = (late_events / total_events) * 100.0
                else:
                    total_events = 0
                    dropped_events = 0
                    late_events = 0
                    drop_rate = np.nan
                    late_rate = np.nan
                
                results.append({
                    'disorder_level': disorder,
                    'window_type': window_type,
                    'delay_sec': delay_sec,
                    'delay_ms': delay,
                    'avg_trigger_latency_ms': avg_trigger_latency,
                    'total_events': total_events,
                    'dropped_events': dropped_events,
                    'late_events': late_events,
                    'drop_rate_pct': drop_rate,
                    'late_rate_pct': late_rate
                })
    
    return pd.DataFrame(results)

# ==========================================
# 3. 可视化
# ==========================================

def plot_tradeoff_comparison(metrics_df):
    """绘制延迟-准确性权衡对比图"""
    
    fig, axes = plt.subplots(2, 3, figsize=(20, 11))
    fig.suptitle('水位线延迟对窗口触发与准确性的影响分析', fontsize=18, fontweight='bold', y=0.995)
    
    # 定义美观的配色
    color_latency = '#2E86AB'  # 深蓝
    color_drop = '#E63946'     # 红色
    
    for idx, disorder in enumerate(DISORDER_LEVELS):
        for jdx, window_type in enumerate(WINDOW_TYPES):
            ax1 = axes[jdx, idx]
            
            subset = metrics_df[
                (metrics_df['disorder_level'] == disorder) & 
                (metrics_df['window_type'] == window_type)
            ].sort_values('delay_sec')
            
            if subset.empty:
                ax1.text(0.5, 0.5, '无数据', ha='center', va='center', 
                        transform=ax1.transAxes, fontsize=14, color='gray')
                ax1.set_title(f'乱序{disorder} - {window_type}', fontsize=13, pad=10)
                continue
            
            # 左轴：触发延迟
            ax1.set_xlabel('Watermark延迟 (秒)', fontsize=11, fontweight='bold', labelpad=8)
            ax1.set_ylabel('平均触发延迟 (ms)', color=color_latency, fontsize=11, fontweight='bold', labelpad=8)
            line1 = ax1.plot(subset['delay_sec'], subset['avg_trigger_latency_ms'], 
                            color=color_latency, marker='o', linewidth=2.5, markersize=7,
                            label='触发延迟', markerfacecolor='white', markeredgewidth=2)
            ax1.tick_params(axis='y', labelcolor=color_latency, labelsize=10)
            ax1.tick_params(axis='x', labelsize=10)
            ax1.grid(True, linestyle='--', alpha=0.4, linewidth=0.8)
            ax1.spines['top'].set_visible(False)
            
            # 右轴：丢失率
            ax2 = ax1.twinx()
            ax2.set_ylabel('数据丢失率 (%)', color=color_drop, fontsize=11, fontweight='bold', labelpad=8)
            line2 = ax2.plot(subset['delay_sec'], subset['drop_rate_pct'], 
                           color=color_drop, marker='D', linestyle='--', linewidth=2.5, markersize=6,
                           label='丢失率', markerfacecolor='white', markeredgewidth=2)
            ax2.tick_params(axis='y', labelcolor=color_drop, labelsize=10)
            ax2.set_ylim(-2, 105)
            ax2.spines['top'].set_visible(False)
            
            # 标题
            ax1.set_title(f'乱序{disorder} - {window_type}', fontsize=13, fontweight='bold', pad=10)
            
            # 图例 - 放在第一个子图
            if idx == 0 and jdx == 0:
                lines = line1 + line2
                labels = [l.get_label() for l in lines]
                ax1.legend(lines, labels, loc='upper left', fontsize=10, 
                          framealpha=0.9, edgecolor='gray', fancybox=True)
    
    plt.tight_layout(rect=[0, 0, 1, 0.99])
    plt.savefig('img/trade_off_comparison.png', dpi=300, bbox_inches='tight', facecolor='white')
    print("已保存: img/trade_off_comparison.png")
    plt.show()




def plot_heatmap(metrics_df):
    """热力图 - 快速定位最优配置区域"""
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 14))
    fig.suptitle('配置热力图 (Heatmap)', fontsize=16, fontweight='bold', y=0.995)
    
    for idx, window_type in enumerate(WINDOW_TYPES):
        # 丢失率热力图
        ax1 = axes[0, idx]
        pivot_drop = metrics_df[metrics_df['window_type'] == window_type].pivot_table(
            values='drop_rate_pct',
            index='disorder_level',
            columns='delay_sec'
        )
        sns.heatmap(pivot_drop, annot=True, fmt='.1f', cmap='RdYlGn_r',
                   ax=ax1, cbar_kws={'label': '丢失率 (%)'})
        ax1.set_title(f'{window_type} - 数据丢失率', fontsize=12, fontweight='bold', pad=10)
        ax1.set_xlabel('Watermark延迟 (秒)', fontsize=11)
        ax1.set_ylabel('乱序等级', fontsize=11)
        
        # 触发延迟热力图
        ax2 = axes[1, idx]
        pivot_latency = metrics_df[metrics_df['window_type'] == window_type].pivot_table(
            values='avg_trigger_latency_ms',
            index='disorder_level',
            columns='delay_sec'
        )
        sns.heatmap(pivot_latency, annot=True, fmt='.0f', cmap='YlOrRd',
                   ax=ax2, cbar_kws={'label': '触发延迟 (ms)'})
        ax2.set_title(f'{window_type} - 触发延迟', fontsize=12, fontweight='bold', pad=10)
        ax2.set_xlabel('Watermark延迟 (秒)', fontsize=11)
        ax2.set_ylabel('乱序等级', fontsize=11)
    
    plt.tight_layout(rect=[0, 0, 1, 0.99])
    plt.savefig('img/configuration_heatmap.png', dpi=300, bbox_inches='tight', facecolor='white')
    print("已保存: img/configuration_heatmap.png")
    plt.show()

def plot_latency_comparison(metrics_df):
    """数据丢失率随Watermark延迟的变化"""
    
    fig, axes = plt.subplots(1, 2, figsize=(18, 7))
    fig.suptitle('数据丢失率随Watermark延迟的变化', fontsize=16, fontweight='bold', y=0.98)
    
    colors = {0: '#06D6A0', 3: '#FFB627', 6: '#EF476F'}
    markers = {0: 'o', 3: 's', 6: '^'}
    
    for idx, window_type in enumerate(WINDOW_TYPES):
        ax = axes[idx]
        
        for disorder in DISORDER_LEVELS:
            subset = metrics_df[
                (metrics_df['disorder_level'] == disorder) & 
                (metrics_df['window_type'] == window_type)
            ].sort_values('delay_sec')
            
            if subset.empty:
                continue
            
            # X轴：延迟（秒），Y轴：丢失率
            ax.plot(subset['delay_sec'], subset['drop_rate_pct'],
                   color=colors[disorder], marker=markers[disorder],
                   linewidth=2.5, markersize=10, label=f'乱序{disorder}',
                   markerfacecolor='white', markeredgewidth=2.5, alpha=0.9)
            
            # 标注关键点
            for _, row in subset.iterrows():
                if row['delay_sec'] in [0, 10, 20]:
                    ax.annotate(f"{row['delay_sec']:.0f}s", 
                              xy=(row['delay_sec'], row['drop_rate_pct']),
                              xytext=(5, 5), textcoords='offset points',
                              fontsize=8, alpha=0.7)
        
        ax.set_xlabel('Watermark延迟 (秒)', fontsize=12, fontweight='bold')
        ax.set_ylabel('数据丢失率 (%)', fontsize=12, fontweight='bold')
        ax.set_title(f'{window_type}', fontsize=14, fontweight='bold', pad=12)
        ax.grid(True, alpha=0.4, linestyle='--')
        ax.legend(fontsize=11, loc='best', framealpha=0.95)
        ax.tick_params(labelsize=10)
        
        # 添加理想区域标识
        ax.axhspan(0, 5, alpha=0.1, color='green')
        ax.text(0.98, 0.02, '理想准确度区域\n(丢失率<5%)', 
               transform=ax.transAxes, fontsize=9,
               ha='right', va='bottom',
               bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.6))
    
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig('img/tradeoff_curve.png', dpi=300, bbox_inches='tight', facecolor='white')
    print("已保存: img/tradeoff_curve.png")
    plt.show()

# ==========================================
# 4. 分析报告生成
# ==========================================

def generate_analysis_report(metrics_df):
    """生成分析报告"""
    
    print("\n" + "="*80)
    print("水位线延迟对窗口触发与准确性影响的综合分析报告")
    print("="*80)
    
    print("\n【1. 整体统计概览】")
    print("-" * 80)
    for disorder in DISORDER_LEVELS:
        for window_type in WINDOW_TYPES:
            subset = metrics_df[
                (metrics_df['disorder_level'] == disorder) & 
                (metrics_df['window_type'] == window_type)
            ]
            if not subset.empty:
                print(f"\n乱序等级 {disorder} - {window_type}:")
                print(f"  延迟范围: {subset['delay_sec'].min():.1f}s - {subset['delay_sec'].max():.1f}s")
                print(f"  触发延迟范围: {subset['avg_trigger_latency_ms'].min():.2f}ms - {subset['avg_trigger_latency_ms'].max():.2f}ms")
                print(f"  丢失率范围: {subset['drop_rate_pct'].min():.2f}% - {subset['drop_rate_pct'].max():.2f}%")
    
    print("\n\n【2. 水位线延迟对触发时机的影响】")
    print("-" * 80)
    print("观察结果：")
    print("  • 随着Watermark延迟增加，窗口触发延迟线性增加")
    print("  • 这是因为水位线越大，窗口需要等待更长时间才能确认所有数据到达")
    
    for window_type in WINDOW_TYPES:
        print(f"\n  {window_type}:")
        for disorder in DISORDER_LEVELS:
            subset = metrics_df[
                (metrics_df['disorder_level'] == disorder) & 
                (metrics_df['window_type'] == window_type)
            ].sort_values('delay_sec')
            if len(subset) >= 2:
                latency_0 = subset.iloc[0]['avg_trigger_latency_ms']
                latency_max = subset.iloc[-1]['avg_trigger_latency_ms']
                increase = latency_max - latency_0
                print(f"    乱序{disorder}: 延迟从 {latency_0:.2f}ms 增至 {latency_max:.2f}ms (增加 {increase:.2f}ms)")
    
    print("\n\n【3. 水位线延迟对计算准确度的影响】")
    print("-" * 80)
    print("观察结果：")
    print("  • 随着Watermark延迟增加，数据丢失率显著降低")
    print("  • 较小的水位线会导致大量迟到数据被丢弃")
    print("  • 乱序程度越高，需要更大的水位线才能保证准确性")
    
    for window_type in WINDOW_TYPES:
        print(f"\n  {window_type}:")
        for disorder in DISORDER_LEVELS:
            subset = metrics_df[
                (metrics_df['disorder_level'] == disorder) & 
                (metrics_df['window_type'] == window_type)
            ].sort_values('delay_sec')
            if len(subset) >= 2:
                drop_0 = subset.iloc[0]['drop_rate_pct']
                drop_max = subset.iloc[-1]['drop_rate_pct']
                print(f"    乱序{disorder}: 丢失率从 {drop_0:.2f}% 降至 {drop_max:.2f}%")
                
                # 找到丢失率<5%的最小延迟
                acceptable = subset[subset['drop_rate_pct'] < 5.0]
                if not acceptable.empty:
                    min_delay = acceptable['delay_sec'].min()
                    print(f"              建议最小延迟: {min_delay:.1f}s (丢失率<5%)")
    
    print("\n\n【4. 延迟与准确性的权衡策略】")
    print("-" * 80)
    
    print("\n▶ 关键发现:")
    print("  1. 延迟-准确性权衡曲线呈现典型的Trade-off特征")
    print("  2. 存在一个「拐点区域」，在此区域内可以用较小的延迟代价获得显著的准确性提升")
    print("  3. 乱序程度直接影响所需的最小水位线延迟")
    
    print("\n▶ 推荐配置策略:")
    
    for disorder in DISORDER_LEVELS:
        print(f"\n  【乱序等级 {disorder}】")
        
        for window_type in WINDOW_TYPES:
            subset = metrics_df[
                (metrics_df['disorder_level'] == disorder) & 
                (metrics_df['window_type'] == window_type)
            ].sort_values('delay_sec')
            
            if subset.empty:
                continue
            
            print(f"    {window_type}:")
            
            # 低延迟场景
            low_latency = subset[subset['delay_sec'] <= 5].copy()
            if not low_latency.empty:
                best_low = low_latency.sort_values('drop_rate_pct').iloc[0]
                print(f"      • 低延迟优先: {best_low['delay_sec']:.1f}s延迟 → "
                      f"触发延迟{best_low['avg_trigger_latency_ms']:.0f}ms, "
                      f"丢失率{best_low['drop_rate_pct']:.1f}%")
            
            # 平衡场景
            balanced = subset[(subset['drop_rate_pct'] < 5) & (subset['drop_rate_pct'] >= 0)]
            if not balanced.empty:
                best_balanced = balanced.sort_values('avg_trigger_latency_ms').iloc[0]
                print(f"      • 平衡策略: {best_balanced['delay_sec']:.1f}s延迟 → "
                      f"触发延迟{best_balanced['avg_trigger_latency_ms']:.0f}ms, "
                      f"丢失率{best_balanced['drop_rate_pct']:.1f}%")
            
            # 高准确性场景
            high_acc = subset[subset['drop_rate_pct'] < 1]
            if not high_acc.empty:
                best_acc = high_acc.sort_values('avg_trigger_latency_ms').iloc[0]
                print(f"      • 准确性优先: {best_acc['delay_sec']:.1f}s延迟 → "
                      f"触发延迟{best_acc['avg_trigger_latency_ms']:.0f}ms, "
                      f"丢失率{best_acc['drop_rate_pct']:.1f}%")
    
    print("\n\n【5. 最终建议】")
    print("-" * 80)
    print("""
  根据业务场景选择合适的水位线配置：
  
  ① 实时性要求高（如实时监控告警）:
     - 使用较小水位线(2-5秒)
     - 接受一定的数据丢失率(5-10%)
     - 优先保证低延迟响应
  
  ② 准确性要求高（如财务计算、统计报表）:
     - 使用较大水位线(10-20秒)
     - 容忍较高的触发延迟
     - 确保数据完整性(丢失率<1%)
  
  ③ 平衡场景（如一般业务分析）:
     - 根据历史数据的乱序程度动态调整
     - 乱序0: 5秒水位线
     - 乱序3: 8-10秒水位线
     - 乱序6: 15秒以上水位线
  
  ④ 自适应策略:
     - 监控实时的迟到数据比例
     - 当丢失率超过阈值时，动态增加水位线
     - 当系统负载低时，可适当增大水位线以提高准确性
    """)
    
    print("="*80)
    
    # 保存到文件
    with open('analysis_report.txt', 'w', encoding='utf-8') as f:
        f.write("水位线延迟对窗口触发与准确性影响的综合分析报告\n")
        f.write("="*80 + "\n\n")
        f.write(metrics_df.to_string())
    
    print("\n已保存详细数据: analysis_report.txt")

# ==========================================
# 5. 主程序
# ==========================================

def main():
    print("开始综合分析...")
    print("="*80)
    
    # 加载数据
    df_summary, df_detail = load_all_data()
    if df_summary is None or df_detail is None:
        return
    
    # 计算指标
    print("\n计算统计指标...")
    metrics_df = calculate_metrics(df_summary, df_detail)
    
    # 显示数据表
    print("\n关键指标汇总:")
    print(metrics_df.to_string())
    
    # 生成可视化
    print("\n生成可视化图表...")
    plot_tradeoff_comparison(metrics_df)
    plot_latency_comparison(metrics_df)
    plot_heatmap(metrics_df)

    # 生成分析报告
    generate_analysis_report(metrics_df)
    


if __name__ == "__main__":
    main()
