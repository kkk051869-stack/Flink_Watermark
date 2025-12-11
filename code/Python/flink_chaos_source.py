import socket
import time
import random
import datetime

# ==================== 1. 配置区域 ====================

# [监听地址] 保持 0.0.0.0 方便组员连接
BIND_IP = '0.0.0.0'
BIND_PORT = 9999

# [乱序概率] 0.6 表示 60% 的数据会被强制延迟
CHAOS_LEVEL = 0

# [重点] 随机延迟的时间范围 (秒)
# 这决定了你的散点图“带子”有多宽 (这里是 5秒宽)
MIN_DELAY = 0.0
MAX_DELAY = 5.0


# ====================================================

def get_current_ts():
    """获取当前时间戳(毫秒级) - 发送给 Flink 用"""
    return int(time.time() * 1000)


def ts_to_readable(ts_ms):
    """(辅助) 把毫秒转成可读字符串 - 仅用于控制台打印，不发送"""
    dt = datetime.datetime.fromtimestamp(ts_ms / 1000.0)
    return dt.strftime('%H:%M:%S.%f')[:-3]


def start_server_source():
    # 创建 socket 服务端
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # 允许端口复用，防止重启报错
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server.bind((BIND_IP, BIND_PORT))
        server.listen(1)
        print("=" * 60)
        print(f"🎧 [数据源] 已启动 - 端口 {BIND_PORT}")
        print(f"📉 [实验模式] 随机范围延迟 (Bounded Random Delay)")
        print(f"🎲 [配置] {CHAOS_LEVEL * 100}% 概率延迟 {MIN_DELAY}~{MAX_DELAY}秒")
        print("=" * 60)

        print("⏳ 等待 Flink 连接...")
        conn, addr = server.accept()
        print(f"✅ Flink 已连接: {addr}")
        print("-" * 60)

        # 缓冲区：存储元组 (计划释放的物理时间, 数据字符串)
        delayed_buffer = []

        while True:
            try:
                # 1. 生成数据 (Event Time = 现在)
                ts = get_current_ts()
                temp = round(random.uniform(20.0, 30.0), 2)
                # 发送格式：sensor_1,毫秒时间戳,温度
                data_content = f"sensor_1,{ts},{temp}"

                readable_time = ts_to_readable(ts)
                current_wall_time = time.time()  # 获取当前物理时间

                # === 核心逻辑：决定是“立刻发”还是“定个闹钟以后发” ===

                if random.random() < CHAOS_LEVEL:
                    # 【随机延迟】在 0~5秒内生成一个随机数
                    random_delay = random.uniform(MIN_DELAY, MAX_DELAY)

                    # 计算出狱时间 = 当前物理时间 + 随机延迟
                    release_time = current_wall_time + random_delay

                    # 放入缓冲区，贴上释放时间的标签
                    delayed_buffer.append((release_time, data_content))

                    print(f"❌ [扣留] {readable_time} -> 随机延迟 {random_delay:.2f}s")
                else:
                    # 【正常发送】
                    msg = data_content + "\n"
                    conn.send(msg.encode('utf-8'))
                    print(f"🚀 [正常] {readable_time} -> 发送")

                # === 2. 检查缓冲区：谁的闹钟响了？ ===
                remaining_buffer = []
                check_time = time.time()

                for r_time, data in delayed_buffer:
                    if check_time >= r_time:
                        # 时间到了！发射！
                        conn.send((data + "\n").encode('utf-8'))

                        # 解析时间用于打印提示
                        raw_ts = int(data.split(',')[1])
                        print(f"⚠️ [补发] 迟到的 {ts_to_readable(raw_ts)} 终于发出")
                    else:
                        # 时间还没到，继续留级
                        remaining_buffer.append((r_time, data))

                # 更新缓冲区，只保留没发出去的
                delayed_buffer = remaining_buffer

                # 控制生产速度 (每秒约10条)
                time.sleep(0.1)

            except BrokenPipeError:
                print("❌ Flink 断开了连接");
                break
            except ConnectionResetError:
                print("❌ 连接被重置");
                break

    except Exception as e:
        print(f"❌ 发生错误: {e}")
    finally:
        server.close()
        print("🛑 服务端已关闭")


if __name__ == "__main__":
    start_server_source()