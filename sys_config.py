import platform
import psutil
import os

def get_system_info():
    info = {
        "OS": platform.system(),
        "OS Version": platform.version(),
        "Processor": platform.processor(),
        "CPU Cores (Physical)": psutil.cpu_count(logical=False),
        "CPU Cores (Logical)": psutil.cpu_count(logical=True),
        "RAM (GB)": round(psutil.virtual_memory().total / (1024 ** 3), 2),
        "Disk Total (GB)": round(psutil.disk_usage('/').total / (1024 ** 3), 2),
        "Disk Used (GB)": round(psutil.disk_usage('/').used / (1024 ** 3), 2),
        "Disk Free (GB)": round(psutil.disk_usage('/').free / (1024 ** 3), 2)
    }
    return info

if __name__ == "__main__":
    system_info = get_system_info()
    print("🔍 System Configuration:\n")
    for key, value in system_info.items():
        print(f"{key}: {value}")
