from sync_script.cpu_cooler_sync import run as run_cpu
from sync_script.gpu_sync import run as run_gpu
from sync_script.monitor_sync import run as run_monitor
from sync_script.motherboard_sync import run as run_motherboard
from sync_script.processor_sync import run as run_processors
from sync_script.ram_sync import run as run_ram

if __name__ == "__main__":
    for label, fn in [
        #("CPU", run_cpu),
        ("GPU", run_gpu),
        #("Monitor", run_monitor),
        ("Motherboard", run_motherboard),
        ("Processor", run_processors),
        #("RAM", run_ram),
    ]:
        print(f"Starting {label} sync...")
        try:
            fn()
            print(f"{label} sync done ")
        except Exception as e:
            print(f"{label} sync failed — {e}")
