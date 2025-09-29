import os
import sys
import numpy as np
import matplotlib.pyplot as plt
import GangScheduler, generate_tasks

# تنظیمات عمومی
plt.rcParams['font.family'] = 'Tahoma'
plt.rcParams['axes.unicode_minus'] = False

# ایجاد دایرکتوری نتایج
RESULTS_DIR = 'نتایج'
os.makedirs(RESULTS_DIR, exist_ok=True)

# آزمایش 1: تأثیر تعداد هسته‌ها
def experiment_core_impact():
    print("آزمایش 1: تأثیر تعداد هسته‌ها")
    core_counts = [4, 8, 16, 32]
    policies = ['FIFO', 'LDF', 'HEFT']
    num_tasks = 100
    utilization = 0.7
    
    # ذخیره نتایج
    response_results = {policy: [] for policy in policies}
    deadline_results = {policy: [] for policy in policies}
    
    tasks = generate_tasks(num_tasks, utilization)
    
    for cores in core_counts:
        for policy in policies:
            scheduler = GangScheduler(cores, scheduling_policy=policy)
            for task in tasks:
                scheduler.add_task(task)
            
            scheduler.run(5000)
            
            # ذخیره نتایج
            response_results[policy].append(scheduler.metrics['avg_response'])
            deadline_results[policy].append(scheduler.metrics['miss_rate'] * 100)
    
    # رسم نمودار زمان پاسخ
    plt.figure(figsize=(10, 6))
    for policy in policies:
        plt.plot(core_counts, response_results[policy], 'o-', label=policy)
    
    plt.xlabel('تعداد هسته‌ها')
    plt.ylabel('میانگین زمان پاسخ (ms)')
    plt.title('تأثیر تعداد هسته‌ها بر زمان پاسخ')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{RESULTS_DIR}/نمودار_زمان_پاسخ.png', dpi=300)
    plt.close()
    
    # رسم نمودار نرخ رعایت مهلت
    plt.figure(figsize=(10, 6))
    for policy in policies:
        plt.plot(core_counts, deadline_results[policy], 's--', label=policy)
    
    plt.xlabel('تعداد هسته‌ها')
    plt.ylabel('نرخ عدم رعایت مهلت (%)')
    plt.title('تأثیر تعداد هسته‌ها بر رعایت مهلت')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{RESULTS_DIR}/نمودار_رعایت_مهلت.png', dpi=300)
    plt.close()

# آزمایش 2: تأثیر بهره‌وری بر انرژی مصرفی
def experiment_energy_utilization():
    print("آزمایش 2: تأثیر بهره‌وری بر انرژی")
    utilizations = [0.25, 0.5, 0.75]
    policies = ['FIFO', 'LDF', 'HEFT']
    num_tasks = 100
    core_count = 16
    
    energy_results = {policy: [] for policy in policies}
    
    for util in utilizations:
        tasks = generate_tasks(num_tasks, util)
        
        for policy in policies:
            scheduler = GangScheduler(core_count, scheduling_policy=policy)
            for task in tasks:
                scheduler.add_task(task)
            
            scheduler.run(5000)
            energy_results[policy].append(scheduler.metrics['energy_consumption'])
    
    # رسم نمودار انرژی
    plt.figure(figsize=(10, 6))
    for policy in policies:
        plt.plot(utilizations, energy_results[policy], 'D-', label=policy)
    
    plt.xlabel('بهره‌وری پردازنده')
    plt.ylabel('انرژی مصرفی (J)')
    plt.title('تأثیر بهره‌وری بر انرژی مصرفی')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{RESULTS_DIR}/نمودار_انرژی_بهره‌وری.png', dpi=300)
    plt.close()

if __name__ == "__main__":
    experiment_core_impact()
    experiment_energy_utilization()
    print("فاز اول با موفقیت اجرا شد. نتایج در دایرکتوری 'نتایج' ذخیره شدند.")