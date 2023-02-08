# PaddleFlow 监控面板
## 所需环境
### helm-chart
* kube-prometheus-stack >= 0.54.1

## 使用方法
从grafana面板导入json即可
## 面板列表
### PaddleFlow Server exporter
* Queue (Job) - gra_queue_job.json
* Node (Job)  - gra_node_job.json
* Job 流程耗时  - gra_job_timepoints.json

### GPU exporter (node_exporter with NVML)
* Node (GPU)  - gra_node_gpu.json

### Scheduler exporter (volcano scheduler)
* Volcano overview - gra_volcano_overview.json