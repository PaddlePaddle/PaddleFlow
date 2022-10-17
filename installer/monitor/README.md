# PaddleFlow 监控面板
## 所需环境
### helm-chart
* kube-prometheus-stack >= 0.54.1

## 使用方法
从grafana面板导入json即可
## 面板列表
### PaddleFlow Server exporter
* Queue (Job) - queue_job.json
* Node (Job)  - node_job.json
* Job 流程耗时  - job_timepoints.json

### GPU exporter (node_exporter with NVML)
* Node (GPU)  - node_gpu.json