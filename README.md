# K8, InfluxDB and Flink Experiment for Downsampling

Downsampling is necessary to reduce the storage burden and search optimization for InfluxDB queries.

This is GO project that handles deployment of a controller that deploys Flink jobs for each downsampling request.

## Deployment

```
metrics-downsampling-deployment-controller go/compile       compile go programs
metrics-downsampling-deployment-controller docker/tags      list the existing tagged images
metrics-downsampling-deployment-controller docker/build     build and tag the Docker image. vars:tag
metrics-downsampling-deployment-controller docker/push      push the Docker image to ECR. vars:tag
metrics-downsampling-deployment-controller helm/install     Deploy stack into kubernetes. vars: stack
metrics-downsampling-deployment-controller helm/delete      delete stack from reference. vars:stack
metrics-downsampling-deployment-controller helm/reinstall   delete stack from reference and then deploy. vars:stack
metrics-downsampling-deployment-controller deploy           Compiles, builds and deploys a stack for a tag. vars: tag, stack
metrics-downsampling-deployment-controller redeploy         Compiles, builds and re-deploys a stack for a tag. vars: tag, stack
metrics-downsampling-deployment-controller help             this helps
```
