#!/usr/bin/env bash
set -euo pipefail
NS="${K8S_NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
echo "Adding shard3 to PerconaServerMongoDB mongo-cluster without downtime..."
kubectl patch psmdb mongo-cluster -n "$NS" --type=json -p='[
  {"op":"add","path":"/spec/replsets/-","value":{
    "name":"shard3",
    "size":3,
    "labels":{"role":"store"},
    "resources":{"requests":{"memory":"512Mi"},"limits":{"memory":"1Gi"}},
    "configuration":"storage:\n  wiredTiger:\n    engineConfig:\n      cacheSizeGB: 0.25\n",
    "topologySpreadConstraints":[{
      "maxSkew":1,
      "topologyKey":"kubernetes.io/hostname",
      "whenUnsatisfiable":"DoNotSchedule",
      "labelSelector":{"matchLabels":{"app.kubernetes.io/replset":"shard3"}}
    }],
    "expose":{"enabled":false,"type":"ClusterIP"},
    "volumeSpec":{"persistentVolumeClaim":{"storageClassName":"local-path","resources":{"requests":{"storage":"1Gi"}}}}
  }}
]'
