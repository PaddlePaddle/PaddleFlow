#!/bin/bash
hn=`hostname`
ns="paddleflow"
pods=`kubectl get pods -n paddleflow | awk '{print $1}'`
# mountpod
mountpod="^pfs-"$hn
for pod in ${pods}
do 
    if [[ "$pod" =~ $mountpod ]]; then
       echo "mountpod: $pod is going to be deleted"
       kubectl delete pod $pod -n $ns
    fi
done
#delete pvc
pvcs=`kubectl get pvc -A | awk '{print $2}'`
targetpvc="^pfs-.+-pvc$"
for pvc in ${pvcs}
do
    if [[ "$pvc" =~ $targetpvc ]];then
        echo "pvc: $pvc is going to be deleted" 
        kubectl delete pvc $pvc
        kubectl patch pvc $pvc -p '{"metadata":{"finalizers":null}}'
    fi

done
targetpv="^pfs-.+-pv$"
#delete pv
pvs=`kubectl get pv  | awk '{print $1}'`
for pv in ${pvs}
do
    if [[ "$pv" =~ $targetpv ]];then
        echo "pv: $pv is going to be deleted"
        kubectl delete pv $pv
        kubectl patch pv $pv -p '{"metadata":{"finalizers":null}}'
    fi
done
