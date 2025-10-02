#!/usr/bin/env bash
dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# $1 is apply/a or delete/d

if [ "$1" == "delete" ] || [ "$1" == "d" ]; then    
    echo "Running: kubectl delete -f $dir/../deployments/operator"
    kubectl delete -f "$dir/../deployments/operator"

elif [ "$1" == "apply" ] || [ "$1" == "a" ]; then
    echo "Running: kubectl apply -f $dir/../deployments/operator"
    kubectl apply -f "$dir/../deployments/operator"

elif [ "$1" == "create" ] || [ "$1" == "c" ]; then
    echo "Running: kubectl create -f $dir/../deployments/operator"
    kubectl create -f "$dir/../deployments/operator"

else
    echo "Usage: $0 [apply|a|delete|d]"
    exit 1
fi