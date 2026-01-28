import requests
import sys
from termcolor import colored
from kubernetes import client, config

def get_pods_for_gateway(cr_name, namespace):
    config.load_kube_config("~/.kube/config")

    custom = client.CustomObjectsApi()
    apps = client.AppsV1Api()
    core = client.CoreV1Api()

    # 1. Get the CR
    cr = custom.get_namespaced_custom_object(
        group="observability.x-k8s.io",
        version="v1alpha1",
        namespace=namespace,
        plural="observabilitygateways",
        name=cr_name
    )
    cr_uid = cr["metadata"]["uid"]

    # 2. Find Deployments owned by the CR
    deployments = apps.list_namespaced_deployment(namespace).items
    owned_deployments = [
        d for d in deployments
        if any(o.uid == cr_uid for o in (d.metadata.owner_references or []))
    ]

    pods = []

    for deploy in owned_deployments:
        # 3. Find ReplicaSets owned by the Deployment
        rs_list = apps.list_namespaced_replica_set(namespace).items
        owned_rs = [
            rs for rs in rs_list
            if any(o.uid == deploy.metadata.uid for o in (rs.metadata.owner_references or []))
        ]

        # 4. Find Pods owned by each ReplicaSet
        for rs in owned_rs:
            pod_list = core.list_namespaced_pod(namespace).items
            owned_pods = [
                p for p in pod_list
                if any(o.uid == rs.metadata.uid for o in (p.metadata.owner_references or []))
            ]
            pods.extend(owned_pods)

    return pods

def post_weights(weights: dict) -> dict:
    """
    POST the given weights to http://localhost:4500/update_weights as JSON.
    weights: dict mapping source names to float values.
    Returns the response as a dict, or error info if request fails.
    """
    url = "http://localhost:4500/update_weights"
    headers = {"Content-Type": "application/json"}
    data = {"weights": weights}
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
        return response.json() if response.content else {"status": "success", "code": response.status_code}
    except Exception as e:
        print(colored(f"POST to {url} failed: {e}", "red"), file=sys.stderr)
        return {"error": str(e)}


def get_observations(window_minutes: int = 5, metrics_proxy_url = "127.0.0.1", queries=None) -> dict:
    """
    POST the given queries and window_minutes to the observations endpoint (or Prometheus if implemented).
    `queries`: optional list of metric query names to request. If None, a default set is used.
    `metrics_proxy_url`: Optional base URL for Prometheus or metrics API.
    Returns the response as a dict, or error info if request fails.
    """
    if queries is None:
        queries = ['forwarded_batches', 'dropped_batches', 'fresh_good_batches', 'queue_length']

    url = f"http://{metrics_proxy_url}/observations"
    headers = {"Content-Type": "application/json"}
    data = {"queries": queries, "window_minutes": window_minutes}
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
        return response.json() if response.content else {"status": "success", "code": response.status_code}
    except Exception as e:
        print(colored(f"POST to {url} failed: {e}", "red"), file=sys.stderr)
        return {"error": str(e)}


def restructure_observations(result: dict) -> dict:
    """
    Transform a flat result dict (with keys like 'class=silver,source=src1')
    into a nested dict: {class: {pod: {source: {metric: value}}}}
    """
    from collections import defaultdict
    def parse_key(key):
        parts = key.split(',')
        d = {}
        for part in parts:
            if '=' in part:
                k, v = part.split('=', 1)
                d[k.strip()] = v.strip()
        return d.get('class'), d.get('pod'), d.get('source')

    # structure: class -> pod -> source -> {metric: value}
    output = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    # Collect all metrics per class/pod/source
    for metric, value in (result or {}).items():
        if isinstance(value, dict):
            for k, v in value.items():
                cls, pod, src = parse_key(k)
                if cls is not None and pod is not None and src is not None:
                    output[cls][pod][src][metric] = v
        else:
            output['global']['global']['global'][metric] = value

    # Calculate derived metrics per source
    for cls, pods in output.items():
        for pod, sources in pods.items():
            for src, metrics in sources.items():
                queue_length = metrics.get('queue_length', 0)
                forwarded_batches = metrics.get('forwarded_batches', 0)
                dropped_batches = metrics.get('dropped_batches', 0)
                fresh_good_batches = metrics.get('fresh_good_batches', 0)

                starvation_flag = int((queue_length > 0) and (forwarded_batches < 1))
                metrics['starvation_flag'] = starvation_flag

                # 1) dropped_batches / (forwarded_batches + dropped_batches)
                total_batches = forwarded_batches + dropped_batches
                drop_ratio = dropped_batches / total_batches if total_batches else 0.0
                metrics['drop_ratio'] = drop_ratio

                # 2) 1 - fresh_good_batches / forwarded_batches
                staleness_ratio = 1.0 - (fresh_good_batches / forwarded_batches) if forwarded_batches else 1.0
                metrics['staleness_ratio'] = staleness_ratio

                # 3) queue_length / 25
                queue_ratio = queue_length / 25.0
                metrics['queue_ratio'] = queue_ratio

    # convert defaultdicts to normal dicts
    return {cls: {pod: {src: dict(metrics) for src, metrics in sources.items()} for pod, sources in pods.items()} for cls, pods in output.items()}
