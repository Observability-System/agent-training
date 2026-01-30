import requests
import sys
from termcolor import colored
from kubernetes import client, config


class Clients:
    """Centralized endpoints/config for metric and agent HTTP endpoints.

        Defaults are provided via constructor arguments. Typical defaults:
            - metrics_proxy: "127.0.0.1:8000"
            - pod_host: "localhost"
            - pod_port: 4500
    """
    def __init__(self, metrics_proxy: str = "127.0.0.1:8000", pod_host: str = "localhost", pod_port: int = 4500):
        self.metrics_proxy = metrics_proxy
        self.pod_host = pod_host
        try:
            self.pod_port = int(pod_port)
        except Exception:
            self.pod_port = 4500

    def observations_url(self) -> str:
        return f"http://{self.metrics_proxy}/observations"
    
    def pod_url(self, pod_ip: str, path: str, port: int = None) -> str:
        """Build an absolute URL to reach a pod by IP.

        Args:
            pod_ip: pod IP or hostname
            path: request path, e.g. '/update_weights' or 'slo/update'
            port: optional port override (defaults to pod_port)
        """
        if not pod_ip:
            raise ValueError("pod_ip is required")
        p = int(port) if port is not None else self.pod_port
        if not path.startswith('/'):
            path = '/' + path
        return f"http://{pod_ip}:{p}{path}"


# module-level default client
DEFAULT_CLIENT = Clients()

def get_pod_ips_by_class(cr_name: str, namespace: str) -> dict:
    """
    Return pod IP addresses grouped by class for deployments owned by a custom resource.

    This function inspects the Kubernetes API to find Deployments that are owned
    by the specified custom resource (an ObservabilityGateway). It then finds
    pods for each deployment (via the `app` label selector) and returns a
    mapping of class -> { pod_name: pod_ip }.

    Parameters
    - cr_name (str): Name of the ObservabilityGateway custom resource.
    - namespace (str): Kubernetes namespace where the CR and deployments live.

    Returns
    - dict: Mapping from class name (str) to a dict mapping pod name (str) to
        pod IP address (str or None). Example:
            {
                "silver": {"src-abc-123": "10.0.0.5", "src-def-456": "10.0.0.6"},
                "gold": {"src-xyz-789": "10.0.0.7"}
            }

    Notes
    - Ownership of deployments is determined by comparing the deployment's
        ownerReferences UID to the custom resource UID.
    - The class for a deployment is determined (in order) from two sources:
        1. the `observability-class` label, if present;
        2. otherwise derived from the deployment name (strip the CR name
           prefix and take the first dash-separated token).
    """

    # Load the local kubeconfig so the client can talk to the cluster
    config.load_kube_config("~/.kube/config")

    custom = client.CustomObjectsApi()
    apps = client.AppsV1Api()
    core = client.CoreV1Api()

    # Retrieve the custom resource to identify which deployments belong to it
    cr = custom.get_namespaced_custom_object(
        group="observability.x-k8s.io",
        version="v1alpha1",
        namespace=namespace,
        plural="observabilitygateways",
        name=cr_name,
    )
    cr_uid = cr["metadata"]["uid"]

    # Collect all deployments owned by this CR (ownerReferences ensures the link)
    deps = [
        d for d in apps.list_namespaced_deployment(namespace).items
        if any(o.uid == cr_uid for o in (d.metadata.owner_references or []))
    ]

    # Build a simple mapping: deployment's app label → class name 
    # The app label uniquely identifies each deployment's pods
    dep_class_by_app = {}
    for d in deps:
        app_value = d.spec.selector.match_labels["app"]
        dep_class = (
            d.metadata.labels.get("observability-class")
            or d.metadata.name.replace(f"{cr_name}-", "").split("-")[0]
        )

        dep_class_by_app[app_value] = dep_class

    # Gather pods for each deployment and group them by class
    result = {}
    for d in deps:
        app_value = d.spec.selector.match_labels["app"]
        label_selector = f"app={app_value}"

        # Kubernetes handles the pod filtering for us via the label selector
        pods = core.list_namespaced_pod(namespace, label_selector=label_selector).items

        # Store pod name → pod IP under the appropriate class
        cls = dep_class_by_app[app_value]
        for p in pods:
            result.setdefault(cls, {})[p.metadata.name] = p.status.pod_ip

    return result

def fetch_observations(window_minutes: int = 5, client: Clients = None, queries=None) -> dict:
    """
    POST the given queries and window_minutes to the observations endpoint.
    Args:
        window_minutes: lookback window in minutes.
        client: optional Clients instance (uses DEFAULT_CLIENT when omitted).
        queries: optional list of metric query names to request. If None, a default set is used.

    Returns the response as a dict, or error info if request fails.
    """
    client = client or DEFAULT_CLIENT
    if queries is None:
        queries = ['forwarded_batches', 'dropped_batches', 'fresh_good_batches', 'queue_length']

    url = client.observations_url()
    headers = {"Content-Type": "application/json"}
    data = {"queries": queries, "window_minutes": window_minutes}
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
        return response.json() if response.content else {"status": "success", "code": response.status_code}
    except Exception as e:
        print(colored(f"POST to {url} failed: {e}", "red"), file=sys.stderr)
        return {"error": str(e)}


def transform_observations(result: dict, class_total_capacity=None, source_count_per_class=None, drop_slo=None) -> dict:
    """
    Transform a flat result dict (with keys like 'class=silver,source=src1')
    into a nested dict: {class: {pod: {source: {metric: value}}}}

    if `class_total_capacity` is provided as a dict or scalar it will be used to compute
     per-source capacity as `class_total_capacity[class] / num_sources`. 
    `source_count_per_class` may be provided as a dict of counts per class.
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

                # Compute drop_slo for this (cls, pod, src) if provided
                # drop_slo should be a dict: class -> list of {source, budget}
                drop_slo_val = None
                if drop_slo and cls in drop_slo:
                    # Find the budget for this source in the list
                    for entry in drop_slo[cls]:
                        # entry: {"source": ..., "budget": ...}
                        if str(entry.get('source')) == str(src):
                            try:
                                drop_slo_val = float(entry.get('budget'))
                            except Exception:
                                drop_slo_val = None
                            break
                if drop_slo_val is not None:
                    metrics['drop_excess'] = max(0.0, drop_ratio - drop_slo_val)
                else:
                    metrics['drop_excess'] = drop_ratio

                # 2) 1 - fresh_good_batches / forwarded_batches
                staleness_ratio = 1.0 - (fresh_good_batches / forwarded_batches) if forwarded_batches else 1.0
                metrics['staleness_ratio'] = staleness_ratio

                # 3) simple division: per-source capacity = class_total_capacity / num_sources
                num_sources = source_count_per_class[cls]
                per_source_capacity = class_total_capacity / num_sources
                queue_ratio = queue_length / per_source_capacity
                metrics['queue_ratio'] = queue_ratio

    # convert defaultdicts to normal dicts
    return {cls: {pod: {src: dict(metrics) for src, metrics in sources.items()} for pod, sources in pods.items()} for cls, pods in output.items()}