from termcolor import colored
import sys
from kubernetes import client, config
from observability_handlers import *

class ObservabilityGatewayEnvironment:
    def __init__(self):
        self.environment_name = "Observability Gateway"
        self.current_class_weights = {"gold": 0.5, "silver": 0.3, "bronze": 0.2}
        self.importance_parameters = {"staleness": 0.5, "batch_loss": 0.3, "backlog": 0.2}
        self.control_context = {
            "total_ingestion_rate": 1000,
            "queue_capacity": 100
        }
        self.metrics_proxy_url = "127.0.0.1:8000"
        self.metrics = None

    def apply_rate_limits(self, request_rates: dict) -> dict:
        """
        Update TrafficPolicy CRs in namespace `observability-ingress` for each class
        based on provided weights.

        request_rates: dict mapping class-name (e.g. 'gold') -> weight (float). Weights
        must sum to 1.0 (floating tolerance accepted).

        Assumption: `self.control_context['total_ingestion_rate']` is the total allowed
        ingestion rate for the CR's fill interval (the CR uses `fillInterval: "1m"`).
        The function calculates per-class tokens as `weight * total_ingestion_rate`
        and sets both `maxTokens` and `tokensPerFill` to that value.

        Returns a dict of results keyed by class with patch status or error.
        """
        # validate input
        if not isinstance(request_rates, dict) or len(request_rates) == 0:
            raise ValueError("request_rates must be a non-empty dict of {class: weight}")

        total = float(sum(float(v) for v in request_rates.values()))
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"weights must sum to 1.0 (got {total})")

        # treat ingestion_rate as total tokens-per-fill interval
        tokens_per_fill_total = int(max(0.0, float(self.control_context.get("total_ingestion_rate", 0))))

        # persist the validated weights on the environment for later use
        try:
            self.current_class_weights = {str(k): float(v) for k, v in request_rates.items()}
            # green success message
            print(colored(f"Updated class weights: {self.current_class_weights}", "green"))
        except Exception:
            # shouldn't happen because we validated above, but be defensive
            # red error message
            print(colored("failed to persist class weights", "red"), file=sys.stderr)

        # load cluster config
        try:
            config.load_incluster_config()
        except Exception:
            try:
                config.load_kube_config()
            except Exception as e:
                raise RuntimeError(f"could not load kube config: {e}")

        api = client.CustomObjectsApi()

        group = "gateway.kgateway.dev"
        version = "v1alpha1"
        plural = "trafficpolicies"
        namespace = "observability-ingress"

        for class_name, weight in request_rates.items():
            try:
                w = float(weight)
            except Exception:
                print(colored(f"invalid weight for class '{class_name}'", "yellow"), file=sys.stderr)
                continue

            tokens = int(round(tokens_per_fill_total * w))

            patch_body = {
                "spec": {
                    "rateLimit": {
                        "local": {
                            "tokenBucket": {
                                "maxTokens": tokens,
                                "tokensPerFill": tokens,
                                "fillInterval": "1m"
                            }
                        }
                    }
                }
            }

            # patch the TrafficPolicy CR
            try:
                api.patch_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=plural,
                    name=class_name,
                    body=patch_body
                )
                print(colored(f"Patched TrafficPolicy '{class_name}' with {tokens} tokens", "green"))
            except Exception as e:
                print(colored(f"failed to patch TrafficPolicy '{class_name}': {e}", "red"), file=sys.stderr)
    
    def post_weights(self, weights: dict) -> dict:
        return post_weights(weights)
        
    def get_observations(self, window_minutes: int = 5) -> dict:
        metrics = get_observations(window_minutes, metrics_proxy_url=self.metrics_proxy_url)
        restructured_metrics = restructure_observations(metrics)
        self.metrics = restructured_metrics
        # compute aggregated class-level metrics (averages + tail/max pain)
        aggregated = self.aggregate_class_metrics(restructured_metrics)
        return aggregated
    
    def aggregate_class_metrics(self, observations: dict) -> dict:
        """
        Compute per-class aggregated metrics from restructured observations.

        observations: {class: {source: {metric: value}}}

        Returns: {class: {avg_queue_ratio, avg_drop_ratio, avg_staleness_ratio,
                          avg_starvation_flag, max_queue_ratio, max_drop_ratio,
                          max_staleness_ratio}}
        """
        # fetch extra class-level metrics (demand_rate, rejection_rate) and keep them raw
        try:
            extra_raw = get_observations(metrics_proxy_url=self.metrics_proxy_url, queries=['demand_rate', 'rejection_rate'])
        except Exception:
            extra_raw = {}

        # build metric -> class -> value maps from the raw response
        metric_class_maps = {}
        for metric_name, label_map in (extra_raw or {}).items():
            metric_class_maps[metric_name] = {}
            if not isinstance(label_map, dict):
                continue
            for label, val in label_map.items():
                # label like 'class=gold,...,pod=...'
                cls = None
                for part in label.split(','):
                    if part.startswith('class='):
                        cls = part.split('=', 1)[1]
                        break
                if cls is not None:
                    try:
                        metric_class_maps[metric_name][cls] = float(val)
                    except Exception:
                        metric_class_maps[metric_name][cls] = val

        result = {}
        for cls, pods in (observations or {}).items():
            queue_sum = 0.0
            drop_sum = 0.0
            stale_sum = 0.0
            starvation_sum = 0.0
            q_max = 0.0
            d_max = 0.0
            s_max = 0.0

            count = 0
            # observations now structured as class -> pod -> source -> metrics
            for pod, sources in (pods or {}).items():
                for src, metrics in (sources or {}).items():
                    qr = float(metrics.get('queue_ratio', 0.0)) if metrics else 0.0
                    dr = float(metrics.get('drop_ratio', 0.0)) if metrics else 0.0
                    sr = float(metrics.get('staleness_ratio', 0.0)) if metrics else 0.0
                    sf = float(metrics.get('starvation_flag', 0)) if metrics else 0.0

                    queue_sum += qr
                    drop_sum += dr
                    stale_sum += sr
                    starvation_sum += sf

                    if qr > q_max:
                        q_max = qr
                    if dr > d_max:
                        d_max = dr
                    if sr > s_max:
                        s_max = sr

                    count += 1

            if count == 0:
                avg_queue = 0.0
                avg_drop = 0.0
                avg_stale = 0.0
                avg_starvation = 0.0
            else:
                avg_queue = queue_sum / count
                avg_drop = drop_sum / count
                avg_stale = stale_sum / count
                avg_starvation = starvation_sum / count

            result[cls] = {
                'avg_queue_ratio': avg_queue,
                'avg_drop_ratio': avg_drop,
                'avg_staleness_ratio': avg_stale,
                'avg_starvation_flag': avg_starvation,
                'max_queue_ratio': q_max,
                'max_drop_ratio': d_max,
                'max_staleness_ratio': s_max,
            }

        # merge the metric->class maps into each class's result
        for metric_name, class_map in metric_class_maps.items():
            for cls_name, val in (class_map or {}).items():
                if cls_name not in result:
                    result[cls_name] = {}
                try:
                    result[cls_name][metric_name] = float(val)
                except Exception:
                    result[cls_name][metric_name] = val

        return result

    def tenant_score_function(self) -> dict:
        """
        Compute tenant scores from `self.metrics` using `self.importance_parameters`.
        Validates that `self.importance_parameters` sums to 1.0 and returns
        softmax-normalized scores per class: {class: {source: normalized_score}}
        """
        # validate importance parameters sum to 1.0
        params = self.importance_parameters or {}
        try:
            total = float(sum(float(v) for v in params.values()))
        except Exception:
            raise ValueError("importance_parameters must be a mapping of numeric values")
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"importance_parameters must sum to 1.0 (got {total})")

        observations = self.metrics or {}

        # compute raw scores (higher -> more urgent/problematic) per class -> pod -> source
        raw_scores = {}
        for cls, pods in (observations or {}).items():
            raw_scores[cls] = {}
            for pod, sources in (pods or {}).items():
                raw_scores[cls][pod] = {}
                for src, metrics in (sources or {}).items():
                    m = metrics or {}
                    staleness = float(m.get('staleness_ratio', 0.0))
                    drop_ratio = float(m.get('drop_ratio', 0.0))
                    queue_utilization = float(m.get('queue_ratio', 0.0))

                    score = (
                        float(params.get('staleness', 0.0)) * staleness
                        + float(params.get('batch_loss', 0.0)) * drop_ratio
                        + float(params.get('backlog', 0.0)) * queue_utilization
                    )
                    raw_scores[cls][pod][src] = score

        # apply softmax normalization per (class, pod) so scores are relative within each pod
        import math

        normalized = {}
        for cls, pods in raw_scores.items():
            normalized[cls] = {}
            for pod, src_map in (pods or {}).items():
                normalized[cls][pod] = {}
                if not src_map:
                    continue
                values = list(src_map.values())
                max_v = max(values)
                exps = [math.exp(v - max_v) for v in values]
                s = sum(exps) if sum(exps) != 0 else 1.0
                for src, e in zip(src_map.keys(), exps):
                    normalized[cls][pod][src] = e / s

        return normalized
    

if __name__ == "__main__":
    # env = ObservabilityGatewayEnvironment()
    # observations = env.get_observations(window_minutes=5)
    # print("Aggregated Observations:")
    # print(observations)
    print(get_pods_for_gateway("observabilitygateway", "observability"))