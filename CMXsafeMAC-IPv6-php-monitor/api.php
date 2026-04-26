<?php

declare(strict_types=1);

$allocatorBaseUrl = rtrim((string) (getenv('MAC_ALLOCATOR_BASE_URL') ?: 'http://127.0.0.1:18080'), '/');
$trafficCollectorBaseUrl = rtrim((string) (getenv('TRAFFIC_COLLECTOR_BASE_URL') ?: 'http://127.0.0.1:18082'), '/');
$dashboardBaseUrl = rtrim((string) (getenv('SSH_DASHBOARD_BASE_URL') ?: ''), '/');
$routePrefixLen = max(1, min(128, (int) (getenv('EXPLICIT_IPV6_ROUTE_PREFIX_LEN') ?: '16')));

function send_json(int $statusCode, array $payload): void
{
    http_response_code($statusCode);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
    exit;
}

function normalize_rows(mixed $payload): array
{
    if (is_array($payload)) {
        $isList = array_keys($payload) === range(0, count($payload) - 1);
        if ($isList) {
            return $payload;
        }
        if (isset($payload['value']) && is_array($payload['value'])) {
            return $payload['value'];
        }
    }

    return [];
}

function lower_string(mixed $value): string
{
    return strtolower(trim((string) $value));
}

function http_request(string $method, string $baseUrl, string $path, ?array $payload = null): array
{
    $headers = [
        'Accept: application/json',
    ];
    $content = '';

    if ($payload !== null) {
        $headers[] = 'Content-Type: application/json';
        $content = json_encode($payload, JSON_THROW_ON_ERROR);
    }

    $context = stream_context_create([
        'http' => [
            'method' => $method,
            'header' => implode("\r\n", $headers),
            'content' => $content,
            'ignore_errors' => true,
            'timeout' => 20,
        ],
    ]);

    $url = $baseUrl . $path;
    $raw = @file_get_contents($url, false, $context);
    $responseHeaders = $http_response_header ?? [];

    if ($raw === false && !$responseHeaders) {
        throw new RuntimeException("Unable to reach service at {$url}.");
    }

    $statusCode = 0;
    if ($responseHeaders && preg_match('/\s(\d{3})\s/', $responseHeaders[0], $matches) === 1) {
        $statusCode = (int) $matches[1];
    }

    if ($raw === false) {
        throw new RuntimeException("HTTP request failed for {$url}.");
    }

    $decoded = json_decode($raw, true);
    if (!is_array($decoded)) {
        $decoded = [];
    }

    return [$statusCode, $decoded];
}

function route_network_for_prefix(string $prefixHex, int $routePrefixLen): string
{
    return strtolower($prefixHex) . "::/{$routePrefixLen}";
}

function build_maps(array $allocations, array $explicitAssignments, int $routePrefixLen): array
{
    $allocationsByPodUid = [];
    $managedByIPv6 = [];
    $explicitByIPv6 = [];
    $prefixSummaries = [];

    foreach ($allocations as $allocation) {
        $podUid = (string) ($allocation['pod_uid'] ?? '');
        if ($podUid !== '') {
            $allocationsByPodUid[$podUid] = $allocation;
        }
        $managedIPv6 = lower_string($allocation['assigned_ipv6'] ?? '');
        if ($managedIPv6 !== '') {
            $managedByIPv6[$managedIPv6] = [
                'type' => 'managed',
                'address' => $managedIPv6,
                'namespace' => (string) ($allocation['namespace'] ?? ''),
                'pod_name' => (string) ($allocation['pod_name'] ?? ''),
                'pod_uid' => $podUid,
                'label' => trim(((string) ($allocation['namespace'] ?? '')) . '/' . ((string) ($allocation['pod_name'] ?? '')) . ' managed IPv6'),
            ];
        }
    }

    foreach ($explicitAssignments as $row) {
        if (upper_string($row['status'] ?? '') !== 'ACTIVE') {
            continue;
        }
        $requestedIPv6 = lower_string($row['requested_ipv6'] ?? '');
        $prefixHex = lower_string($row['gw_tag_hex'] ?? '');
        if ($requestedIPv6 === '' || $prefixHex === '') {
            continue;
        }
        $allocation = $allocationsByPodUid[(string) ($row['pod_uid'] ?? '')] ?? null;
        $namespace = (string) ($allocation['namespace'] ?? '');
        $podName = (string) ($allocation['pod_name'] ?? '');
        $podUid = (string) ($row['pod_uid'] ?? '');
        $routeNetwork = route_network_for_prefix($prefixHex, $routePrefixLen);

        $explicitByIPv6[$requestedIPv6] = [
            'type' => 'explicit',
            'address' => $requestedIPv6,
            'namespace' => $namespace,
            'pod_name' => $podName,
            'pod_uid' => $podUid,
            'prefix_hex' => $prefixHex,
            'route_network' => $routeNetwork,
            'label' => trim($namespace . '/' . $podName . ' explicit IPv6'),
            'mac_dev' => (string) ($row['mac_dev'] ?? ''),
            'target_gw_mac' => lower_string($row['target_gw_mac'] ?? ''),
        ];

        if (!isset($prefixSummaries[$prefixHex])) {
            $prefixSummaries[$prefixHex] = [
                'prefix_hex' => $prefixHex,
                'route_network' => $routeNetwork,
                'assignment_count' => 0,
                'flow_count' => 0,
                'packet_count' => 0,
            ];
        }
        $prefixSummaries[$prefixHex]['assignment_count']++;
    }

    return [
        'allocations_by_pod_uid' => $allocationsByPodUid,
        'managed_by_ipv6' => $managedByIPv6,
        'explicit_by_ipv6' => $explicitByIPv6,
        'prefix_summaries' => $prefixSummaries,
    ];
}

function upper_string(mixed $value): string
{
    return strtoupper(trim((string) $value));
}

function resolve_endpoint(string $address, array $maps): array
{
    $normalized = lower_string($address);
    if ($normalized === '') {
        return [
            'type' => 'unknown',
            'address' => '',
            'label' => '-',
        ];
    }
    if (isset($maps['explicit_by_ipv6'][$normalized])) {
        return $maps['explicit_by_ipv6'][$normalized];
    }
    if (isset($maps['managed_by_ipv6'][$normalized])) {
        return $maps['managed_by_ipv6'][$normalized];
    }
    return [
        'type' => 'unknown',
        'address' => $normalized,
        'label' => $normalized,
    ];
}

function flow_protocol(array $flow): string
{
    return lower_string($flow['protocol'] ?? 'unknown');
}

function flow_classification(array $src, array $dst): string
{
    if (($src['type'] ?? '') === 'explicit' && ($dst['type'] ?? '') === 'explicit') {
        return 'internal-explicit-v6-lan';
    }
    if (($dst['type'] ?? '') === 'explicit' && ($src['type'] ?? '') !== 'explicit') {
        return 'inbound-from-outside-explicit-v6-lan';
    }
    if (($src['type'] ?? '') === 'explicit' && ($dst['type'] ?? '') !== 'explicit') {
        return 'outbound-to-outside-explicit-v6-lan';
    }
    return 'other';
}

function prefix_list_for_flow(array $src, array $dst): array
{
    $prefixes = [];
    foreach ([$src['prefix_hex'] ?? '', $dst['prefix_hex'] ?? ''] as $prefix) {
        $normalized = lower_string($prefix);
        if ($normalized !== '' && !in_array($normalized, $prefixes, true)) {
            $prefixes[] = $normalized;
        }
    }
    return $prefixes;
}

function enrich_traffic(array $collectorPayload, array $maps, ?string $prefixFilter, int $routePrefixLen): array
{
    $collectorFlows = normalize_rows($collectorPayload['flows'] ?? []);
    $classifiedFlows = [];
    $prefixSummaries = $maps['prefix_summaries'];

    foreach ($collectorFlows as $flow) {
        $src = resolve_endpoint((string) ($flow['src_address'] ?? ''), $maps);
        $dst = resolve_endpoint((string) ($flow['dst_address'] ?? ''), $maps);

        if (($src['type'] ?? '') === 'unknown' && ($dst['type'] ?? '') === 'unknown') {
            continue;
        }

        $prefixes = prefix_list_for_flow($src, $dst);
        if ($prefixFilter !== null && $prefixFilter !== '' && !in_array($prefixFilter, $prefixes, true)) {
            continue;
        }

        $classification = flow_classification($src, $dst);
        if ($classification === 'other') {
            continue;
        }

        $primaryPrefix = $prefixes[0] ?? '';
        $routeNetwork = $primaryPrefix !== '' ? route_network_for_prefix($primaryPrefix, $routePrefixLen) : null;

        foreach ($prefixes as $prefix) {
            if (!isset($prefixSummaries[$prefix])) {
                $prefixSummaries[$prefix] = [
                    'prefix_hex' => $prefix,
                    'route_network' => route_network_for_prefix($prefix, $routePrefixLen),
                    'assignment_count' => 0,
                    'flow_count' => 0,
                    'packet_count' => 0,
                ];
            }
            $prefixSummaries[$prefix]['flow_count']++;
            $prefixSummaries[$prefix]['packet_count'] += (int) ($flow['packets'] ?? 0);
        }

        $classifiedFlows[] = [
            'classification' => $classification,
            'prefix_hex' => $primaryPrefix,
            'prefixes_involved' => $prefixes,
            'route_network' => $routeNetwork,
            'protocol' => flow_protocol($flow),
            'src_port' => isset($flow['src_port']) ? (int) $flow['src_port'] : null,
            'dst_port' => isset($flow['dst_port']) ? (int) $flow['dst_port'] : null,
            'icmp_type' => isset($flow['icmp_type']) ? (int) $flow['icmp_type'] : null,
            'packets' => (int) ($flow['packets'] ?? 0),
            'bytes' => (int) ($flow['bytes'] ?? 0),
            'first_seen' => (string) ($flow['first_seen'] ?? ''),
            'last_seen' => (string) ($flow['last_seen'] ?? ''),
            'src' => [
                'address' => (string) ($src['address'] ?? ''),
                'namespace' => (string) ($src['namespace'] ?? ''),
                'pod_name' => (string) ($src['pod_name'] ?? ''),
                'pod_uid' => (string) ($src['pod_uid'] ?? ''),
                'label' => (string) ($src['label'] ?? ''),
                'type' => (string) ($src['type'] ?? 'unknown'),
            ],
            'dst' => [
                'address' => (string) ($dst['address'] ?? ''),
                'namespace' => (string) ($dst['namespace'] ?? ''),
                'pod_name' => (string) ($dst['pod_name'] ?? ''),
                'pod_uid' => (string) ($dst['pod_uid'] ?? ''),
                'label' => (string) ($dst['label'] ?? ''),
                'type' => (string) ($dst['type'] ?? 'unknown'),
            ],
        ];
    }

    usort(
        $classifiedFlows,
        static fn(array $left, array $right): int => strcmp((string) ($right['last_seen'] ?? ''), (string) ($left['last_seen'] ?? ''))
    );

    usort(
        $prefixSummaries,
        static fn(array $left, array $right): int => strcmp((string) ($left['prefix_hex'] ?? ''), (string) ($right['prefix_hex'] ?? ''))
    );

    return [
        'collector_base_url' => $collectorPayload['collector_base_url'] ?? null,
        'collector_capture_interface' => $collectorPayload['capture_interface'] ?? null,
        'capture_active' => (bool) ($collectorPayload['capture_active'] ?? false),
        'capture_filter' => $collectorPayload['capture_filter'] ?? 'ip6',
        'generated_at' => $collectorPayload['generated_at'] ?? gmdate('Y-m-d\TH:i:s\Z'),
        'window_seconds' => (int) ($collectorPayload['window_seconds'] ?? 60),
        'route_prefix_len' => $routePrefixLen,
        'errors' => normalize_rows($collectorPayload['errors'] ?? []),
        'prefixes' => array_values($prefixSummaries),
        'flows' => $classifiedFlows,
    ];
}

function load_collector_snapshot(string $collectorBaseUrl, int $windowSeconds): array
{
    try {
        [$statusCode, $payload] = http_request(
            'GET',
            $collectorBaseUrl,
            '/flows?' . http_build_query([
                'window_seconds' => max(10, min(600, $windowSeconds)),
                'limit' => 500,
            ])
        );
        if ($statusCode < 200 || $statusCode >= 300) {
            return [
                'generated_at' => gmdate('Y-m-d\TH:i:s\Z'),
                'capture_interface' => null,
                'capture_active' => false,
                'capture_filter' => 'ip6',
                'window_seconds' => $windowSeconds,
                'errors' => [
                    [
                        'message' => (string) ($payload['error'] ?? 'Unable to load traffic collector flows.'),
                        'timestamp' => gmdate('Y-m-d\TH:i:s\Z'),
                    ],
                ],
                'flows' => [],
            ];
        }
        $payload['collector_base_url'] = $collectorBaseUrl;
        return $payload;
    } catch (Throwable $exception) {
        return [
            'generated_at' => gmdate('Y-m-d\TH:i:s\Z'),
            'capture_interface' => null,
            'capture_active' => false,
            'capture_filter' => 'ip6',
            'window_seconds' => $windowSeconds,
            'errors' => [
                [
                    'message' => $exception->getMessage(),
                    'timestamp' => gmdate('Y-m-d\TH:i:s\Z'),
                ],
            ],
            'flows' => [],
        ];
    }
}

function load_security_topology(string $dashboardBaseUrl): array
{
    if ($dashboardBaseUrl === '') {
        return [
            'configured' => false,
            'targets' => [],
            'errors' => [
                [
                    'message' => 'SSH_DASHBOARD_BASE_URL is not configured, so registered users/services/access grants are unavailable.',
                    'timestamp' => gmdate('Y-m-d\TH:i:s\Z'),
                ],
            ],
        ];
    }

    try {
        [$statusCode, $payload] = http_request('GET', $dashboardBaseUrl, '/api/topology');
        if ($statusCode < 200 || $statusCode >= 300) {
            return [
                'configured' => true,
                'dashboard_base_url' => $dashboardBaseUrl,
                'targets' => [],
                'errors' => [
                    [
                        'message' => (string) ($payload['error'] ?? 'Unable to load dashboard topology.'),
                        'timestamp' => gmdate('Y-m-d\TH:i:s\Z'),
                    ],
                ],
            ];
        }
        $payload['configured'] = true;
        $payload['dashboard_base_url'] = $dashboardBaseUrl;
        $payload['errors'] = normalize_rows($payload['errors'] ?? []);
        return $payload;
    } catch (Throwable $exception) {
        return [
            'configured' => true,
            'dashboard_base_url' => $dashboardBaseUrl,
            'targets' => [],
            'errors' => [
                [
                    'message' => $exception->getMessage(),
                    'timestamp' => gmdate('Y-m-d\TH:i:s\Z'),
                ],
            ],
        ];
    }
}

function addresses_match(mixed $left, mixed $right): bool
{
    return lower_string($left) !== '' && lower_string($left) === lower_string($right);
}

function flow_matches_request(array $flow, array $path): bool
{
    $consumerIPv6 = lower_string($path['consumer']['canonical_ipv6'] ?? '');
    $serviceIPv6 = lower_string($path['service']['canonical_ipv6'] ?? '');
    $servicePort = (int) ($path['service']['port'] ?? 0);
    return addresses_match($flow['src']['address'] ?? '', $consumerIPv6)
        && addresses_match($flow['dst']['address'] ?? '', $serviceIPv6)
        && (int) ($flow['dst_port'] ?? 0) === $servicePort;
}

function flow_matches_response(array $flow, array $path): bool
{
    $consumerIPv6 = lower_string($path['consumer']['canonical_ipv6'] ?? '');
    $serviceIPv6 = lower_string($path['service']['canonical_ipv6'] ?? '');
    $servicePort = (int) ($path['service']['port'] ?? 0);
    return addresses_match($flow['src']['address'] ?? '', $serviceIPv6)
        && addresses_match($flow['dst']['address'] ?? '', $consumerIPv6)
        && (int) ($flow['src_port'] ?? 0) === $servicePort;
}

function observed_flow_for_path(array $flow, string $direction): array
{
    return [
        'direction' => $direction,
        'classification' => (string) ($flow['classification'] ?? ''),
        'protocol' => (string) ($flow['protocol'] ?? ''),
        'src_address' => (string) ($flow['src']['address'] ?? ''),
        'src_port' => isset($flow['src_port']) ? (int) $flow['src_port'] : null,
        'src_label' => (string) ($flow['src']['label'] ?? ''),
        'dst_address' => (string) ($flow['dst']['address'] ?? ''),
        'dst_port' => isset($flow['dst_port']) ? (int) $flow['dst_port'] : null,
        'dst_label' => (string) ($flow['dst']['label'] ?? ''),
        'packets' => (int) ($flow['packets'] ?? 0),
        'bytes' => (int) ($flow['bytes'] ?? 0),
        'last_seen' => (string) ($flow['last_seen'] ?? ''),
    ];
}

function attach_live_observations(array $topology, array $traffic): array
{
    $flows = normalize_rows($traffic['flows'] ?? []);
    $summary = [
        'identity_count' => 0,
        'published_service_count' => 0,
        'registered_path_count' => 0,
        'enabled_path_count' => 0,
        'live_path_count' => 0,
    ];

    foreach ($topology['targets'] ?? [] as $targetIndex => $target) {
        $summary['identity_count'] += count(normalize_rows($target['identities'] ?? []));
        $summary['published_service_count'] += count(normalize_rows($target['published_services'] ?? []));
        $paths = normalize_rows($target['registered_paths'] ?? []);
        foreach ($paths as $pathIndex => $path) {
            $summary['registered_path_count']++;
            if (!empty($path['enabled'])) {
                $summary['enabled_path_count']++;
            }
            $observed = [];
            $packets = 0;
            $bytes = 0;
            $lastSeen = '';
            foreach ($flows as $flow) {
                $direction = null;
                if (flow_matches_request($flow, $path)) {
                    $direction = 'request';
                } elseif (flow_matches_response($flow, $path)) {
                    $direction = 'response';
                }
                if ($direction === null) {
                    continue;
                }
                $observedRow = observed_flow_for_path($flow, $direction);
                $observed[] = $observedRow;
                $packets += $observedRow['packets'];
                $bytes += $observedRow['bytes'];
                if ($observedRow['last_seen'] > $lastSeen) {
                    $lastSeen = $observedRow['last_seen'];
                }
            }
            $path['observed_flows'] = $observed;
            $path['live'] = count($observed) > 0;
            $path['observed_packet_count'] = $packets;
            $path['observed_byte_count'] = $bytes;
            $path['last_seen'] = $lastSeen;
            if ($path['live']) {
                $summary['live_path_count']++;
            }
            $topology['targets'][$targetIndex]['registered_paths'][$pathIndex] = $path;
        }
    }

    $topology['summary'] = $summary;
    return $topology;
}

function request_snapshot(
    string $allocatorBaseUrl,
    string $trafficCollectorBaseUrl,
    string $dashboardBaseUrl,
    ?string $prefixFilter,
    int $windowSeconds,
    int $routePrefixLen
): array {
    [$allocStatus, $allocations] = http_request('GET', $allocatorBaseUrl, '/allocations');
    [$explicitStatus, $explicitAssignments] = http_request('GET', $allocatorBaseUrl, '/explicit-ipv6-assignments');

    if ($allocStatus < 200 || $allocStatus >= 300) {
        throw new RuntimeException((string) ($allocations['error'] ?? 'Unable to load allocations.'));
    }
    if ($explicitStatus < 200 || $explicitStatus >= 300) {
        throw new RuntimeException((string) ($explicitAssignments['error'] ?? 'Unable to load explicit IPv6 assignments.'));
    }

    $allocationRows = normalize_rows($allocations);
    $explicitRows = normalize_rows($explicitAssignments);
    $maps = build_maps($allocationRows, $explicitRows, $routePrefixLen);
    $collectorPayload = load_collector_snapshot($trafficCollectorBaseUrl, $windowSeconds);
    $traffic = enrich_traffic($collectorPayload, $maps, $prefixFilter, $routePrefixLen);
    $securityTopology = attach_live_observations(load_security_topology($dashboardBaseUrl), $traffic);

    return [
        'generated_at' => gmdate('Y-m-d\TH:i:s\Z'),
        'allocator_base_url' => $allocatorBaseUrl,
        'traffic_collector_base_url' => $trafficCollectorBaseUrl,
        'dashboard_base_url' => $dashboardBaseUrl,
        'allocations' => $allocationRows,
        'explicit_assignments' => $explicitRows,
        'traffic' => $traffic,
        'security_topology' => $securityTopology,
    ];
}

try {
    if ($_SERVER['REQUEST_METHOD'] === 'POST') {
        $action = trim((string) ($_POST['action'] ?? ''));
        if ($action === 'clear_stale') {
            [$statusCode, $payload] = http_request('POST', $allocatorBaseUrl, '/allocations/clear-stale', []);
            send_json($statusCode > 0 ? $statusCode : 500, $payload);
        }

        send_json(400, ['error' => 'Unsupported POST action.']);
    }

    $prefixFilter = lower_string($_GET['prefix'] ?? '');
    $windowSeconds = (int) ($_GET['window_seconds'] ?? 60);
    send_json(
        200,
        request_snapshot(
            $allocatorBaseUrl,
            $trafficCollectorBaseUrl,
            $dashboardBaseUrl,
            $prefixFilter !== '' ? $prefixFilter : null,
            $windowSeconds,
            $routePrefixLen
        )
    );
} catch (Throwable $exception) {
    send_json(500, [
        'error' => $exception->getMessage(),
        'allocator_base_url' => $allocatorBaseUrl,
        'traffic_collector_base_url' => $trafficCollectorBaseUrl,
        'dashboard_base_url' => $dashboardBaseUrl,
    ]);
}
