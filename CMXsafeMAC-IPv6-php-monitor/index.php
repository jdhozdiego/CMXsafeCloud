<?php

declare(strict_types=1);
?>
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>CMXsafeMAC-IPv6 Monitor</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root {
      --bg: #f3efe6;
      --panel: #fffdf8;
      --panel-border: #d8d2c4;
      --ink: #1f2937;
      --muted: #6b7280;
      --accent: #0f766e;
      --accent-soft: #d7f3ef;
      --warning: #b45309;
      --warning-soft: #ffedd5;
      --danger: #b91c1c;
      --danger-soft: #fee2e2;
      --lane: #0f766e;
      --external-in: #c2410c;
      --external-out: #7c3aed;
      --secure: #2563eb;
      --secure-soft: #dbeafe;
      --service: #166534;
      --service-soft: #dcfce7;
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", Arial, sans-serif;
      background: linear-gradient(180deg, #f7f3ea 0%, var(--bg) 100%);
      color: var(--ink);
    }
    header {
      padding: 28px 32px 20px;
      border-bottom: 1px solid rgba(0, 0, 0, 0.06);
      background: rgba(255, 255, 255, 0.7);
      backdrop-filter: blur(10px);
      position: sticky;
      top: 0;
      z-index: 5;
    }
    h1 {
      margin: 0 0 8px;
      font-size: 28px;
    }
    .subtitle {
      margin: 0;
      color: var(--muted);
      max-width: 1080px;
      line-height: 1.45;
    }
    main {
      padding: 24px 32px 40px;
    }
    .controls, .cards, .panels {
      display: grid;
      gap: 16px;
    }
    .controls {
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      margin-bottom: 20px;
      align-items: end;
    }
    .control {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    label {
      color: var(--muted);
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    input, select, button {
      border: 1px solid #cfc7b8;
      border-radius: 14px;
      padding: 11px 13px;
      background: #fff;
      font-size: 14px;
      color: var(--ink);
    }
    button {
      cursor: pointer;
      background: var(--accent);
      border-color: var(--accent);
      color: #fff;
      font-weight: 600;
    }
    button.secondary {
      background: #fff;
      color: var(--danger);
      border-color: #efc4c4;
    }
    button:disabled {
      cursor: not-allowed;
      opacity: 0.55;
    }
    .cards {
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      margin-bottom: 20px;
    }
    .card, .panel {
      background: var(--panel);
      border: 1px solid var(--panel-border);
      border-radius: 18px;
      box-shadow: 0 18px 45px rgba(15, 23, 42, 0.06);
    }
    .card {
      padding: 16px 18px;
    }
    .card .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .card .value {
      margin-top: 8px;
      font-size: 28px;
      font-weight: 700;
    }
    .panels {
      grid-template-columns: 1.45fr 1fr;
      align-items: start;
    }
    .panel {
      padding: 18px;
      overflow: hidden;
    }
    .panel h2 {
      margin: 0 0 10px;
      font-size: 18px;
    }
    .panel p {
      margin: 0 0 16px;
      color: var(--muted);
      line-height: 1.45;
    }
    .legend {
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      margin-bottom: 12px;
      color: var(--muted);
      font-size: 13px;
    }
    .legend span {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .swatch {
      width: 18px;
      height: 4px;
      border-radius: 999px;
      display: inline-block;
    }
    .graph-shell {
      background: linear-gradient(180deg, #fbfaf6 0%, #f8f5ed 100%);
      border-radius: 16px;
      border: 1px solid #e8e1d4;
      overflow: auto;
      min-height: 420px;
    }
    .security-shell {
      display: grid;
      gap: 16px;
      grid-template-columns: 280px minmax(0, 1fr);
    }
    .identity-directory, .path-board {
      display: grid;
      gap: 12px;
    }
    .identity-chip, .path-card, .service-chip {
      border: 1px solid #e4ddcf;
      border-radius: 16px;
      background: #fff;
      padding: 12px;
    }
    .identity-chip {
      display: grid;
      gap: 5px;
    }
    .identity-chip strong, .service-chip strong {
      display: block;
    }
    .identity-chip small, .service-chip small {
      color: var(--muted);
      overflow-wrap: anywhere;
    }
    .path-card {
      display: grid;
      grid-template-columns: minmax(180px, 1fr) minmax(170px, 0.9fr) minmax(180px, 1fr);
      gap: 12px;
      align-items: stretch;
      position: relative;
      overflow: hidden;
    }
    .path-card.live {
      border-color: #93c5fd;
      box-shadow: 0 18px 45px rgba(37, 99, 235, 0.12);
    }
    .path-node {
      border-radius: 14px;
      padding: 11px;
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      min-width: 0;
    }
    .path-node.consumer {
      background: var(--secure-soft);
      border-color: #bfdbfe;
    }
    .path-node.service {
      background: var(--service-soft);
      border-color: #bbf7d0;
    }
    .path-node.gateway {
      background: #fff7ed;
      border-color: #fed7aa;
    }
    .path-node strong {
      display: block;
      margin-bottom: 4px;
    }
    .path-node code, .identity-chip code, .service-chip code {
      overflow-wrap: anywhere;
    }
    .lane {
      height: 9px;
      border-radius: 999px;
      background: #e5e7eb;
      position: relative;
      overflow: hidden;
      margin: 7px 0;
    }
    .path-card.live .lane {
      background: linear-gradient(90deg, var(--secure-soft), #bfdbfe, var(--service-soft));
    }
    .path-card.live .lane::after {
      content: "";
      width: 34px;
      height: 9px;
      border-radius: 999px;
      background: var(--secure);
      position: absolute;
      left: -40px;
      top: 0;
      animation: pathPulse 1.35s linear infinite;
      box-shadow: 0 0 18px rgba(37, 99, 235, 0.7);
    }
    @keyframes pathPulse {
      from { transform: translateX(0); }
      to { transform: translateX(520px); }
    }
    .path-meta {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 8px;
    }
    .path-pill {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 3px 8px;
      background: #f1f5f9;
      color: #475569;
      font-size: 12px;
      font-weight: 700;
    }
    .path-pill.live {
      background: var(--secure-soft);
      color: var(--secure);
    }
    .path-pill.disabled {
      background: #fee2e2;
      color: #991b1b;
    }
    .service-grid {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      margin-top: 14px;
    }
    svg {
      width: 100%;
      min-width: 1100px;
      display: block;
    }
    .message {
      margin-bottom: 16px;
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid var(--panel-border);
      background: var(--accent-soft);
      color: var(--accent);
      display: none;
    }
    .message.error {
      background: var(--danger-soft);
      color: var(--danger);
      border-color: #f4b6b6;
    }
    .table-wrap {
      overflow: auto;
      border-radius: 16px;
      border: 1px solid #e8e1d4;
      background: #fff;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 760px;
    }
    th, td {
      padding: 11px 12px;
      border-bottom: 1px solid #efe8da;
      text-align: left;
      vertical-align: top;
    }
    th {
      background: #faf7f0;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    code {
      background: #eef6f4;
      padding: 2px 6px;
      border-radius: 8px;
      font-size: 12px;
    }
    .muted { color: var(--muted); }
    .status-pill {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 4px 9px;
      font-size: 12px;
      font-weight: 700;
      gap: 6px;
    }
    .status-pill.allocated { background: #e7f6ec; color: #166534; }
    .status-pill.stale { background: #fff1e6; color: #9a3412; }
    .status-pill.released { background: #fee2e2; color: #991b1b; }
    .class-pill {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 4px 9px;
      font-size: 12px;
      font-weight: 700;
    }
    .class-pill.internal { background: #d7f3ef; color: var(--lane); }
    .class-pill.inbound { background: var(--warning-soft); color: var(--warning); }
    .class-pill.outbound { background: #ede9fe; color: var(--external-out); }
    .class-pill.other { background: #f1f5f9; color: #475569; }
    .stack {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .empty {
      color: var(--muted);
      padding: 22px;
      text-align: center;
    }
    @media (max-width: 1200px) {
      .panels {
        grid-template-columns: 1fr;
      }
      .security-shell, .path-card {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <header>
    <h1>CMXsafeMAC-IPv6 Monitor</h1>
    <p class="subtitle">
      Live topology and flow view for managed pods, managed IPv6 addresses on <code>eth0</code>, automatic managed identities on <code>net1</code>, and caller-driven explicit IPv6 addresses on
      <code>net1</code>. The traffic graph is driven by a dedicated collector that sniffs packets on the shared <code>br-explicit-v6</code> lane, so it highlights
      traffic inside <code>explicit-v6-lan</code> and traffic that enters or leaves that lane.
    </p>
  </header>
  <main>
    <div id="message" class="message"></div>

    <section class="controls">
      <div class="control">
        <label for="prefix-filter">Prefix Filter</label>
        <select id="prefix-filter">
          <option value="">All prefixes</option>
        </select>
      </div>
      <div class="control">
        <label for="search-filter">Search</label>
        <input id="search-filter" type="text" placeholder="pod, namespace, MAC, or address">
      </div>
      <div class="control">
        <label for="window-filter">Traffic Window</label>
        <select id="window-filter">
          <option value="30">Last 30 seconds</option>
          <option value="60" selected>Last 60 seconds</option>
          <option value="300">Last 5 minutes</option>
        </select>
      </div>
      <div class="control">
        <label>&nbsp;</label>
        <button id="refresh-button" type="button">Refresh now</button>
      </div>
      <div class="control">
        <label>&nbsp;</label>
        <button id="clear-stale-button" class="secondary" type="button">Clear stale rows</button>
      </div>
    </section>

    <section class="cards" id="cards"></section>

    <section class="panel" style="margin-bottom: 18px;">
      <h2>Secure Communication Path Monitor</h2>
      <p>
        This view combines the dashboard's registered Security Context with live packets. Identities use readable aliases when available, while canonical IPv6 usernames remain visible for auditability. A path starts as registered, then animates when observed traffic matches the declared consumer, published service, and port.
      </p>
      <div id="security-topology" class="security-shell">
        <div class="empty">Waiting for dashboard topology.</div>
      </div>
    </section>

    <section class="panels">
      <div class="panel">
        <h2>Live Traffic Topology</h2>
        <p>
          Pods are shown with their managed identity on <code>eth0</code>, their automatic managed <code>net1</code> identity, and any caller-driven explicit IPv6 addresses on <code>net1</code>. Lines show
          traffic observed by the collector on <code>br-explicit-v6</code> in the current window. Orange lines are incoming from outside the
          <code>explicit-v6-lan</code>.
        </p>
        <div class="legend">
          <span><i class="swatch" style="background: var(--lane)"></i> Internal explicit-v6-lan traffic</span>
          <span><i class="swatch" style="background: var(--external-in)"></i> Incoming from outside explicit-v6-lan</span>
          <span><i class="swatch" style="background: var(--external-out)"></i> Outgoing to outside explicit-v6-lan</span>
        </div>
        <div class="graph-shell">
          <svg id="topology-graph" viewBox="0 0 1400 420" preserveAspectRatio="xMinYMin meet"></svg>
        </div>
      </div>

      <div class="panel">
        <h2>Live Flow Details</h2>
        <p>
          This table is refreshed automatically and groups packets by source address, destination address, protocol, and ports. Prefix filtering
          applies to the explicit route prefix involved in the flow, not to managed IPv6 on <code>eth0</code>.
        </p>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Class</th>
                <th>Source</th>
                <th>Destination</th>
                <th>Protocol</th>
                <th>Packets</th>
                <th>Bytes</th>
                <th>Last Seen</th>
              </tr>
            </thead>
            <tbody id="flows-body">
              <tr><td colspan="7" class="empty">Waiting for traffic data.</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="panel" style="margin-top: 18px;">
      <h2>Managed Pods and Addresses</h2>
      <p>
        Managed IPv6 addresses live on <code>eth0</code>. Automatic managed and caller-driven explicit IPv6 addresses live on <code>net1</code> and are grouped by their explicit
        route prefix. The table stays useful even when there is no live traffic in the current window.
      </p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Status</th>
              <th>Namespace / Pod</th>
              <th>Managed MAC</th>
              <th>Managed IPv6</th>
              <th>net1 IPv6s</th>
              <th>Counter</th>
              <th>Node</th>
            </tr>
          </thead>
          <tbody id="allocations-body">
            <tr><td colspan="7" class="empty">Waiting for allocation data.</td></tr>
          </tbody>
        </table>
      </div>
    </section>
  </main>

  <script>
    const state = {
      snapshot: null,
      timer: null,
      intervalMs: 3000,
    };

    const prefixFilter = document.getElementById('prefix-filter');
    const searchFilter = document.getElementById('search-filter');
    const windowFilter = document.getElementById('window-filter');
    const refreshButton = document.getElementById('refresh-button');
    const clearStaleButton = document.getElementById('clear-stale-button');
    const messageBox = document.getElementById('message');
    const cards = document.getElementById('cards');
    const securityTopology = document.getElementById('security-topology');
    const graph = document.getElementById('topology-graph');
    const flowsBody = document.getElementById('flows-body');
    const allocationsBody = document.getElementById('allocations-body');

    function h(value) {
      return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
    }

    function normalizeText(value) {
      return String(value ?? '').trim().toLowerCase();
    }

    function compactAddress(value) {
      const text = String(value || '');
      if (!text) return '-';
      if (text.length <= 34) return text;
      return `${text.slice(0, 18)}...${text.slice(-12)}`;
    }

    function identityLabel(identity) {
      return identity?.display_name || identity?.alias || identity?.username || 'unknown identity';
    }

    function flattenSecurityTargets(snapshot) {
      const targets = snapshot?.security_topology?.targets || [];
      const identities = [];
      const services = [];
      const paths = [];
      for (const target of targets) {
        for (const identity of target.identities || []) {
          identities.push({ ...identity, target_name: target.target?.name || '' });
        }
        for (const service of target.published_services || []) {
          services.push({ ...service, target_name: target.target?.name || '' });
        }
        for (const path of target.registered_paths || []) {
          paths.push({ ...path, target_name: target.target?.name || '' });
        }
      }
      return { identities, services, paths };
    }

    function uniquePrefixes(snapshot) {
      const values = new Map();
      for (const prefix of snapshot?.traffic?.prefixes || []) {
        if (prefix?.prefix_hex) {
          values.set(prefix.prefix_hex, prefix.route_network || `${prefix.prefix_hex}::/${snapshot.traffic.route_prefix_len || 16}`);
        }
      }
      for (const row of snapshot?.explicit_assignments || []) {
        if ((row.status || '') !== 'ACTIVE') continue;
        if (!row.gw_tag_hex) continue;
        values.set(row.gw_tag_hex, `${row.gw_tag_hex}::/${snapshot?.traffic?.route_prefix_len || 16}`);
      }
      return [...values.entries()].sort((a, b) => a[0].localeCompare(b[0]));
    }

    function setMessage(text, error = false) {
      if (!text) {
        messageBox.style.display = 'none';
        messageBox.textContent = '';
        messageBox.classList.remove('error');
        return;
      }
      messageBox.style.display = 'block';
      messageBox.textContent = text;
      messageBox.classList.toggle('error', error);
    }

    function allocationStatusClass(status) {
      const value = normalizeText(status);
      if (value === 'allocated') return 'allocated';
      if (value === 'stale') return 'stale';
      return 'released';
    }

    function flowClass(flow) {
      switch (flow.classification) {
        case 'internal-explicit-v6-lan':
          return { label: 'Internal lane', className: 'internal' };
        case 'inbound-from-outside-explicit-v6-lan':
          return { label: 'Inbound external', className: 'inbound' };
        case 'outbound-to-outside-explicit-v6-lan':
          return { label: 'Outbound external', className: 'outbound' };
        default:
          return { label: 'Other', className: 'other' };
      }
    }

    function flowMatchesSearch(flow, searchText) {
      if (!searchText) return true;
      const haystack = normalizeText([
        flow.classification,
        flow.protocol,
        flow.src?.address,
        flow.dst?.address,
        flow.src?.namespace,
        flow.src?.pod_name,
        flow.dst?.namespace,
        flow.dst?.pod_name,
        flow.src?.label,
        flow.dst?.label,
        flow.route_network,
        flow.prefix_hex,
        ...(flow.prefixes_involved || []),
      ].join(' '));
      return haystack.includes(searchText);
    }

    function podMatchesSearch(allocation, explicitRows, searchText) {
      if (!searchText) return true;
      const parts = [
        allocation.status,
        allocation.namespace,
        allocation.pod_name,
        allocation.assigned_mac,
        allocation.assigned_ipv6,
        allocation.auto_managed_explicit_ipv6,
        allocation.gw_mac,
        allocation.node_name,
        allocation.counter,
      ];
      for (const row of explicitRows) {
        parts.push(row.requested_ipv6, row.gw_tag_hex, row.mac_dev);
      }
      return normalizeText(parts.join(' ')).includes(searchText);
    }

    function securityPathMatchesSearch(path, searchText) {
      if (!searchText) return true;
      const parts = [
        path.target_name,
        path.context_alias,
        path.state,
        path.consumer?.username,
        path.consumer?.alias,
        path.consumer?.canonical_ipv6,
        path.publisher?.username,
        path.publisher?.alias,
        path.publisher?.canonical_ipv6,
        path.service?.alias,
        path.service?.canonical_ipv6,
        path.service?.port,
        ...(path.observed_flows || []).flatMap((flow) => [
          flow.src_address,
          flow.src_port,
          flow.src_label,
          flow.dst_address,
          flow.dst_port,
          flow.dst_label,
          flow.direction,
        ]),
      ];
      return normalizeText(parts.join(' ')).includes(searchText);
    }

    function explicitTagFromAddress(address) {
      const segment = String(address || '').trim().split(':')[0] || '';
      return segment ? segment.padStart(4, '0').slice(-4).toLowerCase() : '';
    }

    function autoManagedExplicitRow(allocation) {
      if (!allocation?.auto_managed_explicit_ipv6) {
        return null;
      }
      return {
        requested_ipv6: allocation.auto_managed_explicit_ipv6,
        gw_tag_hex: explicitTagFromAddress(allocation.auto_managed_explicit_ipv6),
        target_gw_mac: allocation.gw_mac || '',
        mac_dev: '00:00:00:00:00:00',
        automatic: true,
      };
    }

    function net1RowsForAllocation(allocation, explicitRows) {
      const rows = [...explicitRows];
      const autoRow = autoManagedExplicitRow(allocation);
      if (autoRow) {
        rows.unshift(autoRow);
      }
      return rows.sort((a, b) => String(a.requested_ipv6 || '').localeCompare(String(b.requested_ipv6 || '')));
    }

    function formatProtocol(flow) {
      const protocol = normalizeText(flow.protocol);
      if (protocol === 'tcp' || protocol === 'udp') {
        return `${protocol}/${flow.src_port || '-'}→${flow.dst_port || '-'}`;
      }
      if (protocol === 'icmp6') {
        return `icmp6/${flow.icmp_type ?? '-'}`;
      }
      return protocol || '-';
    }

    function buildView(snapshot) {
      const prefix = normalizeText(prefixFilter.value);
      const searchText = normalizeText(searchFilter.value);
      const allocations = [...(snapshot.allocations || [])];
      const activeExplicit = (snapshot.explicit_assignments || []).filter((row) => normalizeText(row.status) === 'active');
      const flows = (snapshot.traffic?.flows || []).filter((flow) => {
        const prefixes = (flow.prefixes_involved || []).map((value) => normalizeText(value));
        if (prefix && !prefixes.includes(prefix)) return false;
        return flowMatchesSearch(flow, searchText);
      });
      const security = flattenSecurityTargets(snapshot);
      const securityPaths = security.paths.filter((path) => securityPathMatchesSearch(path, searchText));

      const explicitByPodUid = new Map();
      for (const row of activeExplicit) {
        if (prefix && normalizeText(row.gw_tag_hex) !== prefix) continue;
        const podUid = row.pod_uid || '';
        if (!explicitByPodUid.has(podUid)) {
          explicitByPodUid.set(podUid, []);
        }
        explicitByPodUid.get(podUid).push(row);
      }

      const visibleAllocations = allocations.filter((allocation) => {
        const explicitRows = explicitByPodUid.get(allocation.pod_uid || '') || [];
        const net1Rows = net1RowsForAllocation(allocation, explicitRows);
        const matchingPrefixRows = net1Rows.filter((row) => normalizeText(row.gw_tag_hex) === prefix);
        if (prefix && matchingPrefixRows.length === 0 && normalizeText(allocation.status) === 'allocated') {
          return false;
        }
        return podMatchesSearch(allocation, net1Rows, searchText);
      });

      return {
        allocations,
        activeExplicit,
        explicitByPodUid,
        visibleAllocations,
        flows,
        security,
        securityPaths,
        prefix,
        searchText,
      };
    }

    function renderCards(snapshot, view) {
      const allocations = snapshot.allocations || [];
      const explicit = view.activeExplicit || [];
      const inbound = view.flows.filter((flow) => flow.classification === 'inbound-from-outside-explicit-v6-lan').length;
      const lastPacket = snapshot.traffic?.flows?.[0]?.last_seen || snapshot.generated_at || '-';
      const securitySummary = snapshot.security_topology?.summary || {};
      const counters = {
        allocated: allocations.filter((row) => normalizeText(row.status) === 'allocated').length,
        stale: allocations.filter((row) => normalizeText(row.status) === 'stale').length,
        released: allocations.filter((row) => normalizeText(row.status) === 'released').length,
        explicit: explicit.length,
        identities: securitySummary.identity_count || view.security.identities.length,
        services: securitySummary.published_service_count || view.security.services.length,
        registeredPaths: securitySummary.registered_path_count || view.security.paths.length,
        livePaths: securitySummary.live_path_count || view.security.paths.filter((path) => path.live).length,
      };

      cards.innerHTML = `
        <article class="card"><div class="label">Allocated</div><div class="value">${h(counters.allocated)}</div></article>
        <article class="card"><div class="label">Stale</div><div class="value">${h(counters.stale)}</div></article>
        <article class="card"><div class="label">Released</div><div class="value">${h(counters.released)}</div></article>
        <article class="card"><div class="label">Explicit Active</div><div class="value">${h(counters.explicit)}</div></article>
        <article class="card"><div class="label">Identities</div><div class="value">${h(counters.identities)}</div></article>
        <article class="card"><div class="label">Services</div><div class="value">${h(counters.services)}</div></article>
        <article class="card"><div class="label">Registered Paths</div><div class="value">${h(counters.registeredPaths)}</div></article>
        <article class="card"><div class="label">Live Secure Paths</div><div class="value">${h(counters.livePaths)}</div></article>
        <article class="card"><div class="label">Live Flows</div><div class="value">${h(view.flows.length)}</div></article>
        <article class="card"><div class="label">Inbound External</div><div class="value">${h(inbound)}</div></article>
        <article class="card"><div class="label">Last Seen</div><div class="value" style="font-size:18px">${h(lastPacket)}</div></article>
      `;
      clearStaleButton.disabled = counters.stale < 1;
    }

    function renderSecurityTopology(snapshot, view) {
      const topology = snapshot.security_topology || {};
      const errors = topology.errors || [];
      if (errors.length && !view.security.paths.length && !view.security.identities.length) {
        securityTopology.innerHTML = `
          <div class="empty" style="grid-column: 1 / -1;">
            ${h(errors[0].message || 'Dashboard topology is unavailable.')}
          </div>
        `;
        return;
      }

      const identities = view.security.identities;
      const services = view.security.services;
      const paths = view.securityPaths;

      const identityCards = identities.length
        ? identities.slice(0, 18).map((identity) => `
            <div class="identity-chip">
              <strong>${h(identityLabel(identity))}</strong>
              <small>${h(identity.target_name || 'target')}</small>
              <code>${h(compactAddress(identity.canonical_ipv6 || identity.username))}</code>
            </div>
          `).join('')
        : '<div class="empty">No registered identities yet.</div>';

      const serviceCards = services.length
        ? services.slice(0, 8).map((service) => `
            <div class="service-chip">
              <strong>${h(service.alias)}</strong>
              <small>publisher: ${h(service.owner_display_name || service.owner_alias || service.owner_username || '-')}</small>
              <code>[${h(service.canonical_ipv6)}]:${h(service.port)}</code>
            </div>
          `).join('')
        : '<div class="empty">No publishable services registered yet.</div>';

      const pathCards = paths.length
        ? paths.map((path) => {
            const observed = path.observed_flows || [];
            const live = Boolean(path.live);
            const statusClass = live ? 'live' : (!path.enabled ? 'disabled' : '');
            const statusLabel = live ? 'live now' : (path.enabled ? 'registered' : 'disabled');
            const observedSummary = observed.length
              ? observed.slice(0, 3).map((flow) => `
                  <div class="muted">
                    ${h(flow.direction)} ${h(flow.protocol)} ${h(flow.src_address)}:${h(flow.src_port || '-')} to ${h(flow.dst_address)}:${h(flow.dst_port || '-')}
                    <br>gateway observation: ${h(flow.src_label || '-')} to ${h(flow.dst_label || '-')}
                  </div>
                `).join('')
              : '<div class="muted">No matching live flow in the selected window.</div>';
            return `
              <article class="path-card ${live ? 'live' : ''}">
                <div class="path-node consumer">
                  <strong>${h(identityLabel(path.consumer))}</strong>
                  <code>${h(path.consumer?.canonical_ipv6 || path.consumer?.username || '-')}</code>
                  <div class="lane"></div>
                  <small>direct-forwarding endpoint</small>
                </div>
                <div class="path-node gateway">
                  <strong>CMXsafe gateway path</strong>
                  <small>${h(path.context_alias || 'registered access path')}</small>
                  <div class="path-meta">
                    <span class="path-pill ${statusClass}">${h(statusLabel)}</span>
                    <span class="path-pill">${h(path.observed_packet_count || 0)} packets</span>
                    <span class="path-pill">${h(path.last_seen || 'no traffic yet')}</span>
                  </div>
                  ${observedSummary}
                </div>
                <div class="path-node service">
                  <strong>${h(path.service?.alias || 'service')}</strong>
                  <code>[${h(path.service?.canonical_ipv6 || '-')}]:${h(path.service?.port || '-')}</code>
                  <div class="lane"></div>
                  <small>publisher: ${h(identityLabel(path.publisher))}</small>
                </div>
              </article>
            `;
          }).join('')
        : '<div class="empty">No registered access paths match the current search.</div>';

      securityTopology.innerHTML = `
        <aside class="identity-directory">
          <h3 style="margin:0;">Endpoint Identities</h3>
          ${identityCards}
        </aside>
        <div class="path-board">
          <div class="service-grid">${serviceCards}</div>
          ${pathCards}
        </div>
      `;
    }

    function renderAllocations(view) {
      if (!view.visibleAllocations.length) {
        allocationsBody.innerHTML = '<tr><td colspan="7" class="empty">No managed pods match the current filter.</td></tr>';
        return;
      }

      allocationsBody.innerHTML = view.visibleAllocations.map((allocation) => {
        const explicitRows = view.explicitByPodUid.get(allocation.pod_uid || '') || [];
        const net1Rows = net1RowsForAllocation(allocation, explicitRows);
        const explicitCell = net1Rows.length
          ? net1Rows.map((row) => {
              const layout = row.automatic
                ? `[${h(row.gw_tag_hex)} - ${h(row.target_gw_mac)} - counter+1 - 00:00:00:00:00:00] automatic`
                : `[${h(row.gw_tag_hex)} - ${h(row.target_gw_mac)} - 0000 - ${h(row.mac_dev)}]`;
              return `<div class="stack"><code>${h(row.requested_ipv6)}</code><span class="muted">${layout}</span></div>`;
            }).join('')
          : '<span class="muted">No net1 IPv6 assigned</span>';
        return `
          <tr>
            <td><span class="status-pill ${allocationStatusClass(allocation.status)}">${h(allocation.status)}</span></td>
            <td><div class="stack"><strong>${h(allocation.namespace)} / ${h(allocation.pod_name)}</strong><span class="muted">${h(allocation.pod_uid)}</span></div></td>
            <td><code>${h(allocation.assigned_mac || '-')}</code></td>
            <td><code>${h(allocation.assigned_ipv6 || '-')}</code></td>
            <td>${explicitCell}</td>
            <td>${h(allocation.counter ?? '-')}</td>
            <td>${h(allocation.node_name || '-')}</td>
          </tr>
        `;
      }).join('');
    }

    function renderFlows(view) {
      if (!view.flows.length) {
        flowsBody.innerHTML = '<tr><td colspan="7" class="empty">No flow has been observed by the collector on br-explicit-v6 in the current window.</td></tr>';
        return;
      }
      flowsBody.innerHTML = view.flows.map((flow) => {
        const meta = flowClass(flow);
        return `
          <tr>
            <td><span class="class-pill ${meta.className}">${h(meta.label)}</span></td>
            <td><div class="stack"><code>${h(flow.src?.address || '-')}</code><span class="muted">${h(flow.src?.label || '-')}</span></div></td>
            <td><div class="stack"><code>${h(flow.dst?.address || '-')}</code><span class="muted">${h(flow.dst?.label || '-')}</span></div></td>
            <td>${h(formatProtocol(flow))}</td>
            <td>${h(flow.packets)}</td>
            <td>${h(flow.bytes)}</td>
            <td>${h(flow.last_seen || '-')}</td>
          </tr>
        `;
      }).join('');
    }

    function renderGraph(view) {
      const activeAllocations = view.visibleAllocations.filter((row) => normalizeText(row.status) === 'allocated');
      if (!activeAllocations.length) {
        graph.setAttribute('viewBox', '0 0 1400 320');
        graph.innerHTML = '<text x="36" y="52" font-size="18" fill="#6b7280">No active managed pod matches the current filter.</text>';
        return;
      }

      const podLayouts = [];
      const addressNodes = new Map();
      let y = 32;
      const boxX = 360;
      const boxWidth = 560;

      for (const allocation of activeAllocations) {
        const explicitRows = view.explicitByPodUid.get(allocation.pod_uid || '') || [];
        const net1Rows = net1RowsForAllocation(allocation, explicitRows);
        const rowCount = Math.max(1, net1Rows.length);
        const boxHeight = 120 + Math.max(0, rowCount - 1) * 34;
        const managedY = y + 68;
        const explicitBaseY = y + 68;
        const pod = {
          allocation,
          explicitRows: net1Rows,
          x: boxX,
          y,
          width: boxWidth,
          height: boxHeight,
          managedNode: { x: boxX + 34, y: managedY, address: allocation.assigned_ipv6, label: 'managed IPv6 on eth0' },
        };
        if (allocation.assigned_ipv6) {
          addressNodes.set(allocation.assigned_ipv6, { x: pod.managedNode.x, y: pod.managedNode.y, type: 'managed', pod });
        }
        net1Rows.forEach((row, index) => {
          const node = { x: boxX + 270, y: explicitBaseY + index * 34, address: row.requested_ipv6, type: 'explicit', pod };
          addressNodes.set(row.requested_ipv6, node);
        });
        podLayouts.push(pod);
        y += boxHeight + 26;
      }

      const inboundExternal = new Map();
      const outboundExternal = new Map();
      for (const flow of view.flows) {
        if (flow.classification === 'inbound-from-outside-explicit-v6-lan') {
          inboundExternal.set(flow.src.address, null);
        }
        if (flow.classification === 'outbound-to-outside-explicit-v6-lan') {
          outboundExternal.set(flow.dst.address, null);
        }
      }

      let externalY = 56;
      for (const address of inboundExternal.keys()) {
        inboundExternal.set(address, { x: 110, y: externalY });
        externalY += 72;
      }
      externalY = 56;
      for (const address of outboundExternal.keys()) {
        outboundExternal.set(address, { x: 1220, y: externalY });
        externalY += 72;
      }

      const totalHeight = Math.max(y + 24, 360, inboundExternal.size * 72 + 80, outboundExternal.size * 72 + 80);
      graph.setAttribute('viewBox', `0 0 1400 ${totalHeight}`);

      const shapes = [];
      const lines = [];

      shapes.push(`<text x="56" y="28" font-size="14" fill="#6b7280">Outside explicit-v6-lan</text>`);
      shapes.push(`<text x="1090" y="28" font-size="14" fill="#6b7280">Outside explicit-v6-lan</text>`);

      for (const [address, node] of inboundExternal) {
        shapes.push(`<circle cx="${node.x}" cy="${node.y}" r="13" fill="#fff3e8" stroke="#c2410c" stroke-width="2"></circle>`);
        shapes.push(`<text x="${node.x + 22}" y="${node.y + 5}" font-size="13" fill="#7c2d12">${h(address)}</text>`);
      }
      for (const [address, node] of outboundExternal) {
        shapes.push(`<circle cx="${node.x}" cy="${node.y}" r="13" fill="#f1e8ff" stroke="#7c3aed" stroke-width="2"></circle>`);
        shapes.push(`<text x="${node.x - 18}" y="${node.y + 5}" text-anchor="end" font-size="13" fill="#5b21b6">${h(address)}</text>`);
      }

      for (const pod of podLayouts) {
        shapes.push(`<rect x="${pod.x}" y="${pod.y}" width="${pod.width}" height="${pod.height}" rx="18" fill="#fffdf8" stroke="#d8d2c4" stroke-width="1.5"></rect>`);
        shapes.push(`<text x="${pod.x + 22}" y="${pod.y + 28}" font-size="18" font-weight="700" fill="#1f2937">${h(`${pod.allocation.namespace} / ${pod.allocation.pod_name}`)}</text>`);
        shapes.push(`<text x="${pod.x + 22}" y="${pod.y + 48}" font-size="12" fill="#6b7280">MAC ${h(pod.allocation.assigned_mac || '-')} | counter ${h(pod.allocation.counter ?? '-')} | node ${h(pod.allocation.node_name || '-')}</text>`);
        shapes.push(`<circle cx="${pod.managedNode.x}" cy="${pod.managedNode.y}" r="12" fill="#eef6f4" stroke="#0f766e" stroke-width="2"></circle>`);
        shapes.push(`<text x="${pod.managedNode.x + 24}" y="${pod.managedNode.y + 5}" font-size="13" fill="#0f766e">${h(pod.allocation.assigned_ipv6 || 'managed IPv6 unavailable')}</text>`);
        shapes.push(`<text x="${pod.managedNode.x + 24}" y="${pod.managedNode.y + 22}" font-size="11" fill="#6b7280">managed IPv6 on eth0</text>`);
        if (!pod.explicitRows.length) {
          shapes.push(`<text x="${pod.x + 270}" y="${pod.y + 73}" font-size="12" fill="#6b7280">No net1 IPv6 for this filter</text>`);
        }
        pod.explicitRows.forEach((row, index) => {
          const node = addressNodes.get(row.requested_ipv6);
          if (!node) return;
          shapes.push(`<circle cx="${node.x}" cy="${node.y}" r="12" fill="#d7f3ef" stroke="#0f766e" stroke-width="2"></circle>`);
          shapes.push(`<text x="${node.x + 24}" y="${node.y + 5}" font-size="13" fill="#0f766e">${h(row.requested_ipv6)}</text>`);
          shapes.push(`<text x="${node.x + 24}" y="${node.y + 22}" font-size="11" fill="#6b7280">${row.automatic ? `[${h(row.gw_tag_hex)} - ${h(row.target_gw_mac)} - counter+1 - 00:00:00:00:00:00]` : `[${h(row.gw_tag_hex)} - ${h(row.target_gw_mac)} - 0000 - ${h(row.mac_dev)}]`}</text>`);
        });
      }

      for (const flow of view.flows) {
        const width = Math.max(1.5, Math.min(7, 1.5 + Math.log2((flow.packets || 0) + 1)));
        let color = '#94a3b8';
        let start = null;
        let end = null;

        if (flow.classification === 'internal-explicit-v6-lan') {
          color = '#0f766e';
          start = addressNodes.get(flow.src.address);
          end = addressNodes.get(flow.dst.address);
        } else if (flow.classification === 'inbound-from-outside-explicit-v6-lan') {
          color = '#c2410c';
          start = inboundExternal.get(flow.src.address);
          end = addressNodes.get(flow.dst.address);
        } else if (flow.classification === 'outbound-to-outside-explicit-v6-lan') {
          color = '#7c3aed';
          start = addressNodes.get(flow.src.address);
          end = outboundExternal.get(flow.dst.address);
        }

        if (!start || !end) {
          continue;
        }

        lines.push(
          `<path d="M ${start.x} ${start.y} C ${(start.x + end.x) / 2} ${start.y}, ${(start.x + end.x) / 2} ${end.y}, ${end.x} ${end.y}" ` +
          `fill="none" stroke="${color}" stroke-opacity="0.74" stroke-width="${width}" stroke-linecap="round"></path>`
        );
      }

      if (!lines.length) {
        shapes.push(`<text x="34" y="${Math.max(totalHeight - 28, 120)}" font-size="14" fill="#6b7280">No explicit-lane traffic has been observed in the current window. Generate ping or application traffic to populate this view.</text>`);
      }

      graph.innerHTML = `${lines.join('')}${shapes.join('')}`;
    }

    async function clearStaleRows() {
      try {
        const form = new URLSearchParams();
        form.set('action', 'clear_stale');
        const response = await fetch('api.php', {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' },
          body: form.toString(),
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || 'Unable to clear stale rows.');
        }
        setMessage(`Cleared ${payload.deleted ?? 0} stale allocation row(s).`);
        await loadSnapshot();
      } catch (error) {
        setMessage(error.message, true);
      }
    }

    async function loadSnapshot() {
      const query = new URLSearchParams();
      query.set('window_seconds', windowFilter.value || '60');
      if (prefixFilter.value) {
        query.set('prefix', prefixFilter.value);
      }

      try {
        refreshButton.disabled = true;
        const response = await fetch(`api.php?${query.toString()}`, { cache: 'no-store' });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || 'Unable to load monitor data.');
        }
        state.snapshot = payload;
        const prefixes = uniquePrefixes(payload);
        const currentPrefix = prefixFilter.value;
        prefixFilter.innerHTML = '<option value="">All prefixes</option>' + prefixes.map(([prefixHex, route]) => (
          `<option value="${h(prefixHex)}">${h(prefixHex)} (${h(route)})</option>`
        )).join('');
        if ([...prefixFilter.options].some((option) => option.value === currentPrefix)) {
          prefixFilter.value = currentPrefix;
        }

        const view = buildView(payload);
        renderCards(payload, view);
        renderSecurityTopology(payload, view);
        renderAllocations(view);
        renderFlows(view);
        renderGraph(view);
        if (payload.traffic?.errors?.length) {
          setMessage(`Traffic collector reported ${payload.traffic.errors.length} capture warning(s).`, true);
        } else {
          setMessage('');
        }
      } catch (error) {
        setMessage(error.message, true);
      } finally {
        refreshButton.disabled = false;
      }
    }

    refreshButton.addEventListener('click', () => loadSnapshot());
    clearStaleButton.addEventListener('click', () => clearStaleRows());
    prefixFilter.addEventListener('change', () => loadSnapshot());
    windowFilter.addEventListener('change', () => loadSnapshot());
    searchFilter.addEventListener('input', () => {
      if (!state.snapshot) return;
      const view = buildView(state.snapshot);
      renderCards(state.snapshot, view);
      renderSecurityTopology(state.snapshot, view);
      renderAllocations(view);
      renderFlows(view);
      renderGraph(view);
    });

    loadSnapshot();
    state.timer = window.setInterval(loadSnapshot, state.intervalMs);
  </script>
</body>
</html>
