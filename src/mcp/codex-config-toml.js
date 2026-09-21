function parseSimpleTomlValue(rawValue) {
  const trimmed = rawValue.trim();
  if (!trimmed) {
    return void 0;
  }
  if ((trimmed.startsWith("\"") && trimmed.endsWith("\""))
    || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
    try {
      return JSON.parse(trimmed);
    } catch {
      return trimmed;
    }
  }
  if (trimmed === "true") {
    return true;
  }
  if (trimmed === "false") {
    return false;
  }
  if (/^[+-]?\d+$/.test(trimmed)) {
    return Number.parseInt(trimmed, 10);
  }
  if (/^[+-]?\d+\.\d+$/.test(trimmed)) {
    return Number.parseFloat(trimmed);
  }
  return trimmed;
}

export function parseMcpServersFromToml(rawConfig) {
  const mcpServers = {};
  let currentServer = null;
  for (const rawLine of rawConfig.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }
    const sectionMatch = /^\[\s*mcp_servers\.(?:"([^"]+)"|([A-Za-z0-9_.-]+))\s*\]$/.exec(line);
    if (sectionMatch) {
      currentServer = sectionMatch[1] ?? sectionMatch[2] ?? null;
      if (currentServer) {
        mcpServers[currentServer] = mcpServers[currentServer] ?? {};
      }
      continue;
    }
    if (line.startsWith("[")) {
      currentServer = null;
      continue;
    }
    if (!currentServer) {
      continue;
    }
    const separatorIndex = line.indexOf("=");
    if (separatorIndex === -1) {
      continue;
    }
    const key = line.slice(0, separatorIndex).trim();
    if (!key) {
      continue;
    }
    const value = parseSimpleTomlValue(line.slice(separatorIndex + 1));
    if (value !== void 0) {
      mcpServers[currentServer][key] = value;
    }
  }
  return mcpServers;
}
