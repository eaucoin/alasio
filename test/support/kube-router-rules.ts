/**
 * A k3s node's filter table as iptables-save prints it, with kube-router's network policy
 * rules for the pods on it, beside kube-proxy's and flannel's: a stand-in for what
 * `iptables-save -t filter` (or `ip6tables-save`) answers on a node. Its pods' chains,
 * and the jumps to them, are as kube-router v2.6.3 writes them
 * (pkg/controllers/netpol/pod.go, network_policy_controller.go); its policies' chains are
 * abridged to a rule that lets through and its return.
 */
import { createHash } from "node:crypto";

/** A network policy selecting a pod, by its name and the directions it holds. */
export interface FixturePolicy {
  readonly name: string;
  readonly type: "ingress" | "egress" | "both";
}

/** A pod on the node, by its namespace, name and address, and the policies selecting it, of which it may have none. */
export interface FixturePod {
  readonly namespace: string;
  readonly pod: string;
  readonly ip: string;
  readonly policies: readonly FixturePolicy[];
}

/** A session's pod, as a session's namespace's policies select it (cli/src/manifests/network-policies.ts). */
export const sessionPod = (pod: string, ip: string, namespace = "alasio-sessions"): FixturePod => ({
  namespace,
  pod,
  ip,
  policies: [{ name: "default-deny", type: "both" }, { name: "bayma-from-alasio", type: "ingress" }, { name: "telemetry-to-alasio", type: "egress" }],
});

/** The cluster's DNS, which no policy selects. */
export const clusterDns = (ip: string): FixturePod => ({ namespace: "kube-system", pod: "coredns-7f496c8d7d-x2v5q", ip, policies: [] });

/** RFC 4648 base32, as Go's base32.StdEncoding writes it, without its padding. */
function base32(bytes: Buffer): string {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
  let bits = "";
  for (const byte of bytes) bits += byte.toString(2).padStart(8, "0");
  return [...bits.padEnd(Math.ceil(bits.length / 5) * 5, "0").matchAll(/.{5}/gu)].map(([chunk]) => alphabet[Number.parseInt(chunk, 2)]).join("");
}

/** A chain's name as kube-router makes it: its prefix and 16 characters of the base32 of the SHA-256 of `parts`. */
const chainName = (prefix: string, ...parts: string[]): string => prefix + base32(createHash("sha256").update(parts.join("")).digest()).slice(0, 16);

/** A comment as iptables-save prints it: quoted, its quotes, backslashes and apostrophes escaped. */
const comment = (text: string): string => `-m comment --comment "${text.replace(/["'\\]/gu, (special) => `\\${special}`)}"`;

/** The filter table of a node of `family` with `pods` on it, as kube-router's sync `sync` (its version, a timestamp) wrote it. */
export function filterTable(pods: readonly FixturePod[], { family = 4, sync = "1791844064000000000" }: { readonly family?: 4 | 6; readonly sync?: string } = {}): string {
  const v6 = family === 6;
  const mask = v6 ? "128" : "32";
  const icmp = v6 ? ["ipv6-icmp", "icmp6 --icmpv6-type", "128", "1", "3"] : ["icmp", "icmp --icmp-type", "8", "3", "11"];
  const [protocol, match, echo, unreachable, exceeded] = icmp;
  const podCidr = v6 ? "fd00:42::/56" : "10.42.0.0/16";
  const policies = new Map(pods.flatMap(({ namespace, policies: selecting }) => selecting.map(({ name }) => [`${namespace}/${name}`, chainName("KUBE-NWPLCY-", namespace, name, sync, v6 ? "IPv6" : "IPv4")])));
  const podChains = pods.map((pod) => ({ ...pod, chain: chainName("KUBE-POD-FW-", pod.namespace, pod.pod, sync) }));

  const chains = [
    "FLANNEL-FWD",
    "KUBE-FIREWALL",
    "KUBE-FORWARD",
    "KUBE-KUBELET-CANARY",
    "KUBE-NWPLCY-COMMON",
    "KUBE-NWPLCY-DEFAULT",
    ...policies.values(),
    ...podChains.map(({ chain }) => chain),
    "KUBE-PROXY-FIREWALL",
    "KUBE-ROUTER-FORWARD",
    "KUBE-ROUTER-INPUT",
    "KUBE-ROUTER-OUTPUT",
    "KUBE-SERVICES",
  ];

  const policyRules = [...policies].flatMap(([key, chain]) => {
    const [namespace, name] = key.split("/");
    return name === "default-deny" ? [] : [
      `-A ${chain} ${comment(`rule to ACCEPT traffic from all sources to dest pods selected by policy name: ${name} namespace ${namespace}`)} -j MARK --set-xmark 0x10000/0x10000`,
      `-A ${chain} ${comment(`rule to ACCEPT traffic from all sources to dest pods selected by policy name: ${name} namespace ${namespace}`)} -m mark --mark 0x10000/0x10000 -j RETURN`,
    ];
  });

  // Each pod's chain: what pod.go inserts at its top, last first, then what it appends.
  const podRules = podChains.flatMap(({ namespace, pod, ip, policies: selecting, chain }) => {
    const address = `${ip}/${mask}`;
    const types = new Set(selecting.map(({ type }) => type));
    const ingress = types.has("ingress") || types.has("both");
    const egress = types.has("egress") || types.has("both");
    return [
      `-A ${chain} ${comment("common bi-directional traffic policy rules")} -j KUBE-NWPLCY-COMMON`,
      `-A ${chain} -d ${address} ${comment("rule to permit the traffic traffic to pods when source is the pod's local node")} -m addrtype --src-type LOCAL -j ACCEPT`,
      ...(egress ? [] : [`-A ${chain} -s ${address} ${comment("run through default egress network policy chain")} -j KUBE-NWPLCY-DEFAULT`]),
      ...(ingress ? [] : [`-A ${chain} -d ${address} ${comment("run through default ingress network policy chain")} -j KUBE-NWPLCY-DEFAULT`]),
      ...[...selecting].reverse().map(({ name, type }) => {
        const held = type === "ingress" ? `-d ${address} ` : type === "egress" ? `-s ${address} ` : "";
        return `-A ${chain} ${held}${comment(`run through nw policy ${name}`)} -j ${policies.get(`${namespace}/${name}`)}`;
      }),
      `-A ${chain} ${comment(`rule to log dropped traffic POD name:${pod} namespace: ${namespace}`)} -m mark ! --mark 0x10000/0x10000 -m limit --limit 10/min --limit-burst 10 -j NFLOG --nflog-group 100`,
      `-A ${chain} ${comment(`rule to REJECT traffic destined for POD name:${pod} namespace: ${namespace}`)} -m mark ! --mark 0x10000/0x10000 -j REJECT --reject-with ${v6 ? "icmp6-port-unreachable" : "icmp-port-unreachable"}`,
      `-A ${chain} -j MARK --set-xmark 0x0/0x10000`,
      `-A ${chain} ${comment("set mark to ACCEPT traffic that comply to network policies")} -j MARK --set-xmark 0x20000/0x20000`,
    ];
  });

  const jumps = (router: "FORWARD" | "INPUT" | "OUTPUT") =>
    podChains.flatMap(({ namespace, pod, ip, chain }) => {
      const address = `${ip}/${mask}`;
      const to = comment(`rule to jump traffic destined to POD name:${pod} namespace: ${namespace} to chain ${chain}`);
      const from = comment(`rule to jump traffic from POD name:${pod} namespace: ${namespace} to chain ${chain}`);
      const self = `KUBE-ROUTER-${router}`;
      return [
        ...(router === "INPUT" ? [] : [`-A ${self} -d ${address} ${to} -j ${chain}`]),
        ...(router === "FORWARD" ? [`-A ${self} -d ${address} -m physdev --physdev-is-bridged ${to} -j ${chain}`] : []),
        `-A ${self} -s ${address} ${from} -j ${chain}`,
        ...(router === "FORWARD" ? [`-A ${self} -s ${address} -m physdev --physdev-is-bridged ${from} -j ${chain}`] : []),
      ];
    });
  const explicitAccept = (router: string) => `-A KUBE-ROUTER-${router} ${comment("rule to explicitly ACCEPT traffic that comply to network policies")} -m mark --mark 0x20000/0x20000 -j ACCEPT`;

  return [
    `# Generated by ${v6 ? "ip6tables-save" : "iptables-save"} v1.8.11 (nf_tables) on Mon Oct 12 09:00:00 2026`,
    "*filter",
    ":INPUT ACCEPT [0:0]",
    ":FORWARD ACCEPT [0:0]",
    ":OUTPUT ACCEPT [0:0]",
    ...chains.map((chain) => `:${chain} - [0:0]`),
    `-A INPUT ${comment("kube-router netpol - 4IA2OSFRMVNDXBVV")} -j KUBE-ROUTER-INPUT`,
    `-A INPUT -m conntrack --ctstate NEW ${comment("kubernetes load balancer firewall")} -j KUBE-PROXY-FIREWALL`,
    `-A INPUT -m conntrack --ctstate NEW ${comment("kubernetes externally-visible service portals")} -j KUBE-SERVICES`,
    "-A INPUT -j KUBE-FIREWALL",
    `-A FORWARD ${comment("kube-router netpol - TEMCG2JMHZYE7H7T")} -j KUBE-ROUTER-FORWARD`,
    `-A FORWARD -m conntrack --ctstate NEW ${comment("kubernetes load balancer firewall")} -j KUBE-PROXY-FIREWALL`,
    `-A FORWARD ${comment("kubernetes forwarding rules")} -j KUBE-FORWARD`,
    `-A FORWARD ${comment("flanneld forward")} -j FLANNEL-FWD`,
    `-A OUTPUT ${comment("kube-router netpol - VEAAIY32XVBHCSCY")} -j KUBE-ROUTER-OUTPUT`,
    `-A OUTPUT -m conntrack --ctstate NEW ${comment("kubernetes service portals")} -j KUBE-SERVICES`,
    "-A OUTPUT -j KUBE-FIREWALL",
    `-A FLANNEL-FWD -s ${podCidr} ${comment("flanneld forward")} -j ACCEPT`,
    `-A FLANNEL-FWD -d ${podCidr} ${comment("flanneld forward")} -j ACCEPT`,
    ...(v6 ? [] : [`-A KUBE-FIREWALL ! -s 127.0.0.0/8 -d 127.0.0.0/8 ${comment("block incoming localnet connections")} -m conntrack ! --ctstate RELATED,ESTABLISHED,DNAT -j DROP`]),
    "-A KUBE-FORWARD -m conntrack --ctstate INVALID -m nfacct --nfacct-name ct_state_invalid_dropped_pkts -j DROP",
    `-A KUBE-FORWARD ${comment("kubernetes forwarding rules")} -m mark --mark 0x4000/0x4000 -j ACCEPT`,
    `-A KUBE-FORWARD ${comment("kubernetes forwarding conntrack rule")} -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT`,
    `-A KUBE-NWPLCY-COMMON ${comment("rule to drop invalid state for pod")} -m conntrack --ctstate INVALID -j DROP`,
    `-A KUBE-NWPLCY-COMMON ${comment("rule for stateful firewall for pod")} -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT`,
    `-A KUBE-NWPLCY-COMMON -p ${protocol} ${comment("allow icmp echo requests")} -m ${match} ${echo} -j ACCEPT`,
    `-A KUBE-NWPLCY-COMMON -p ${protocol} ${comment("allow icmp destination unreachable messages")} -m ${match} ${unreachable} -j ACCEPT`,
    `-A KUBE-NWPLCY-COMMON -p ${protocol} ${comment("allow icmp time exceeded messages")} -m ${match} ${exceeded} -j ACCEPT`,
    `-A KUBE-NWPLCY-DEFAULT ${comment("rule to mark traffic matching a network policy")} -j MARK --set-xmark 0x10000/0x10000`,
    ...policyRules,
    ...podRules,
    ...jumps("FORWARD"),
    explicitAccept("FORWARD"),
    ...jumps("INPUT"),
    explicitAccept("INPUT"),
    ...jumps("OUTPUT"),
    explicitAccept("OUTPUT"),
    "COMMIT",
    "# Completed on Mon Oct 12 09:00:00 2026",
    "",
  ].join("\n");
}
