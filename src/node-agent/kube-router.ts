/**
 * kube-router's rules for what a pod sends, as k3s's network policy controller, kube-router
 * (v2.6.3 as k3s v1.37 vendors it, pkg/controllers/netpol/pod.go), programs them on the
 * pod's node, and iptables-save prints its filter table. For each pod on the node, in the
 * table of each family it has an address of:
 *
 * - a chain of its own, `KUBE-POD-FW-<hash>`, named after the pod and the sync that wrote
 *   it, so anew by every sync: it is found by what the rules say, never by its name;
 * - in KUBE-ROUTER-FORWARD, which FORWARD jumps to first, a jump to that chain of what the
 *   pod's address sends (`-s <ip>/32`, `/128` in IPv6), commented "rule to jump traffic
 *   from POD name:<pod> namespace: <namespace> to chain <chain>", and another of what it
 *   sends bridged (`-m physdev --physdev-is-bridged`);
 * - in that chain, after a jump to KUBE-NWPLCY-COMMON (which lets the packets of tracked
 *   connections through), a jump to the chain of each network policy selecting the pod
 *   (`KUBE-NWPLCY-<hash>`), commented "run through nw policy <policy>", held to what the
 *   pod receives (`-d <ip>`) for an Ingress policy, to what it sends (`-s <ip>`) for an
 *   Egress one, to neither for one of both. Where no policy selects it for a direction, a
 *   jump to KUBE-NWPLCY-DEFAULT instead, which lets everything through. Then the
 *   rejection of what no policy's chain marked as let through (`-m mark ! --mark
 *   0x10000/0x10000 -j REJECT`), commented "rule to REJECT traffic destined for POD
 *   name:<pod> namespace: <namespace>", whatever its direction.
 *
 * Until a sync writes them, no rule matches a pod's new address, and what it sends goes
 * through. A sync writes the table whole (iptables-restore), so a save holds all of a
 * sync's rules or none.
 *
 * All that says whether a pod's address has its rules is in two chains: KUBE-ROUTER-FORWARD,
 * and the pod's own its jump reaches. They are listed alone (`iptables -S <chain>`, which
 * prints a rule as iptables-save does), not the whole table, which holds every pod's rules
 * on the node (listedEgressGap). A sync between the two lists names the pod's chain anew, the
 * one the first found gone by the second: a gap, until a look finds both of one sync.
 *
 * Nothing of the rules names the pod but its name and namespace, which a pod before it of
 * the same name shares, and its address, which that pod may have had: until a sync
 * after the new pod has its address, the old one's rules hold it, run through the network
 * policies that selected the old one. They are the new one's only where the same
 * policies select it, which the jumps' comments name: a pod's are held to name those
 * that select it now, no more and no fewer.
 */
import { Effect } from "effect";

/** The chain FORWARD sends every forwarded packet through first. */
const FORWARD_CHAIN = "KUBE-ROUTER-FORWARD";
const POD_CHAIN_PREFIX = "KUBE-POD-FW-";
/** The prefix of a network policy's chain, and of KUBE-NWPLCY-COMMON's and KUBE-NWPLCY-DEFAULT's, whose jumps are commented otherwise. */
const POLICY_CHAIN_PREFIX = "KUBE-NWPLCY-";
/** The mark a network policy's chain sets on what it lets through. */
const POLICY_MARK = "0x10000/0x10000";

/** An address of a pod's, as the rules name it, and the network policies that select the pod. */
export interface PodAddress {
  readonly namespace: string;
  readonly pod: string;
  /** In its canonical form, as Kubernetes gives it and iptables-save prints it. */
  readonly ip: string;
  /** The network policies of its namespace that select it, by name, as the API server has them. */
  readonly policies: readonly string[];
}

/** A rule of a chain: its chain, and its words after it, a quoted one unquoted. */
interface Rule {
  readonly chain: string;
  readonly words: readonly string[];
}

/** The words of a line of iptables-save's, a quoted one (as it quotes a comment) unquoted and unescaped. */
function wordsOf(line: string): string[] {
  return [...line.matchAll(/"((?:[^"\\]|\\.)*)"|(\S+)/gu)].map(([, quoted, bare = ""]) =>
    quoted === undefined ? bare : quoted.replace(/\\(.)/gu, (_escape, escaped: string) => escaped)
  );
}

/** The rules of a table iptables-save printed, in order. */
function rulesOf(saved: string): Rule[] {
  return saved.split("\n").flatMap((line) => {
    const [append, chain = "", ...words] = wordsOf(line);
    return append === "-A" ? [{ chain, words }] : [];
  });
}

/** The value of a rule's option `name`, or undefined. */
function option({ words }: Rule, name: string): string | undefined {
  const at = words.indexOf(name);
  return at >= 0 ? words[at + 1] : undefined;
}

/** The jump in KUBE-ROUTER-FORWARD of what the pod's address sends to a chain of the pod's, commented as the pod's, if any. */
function jumpOf(rules: readonly Rule[], { namespace, pod, ip }: Omit<PodAddress, "policies">): Rule | undefined {
  const source = `${ip}/${ip.includes(":") ? 128 : 32}`;
  return rules.find((rule) => {
    const target = option(rule, "-j") ?? "";
    return rule.chain === FORWARD_CHAIN &&
      option(rule, "-s") === source &&
      target.startsWith(POD_CHAIN_PREFIX) &&
      option(rule, "--comment") === `rule to jump traffic from POD name:${pod} namespace: ${namespace} to chain ${target}`;
  });
}

/** Whether a rule rejects what no network policy's chain marked. */
function rejectsUnmarked(rule: Rule): boolean {
  const at = rule.words.indexOf("--mark");
  return option(rule, "-j") === "REJECT" && at > 0 && rule.words[at - 1] === "!" && rule.words[at + 1] === POLICY_MARK;
}

/**
 * What of kube-router's rules holding what a pod's address sends to its network policies
 * the filter table `saved` lacks, as the module says, said as a sentence ends; null when
 * it holds them all. A jump of another pod's from the address, as one the address had
 * before, is none of this pod's, nor are the rules of a pod of the same name run through
 * other policies.
 */
export function egressGap(saved: string, { namespace, pod, ip, policies }: PodAddress): string | null {
  const rules = rulesOf(saved);
  const jump = jumpOf(rules, { namespace, pod, ip });
  if (!jump) return `${FORWARD_CHAIN} has no jump of what it sends to the chain of ${namespace}/${pod}`;
  const chain = option(jump, "-j") ?? "";
  const own = rules.filter((rule) => rule.chain === chain);
  const policed = own.some((rule) =>
    (option(rule, "-j") ?? "").startsWith(POLICY_CHAIN_PREFIX) &&
    (option(rule, "--comment") ?? "").startsWith("run through nw policy ") &&
    option(rule, "-d") === undefined
  );
  if (!policed) return `the chain of ${namespace}/${pod}, ${chain}, runs what it sends through no network policy`;
  const named = [...new Set(own.flatMap((rule) => {
    const comment = option(rule, "--comment") ?? "";
    return (option(rule, "-j") ?? "").startsWith(POLICY_CHAIN_PREFIX) && comment.startsWith("run through nw policy ") ? [comment.slice("run through nw policy ".length)] : [];
  }))].sort();
  const expected = [...new Set(policies)].sort();
  if (named.join(",") !== expected.join(",")) {
    return `the chain of ${namespace}/${pod}, ${chain}, runs it through the network policies ${named.join(", ")}, not ${expected.join(", ")}, as a pod of its name before it was`;
  }
  const rejection = `rule to REJECT traffic destined for POD name:${pod} namespace: ${namespace}`;
  if (!own.some((rule) => rejectsUnmarked(rule) && option(rule, "--comment") === rejection)) {
    return `the chain of ${namespace}/${pod}, ${chain}, does not reject what its network policies do not let through`;
  }
  return null;
}

/** A chain of a node's filter table as iptables lists it: its rules, as iptables-save prints them; or, where it could not list it, as one there is not, what it said. */
export type Listing = { readonly rules: string } | { readonly unlisted: string };

/**
 * What egressGap says of the filter table of the address's family, read as the module says:
 * KUBE-ROUTER-FORWARD listed by `list`, then the chain its jump of what the address sends
 * reaches, all of the table egressGap reads. A chain that could not be listed is a gap, said
 * with why: KUBE-ROUTER-FORWARD before kube-router made it, or the pod's chain a sync named
 * anew since the first list.
 */
export const listedEgressGap = <E>(list: (chain: string) => Effect.Effect<Listing, E>, address: PodAddress): Effect.Effect<string | null, E> =>
  Effect.gen(function*() {
    const forward = yield* list(FORWARD_CHAIN);
    if ("unlisted" in forward) return `${FORWARD_CHAIN} could not be listed: ${forward.unlisted}`;
    const jump = jumpOf(rulesOf(forward.rules), address);
    if (!jump) return egressGap(forward.rules, address);
    const chain = option(jump, "-j") ?? "";
    const own = yield* list(chain);
    if ("unlisted" in own) return `the chain of ${address.namespace}/${address.pod}, ${chain}, could not be listed: ${own.unlisted}`;
    return egressGap(`${forward.rules}\n${own.rules}`, address);
  });
