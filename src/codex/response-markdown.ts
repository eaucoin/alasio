/** A block of a response as stored, of which only final-answer text blocks are read. */
interface StoredBlock {
  readonly type: string;
  readonly content?: unknown;
  readonly phase?: unknown;
}

export function finalResponseToMarkdown(blockSequence: readonly StoredBlock[]): string {
  const finalAnswer = [...blockSequence].reverse().find((block) =>
    block.type === "text"
    && block.phase === "final_answer"
    && String(block.content ?? "").trim()
  );
  if (finalAnswer) {
    return String(finalAnswer.content).trim();
  }
  return "";
}
