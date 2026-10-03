// @ts-nocheck
export function finalResponseToMarkdown(blockSequence) {
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
