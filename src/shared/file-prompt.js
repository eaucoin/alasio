function ordinal(n) {
  const mod100 = n % 100;
  if (mod100 >= 11 && mod100 <= 13) {
    return `${n}th`;
  }
  const suffixes = { 1: "st", 2: "nd", 3: "rd" };
  return `${n}${suffixes[n % 10] ?? "th"}`;
}

export function buildFilePromptSuffix(filePaths) {
  if (filePaths.length === 0) {
    return "";
  }
  if (filePaths.length === 1) {
    return `\n\nYou can see the file at ${filePaths[0]}`;
  }
  return `\n\n${filePaths.map((path, index) => `You can see the ${ordinal(index + 1)} file at ${path}`).join("\n")}`;
}
