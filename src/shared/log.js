export function createLogger(scope) {
  return {
    info: (message) => console.log(`[${scope}] ${message}`),
    warn: (message) => console.warn(`[${scope}] ${message}`),
    error: (message) => console.error(`[${scope}] ${message}`),
  };
}
